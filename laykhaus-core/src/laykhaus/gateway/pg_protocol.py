"""
PostgreSQL Wire Protocol Implementation for LaykHaus.

Implements PostgreSQL wire protocol v3 to enable standard SQL client connectivity.
"""

import asyncio
import hashlib
import struct
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum

from laykhaus.core.logging import get_logger
from laykhaus.federation import FederationExecutor

logger = get_logger(__name__)


class MessageType(Enum):
    """PostgreSQL wire protocol message types."""
    AUTHENTICATION = b'R'
    BACKEND_KEY_DATA = b'K'
    BIND = b'B'
    BIND_COMPLETE = b'2'
    CLOSE = b'C'
    CLOSE_COMPLETE = b'3'
    COMMAND_COMPLETE = b'C'
    COPY_DATA = b'd'
    COPY_DONE = b'c'
    COPY_FAIL = b'f'
    COPY_IN_RESPONSE = b'G'
    COPY_OUT_RESPONSE = b'H'
    DATA_ROW = b'D'
    DESCRIBE = b'D'
    EMPTY_QUERY_RESPONSE = b'I'
    ERROR_RESPONSE = b'E'
    EXECUTE = b'E'
    FLUSH = b'H'
    FUNCTION_CALL = b'F'
    FUNCTION_CALL_RESPONSE = b'V'
    NO_DATA = b'n'
    NOTICE_RESPONSE = b'N'
    NOTIFICATION_RESPONSE = b'A'
    PARAMETER_DESCRIPTION = b't'
    PARAMETER_STATUS = b'S'
    PARSE = b'P'
    PARSE_COMPLETE = b'1'
    PASSWORD_MESSAGE = b'p'
    PORTAL_SUSPENDED = b's'
    QUERY = b'Q'
    READY_FOR_QUERY = b'Z'
    ROW_DESCRIPTION = b'T'
    SYNC = b'S'
    TERMINATE = b'X'


class PGProtocolHandler:
    """
    Handles PostgreSQL wire protocol communication.
    Translates between PostgreSQL protocol and LaykHaus federation engine.
    """
    
    def __init__(self, executor: FederationExecutor):
        """Initialize protocol handler with federation executor."""
        self.executor = executor
        self.logger = get_logger(__name__)
        self.prepared_statements = {}
        self.portals = {}
        
    async def handle_startup(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> Optional[Dict]:
        """
        Handle PostgreSQL startup handshake.
        
        Returns:
            User context if authentication successful, None otherwise
        """
        try:
            # Read startup message
            length = struct.unpack('!I', await reader.read(4))[0]
            startup_data = await reader.read(length - 4)
            
            # Parse protocol version
            protocol_version = struct.unpack('!I', startup_data[:4])[0]
            major = protocol_version >> 16
            minor = protocol_version & 0xFFFF
            
            # Handle SSL request (protocol 1234.5679)
            if protocol_version == 80877103:  # SSL request
                self.logger.info("Client requesting SSL connection")
                # Send SSL not supported ('N')
                writer.write(b'N')
                await writer.drain()
                # Read the actual startup message after SSL rejection
                length = struct.unpack('!I', await reader.read(4))[0]
                startup_data = await reader.read(length - 4)
                protocol_version = struct.unpack('!I', startup_data[:4])[0]
                major = protocol_version >> 16
                minor = protocol_version & 0xFFFF
            
            self.logger.info(f"Client connected with protocol {major}.{minor}")
            
            # Parse startup parameters
            params = {}
            data = startup_data[4:]
            while data:
                null_idx = data.find(b'\x00')
                if null_idx == 0:
                    break
                    
                key = data[:null_idx].decode('utf-8')
                data = data[null_idx + 1:]
                
                null_idx = data.find(b'\x00')
                value = data[:null_idx].decode('utf-8')
                data = data[null_idx + 1:]
                
                params[key] = value
            
            # For demo, accept all connections (no auth)
            await self._send_auth_ok(writer)
            
            # Send backend key data
            await self._send_backend_key_data(writer)
            
            # Send parameter status messages
            await self._send_parameter_status(writer, "server_version", "14.0 LaykHaus")
            await self._send_parameter_status(writer, "server_encoding", "UTF8")
            await self._send_parameter_status(writer, "client_encoding", "UTF8")
            await self._send_parameter_status(writer, "DateStyle", "ISO")
            
            # Send ready for query
            await self._send_ready_for_query(writer)
            
            return {
                "user": params.get("user", "anonymous"),
                "database": params.get("database", "laykhaus"),
                "application": params.get("application_name", "unknown")
            }
            
        except Exception as e:
            self.logger.error(f"Startup handshake failed: {e}")
            return None
    
    async def handle_query(self, query: str, writer: asyncio.StreamWriter, user_context: Dict):
        """
        Handle simple query protocol.
        
        Args:
            query: SQL query string
            writer: Stream writer for response
            user_context: User context
        """
        try:
            # Special handling for common utility queries
            if query.upper().startswith("SELECT VERSION()"):
                await self._send_version_response(writer)
                return
            elif query.upper().startswith("SELECT CURRENT_DATABASE()"):
                await self._send_current_database(writer, user_context)
                return
            elif query.upper() == "SELECT 1":
                await self._send_select_one(writer)
                return
            
            # Execute through federation engine
            result = await self.executor.execute(query, user_context)
            
            # Send row description
            await self._send_row_description(writer, result.schema)
            
            # Send data rows
            for row in result.rows:
                await self._send_data_row(writer, row, result.schema)
            
            # Send command complete
            await self._send_command_complete(writer, "SELECT", len(result.rows))
            
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            await self._send_error(writer, str(e))
    
    async def handle_parse(self, statement_name: str, query: str, param_types: List[int], writer: asyncio.StreamWriter):
        """Handle PARSE message for prepared statements."""
        try:
            # Store prepared statement
            self.prepared_statements[statement_name] = {
                "query": query,
                "param_types": param_types
            }
            
            # Send parse complete
            await self._send_parse_complete(writer)
            
        except Exception as e:
            await self._send_error(writer, str(e))
    
    async def handle_bind(self, portal_name: str, statement_name: str, params: List[Any], writer: asyncio.StreamWriter):
        """Handle BIND message to create portal from prepared statement."""
        try:
            if statement_name not in self.prepared_statements:
                raise ValueError(f"Prepared statement {statement_name} not found")
            
            # Create portal
            self.portals[portal_name] = {
                "statement": self.prepared_statements[statement_name],
                "params": params
            }
            
            # Send bind complete
            await self._send_bind_complete(writer)
            
        except Exception as e:
            await self._send_error(writer, str(e))
    
    async def handle_execute(self, portal_name: str, max_rows: int, writer: asyncio.StreamWriter, user_context: Dict):
        """Handle EXECUTE message to run portal."""
        try:
            if portal_name not in self.portals:
                # Unnamed portal - execute immediately
                await self._send_empty_query_response(writer)
                return
            
            portal = self.portals[portal_name]
            query = portal["statement"]["query"]
            
            # Substitute parameters (simplified)
            for i, param in enumerate(portal["params"]):
                query = query.replace(f"${i+1}", str(param))
            
            # Execute query
            result = await self.executor.execute(query, user_context)
            
            # Send results
            await self._send_row_description(writer, result.schema)
            
            rows_sent = 0
            for row in result.rows:
                if max_rows > 0 and rows_sent >= max_rows:
                    break
                await self._send_data_row(writer, row, result.schema)
                rows_sent += 1
            
            if max_rows > 0 and rows_sent < len(result.rows):
                await self._send_portal_suspended(writer)
            else:
                await self._send_command_complete(writer, "SELECT", rows_sent)
                
        except Exception as e:
            await self._send_error(writer, str(e))
    
    async def _send_auth_ok(self, writer: asyncio.StreamWriter):
        """Send authentication OK message."""
        msg = struct.pack('!cII', MessageType.AUTHENTICATION.value, 8, 0)
        writer.write(msg)
        await writer.drain()
    
    async def _send_backend_key_data(self, writer: asyncio.StreamWriter):
        """Send backend key data."""
        process_id = 12345  # Dummy process ID
        secret_key = 67890  # Dummy secret key
        msg = struct.pack('!cIII', MessageType.BACKEND_KEY_DATA.value, 12, process_id, secret_key)
        writer.write(msg)
        await writer.drain()
    
    async def _send_parameter_status(self, writer: asyncio.StreamWriter, param: str, value: str):
        """Send parameter status message."""
        param_bytes = param.encode('utf-8') + b'\x00'
        value_bytes = value.encode('utf-8') + b'\x00'
        length = 4 + len(param_bytes) + len(value_bytes)
        
        msg = struct.pack('!cI', MessageType.PARAMETER_STATUS.value, length)
        msg += param_bytes + value_bytes
        writer.write(msg)
        await writer.drain()
    
    async def _send_ready_for_query(self, writer: asyncio.StreamWriter, status: str = 'I'):
        """Send ready for query message."""
        msg = struct.pack('!cIc', MessageType.READY_FOR_QUERY.value, 5, status.encode('utf-8'))
        writer.write(msg)
        await writer.drain()
    
    async def _send_row_description(self, writer: asyncio.StreamWriter, schema: Dict[str, str]):
        """Send row description message."""
        fields = []
        for col_name, col_type in schema.items():
            # Column name
            name_bytes = col_name.encode('utf-8') + b'\x00'
            
            # Type OID (simplified mapping)
            type_oid = {
                "text": 25,
                "varchar": 1043,
                "int": 23,
                "bigint": 20,
                "float": 700,
                "double": 701,
                "boolean": 16,
                "timestamp": 1114,
                "date": 1082,
                "json": 114,
                "jsonb": 3802
            }.get(col_type.lower(), 25)  # Default to text
            
            # Field: name, table_oid, column_num, type_oid, type_size, type_modifier, format
            field = name_bytes + struct.pack('!IhIhhh', 0, 0, type_oid, -1, -1, 0)
            fields.append(field)
        
        # Build message
        field_count = len(fields)
        field_data = b''.join(fields)
        length = 4 + 2 + len(field_data)
        
        msg = struct.pack('!cIh', MessageType.ROW_DESCRIPTION.value, length, field_count)
        msg += field_data
        writer.write(msg)
        await writer.drain()
    
    async def _send_data_row(self, writer: asyncio.StreamWriter, row: Dict[str, Any], schema: Dict[str, str]):
        """Send data row message."""
        values = []
        for col_name in schema.keys():
            value = row.get(col_name)
            if value is None:
                # NULL value
                values.append(struct.pack('!i', -1))
            else:
                # Convert to string and encode
                value_bytes = str(value).encode('utf-8')
                values.append(struct.pack('!I', len(value_bytes)) + value_bytes)
        
        # Build message
        field_count = len(values)
        value_data = b''.join(values)
        length = 4 + 2 + len(value_data)
        
        msg = struct.pack('!cIh', MessageType.DATA_ROW.value, length, field_count)
        msg += value_data
        writer.write(msg)
        await writer.drain()
    
    async def _send_command_complete(self, writer: asyncio.StreamWriter, command: str, row_count: int):
        """Send command complete message."""
        if command.upper() == "SELECT":
            tag = f"SELECT {row_count}".encode('utf-8') + b'\x00'
        else:
            tag = command.encode('utf-8') + b'\x00'
        
        length = 4 + len(tag)
        msg = struct.pack('!cI', MessageType.COMMAND_COMPLETE.value, length) + tag
        writer.write(msg)
        await writer.drain()
    
    async def _send_error(self, writer: asyncio.StreamWriter, error_msg: str):
        """Send error response message."""
        # Error fields: S=Severity, C=Code, M=Message
        fields = [
            b'S' + b'ERROR\x00',
            b'C' + b'42000\x00',  # Syntax error code
            b'M' + error_msg.encode('utf-8') + b'\x00',
            b'\x00'  # Terminator
        ]
        
        field_data = b''.join(fields)
        length = 4 + len(field_data)
        
        msg = struct.pack('!cI', MessageType.ERROR_RESPONSE.value, length) + field_data
        writer.write(msg)
        await writer.drain()
    
    async def _send_version_response(self, writer: asyncio.StreamWriter):
        """Send version information."""
        schema = {"version": "text"}
        row = {"version": "PostgreSQL 14.0 (LaykHaus Federation Engine)"}
        
        await self._send_row_description(writer, schema)
        await self._send_data_row(writer, row, schema)
        await self._send_command_complete(writer, "SELECT", 1)
    
    async def _send_current_database(self, writer: asyncio.StreamWriter, user_context: Dict):
        """Send current database name."""
        schema = {"current_database": "text"}
        row = {"current_database": user_context.get("database", "laykhaus")}
        
        await self._send_row_description(writer, schema)
        await self._send_data_row(writer, row, schema)
        await self._send_command_complete(writer, "SELECT", 1)
    
    async def _send_select_one(self, writer: asyncio.StreamWriter):
        """Send result for SELECT 1."""
        schema = {"?column?": "int"}
        row = {"?column?": 1}
        
        await self._send_row_description(writer, schema)
        await self._send_data_row(writer, row, schema)
        await self._send_command_complete(writer, "SELECT", 1)
    
    async def _send_parse_complete(self, writer: asyncio.StreamWriter):
        """Send parse complete message."""
        msg = struct.pack('!cI', MessageType.PARSE_COMPLETE.value, 4)
        writer.write(msg)
        await writer.drain()
    
    async def _send_bind_complete(self, writer: asyncio.StreamWriter):
        """Send bind complete message."""
        msg = struct.pack('!cI', MessageType.BIND_COMPLETE.value, 4)
        writer.write(msg)
        await writer.drain()
    
    async def _send_empty_query_response(self, writer: asyncio.StreamWriter):
        """Send empty query response."""
        msg = struct.pack('!cI', MessageType.EMPTY_QUERY_RESPONSE.value, 4)
        writer.write(msg)
        await writer.drain()
    
    async def _send_portal_suspended(self, writer: asyncio.StreamWriter):
        """Send portal suspended message."""
        msg = struct.pack('!cI', MessageType.PORTAL_SUSPENDED.value, 4)
        writer.write(msg)
        await writer.drain()


class PGWireGateway:
    """
    PostgreSQL Wire Protocol Gateway Server.
    
    Listens for PostgreSQL client connections and handles them using the protocol handler.
    """
    
    def __init__(self, executor: FederationExecutor, host: str = '0.0.0.0', port: int = 5433):
        """
        Initialize gateway server.
        
        Args:
            executor: Federation executor for query processing
            host: Host to bind to
            port: Port to listen on (default 5433 to avoid conflict with real PostgreSQL)
        """
        self.executor = executor
        self.host = host
        self.port = port
        self.logger = get_logger(__name__)
        self.handler = PGProtocolHandler(executor)
        self.server = None
    
    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle a client connection."""
        addr = writer.get_extra_info('peername')
        self.logger.info(f"Client connected from {addr}")
        
        try:
            # Handle startup handshake
            user_context = await self.handler.handle_startup(reader, writer)
            if not user_context:
                self.logger.warning(f"Authentication failed for {addr}")
                writer.close()
                await writer.wait_closed()
                return
            
            # Main message loop
            while True:
                # Read message type
                msg_type = await reader.read(1)
                if not msg_type:
                    break
                
                # Read message length
                length_bytes = await reader.read(4)
                if len(length_bytes) < 4:
                    break
                    
                length = struct.unpack('!I', length_bytes)[0]
                
                # Read message body
                body = await reader.read(length - 4)
                
                # Handle message based on type
                if msg_type == MessageType.QUERY.value:
                    # Simple query
                    query = body.decode('utf-8').rstrip('\x00')
                    self.logger.debug(f"Query: {query}")
                    await self.handler.handle_query(query, writer, user_context)
                    await self.handler._send_ready_for_query(writer)
                    
                elif msg_type == MessageType.PARSE.value:
                    # Parse prepared statement
                    # (Simplified parsing - production would be more robust)
                    parts = body.split(b'\x00')
                    statement_name = parts[0].decode('utf-8')
                    query = parts[1].decode('utf-8')
                    await self.handler.handle_parse(statement_name, query, [], writer)
                    
                elif msg_type == MessageType.BIND.value:
                    # Bind portal
                    # (Simplified - production would parse properly)
                    await self.handler.handle_bind("", "", [], writer)
                    
                elif msg_type == MessageType.EXECUTE.value:
                    # Execute portal
                    await self.handler.handle_execute("", 0, writer, user_context)
                    
                elif msg_type == MessageType.SYNC.value:
                    # Sync
                    await self.handler._send_ready_for_query(writer)
                    
                elif msg_type == MessageType.TERMINATE.value:
                    # Terminate
                    break
                    
                else:
                    self.logger.warning(f"Unhandled message type: {msg_type}")
        
        except Exception as e:
            self.logger.error(f"Client handler error: {e}")
        
        finally:
            writer.close()
            await writer.wait_closed()
            self.logger.info(f"Client disconnected from {addr}")
    
    async def start(self):
        """Start the gateway server."""
        self.server = await asyncio.start_server(
            self.handle_client,
            self.host,
            self.port
        )
        
        self.logger.info(f"PostgreSQL Wire Gateway listening on {self.host}:{self.port}")
        
        async with self.server:
            await self.server.serve_forever()
    
    async def stop(self):
        """Stop the gateway server."""
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            self.logger.info("PostgreSQL Wire Gateway stopped")