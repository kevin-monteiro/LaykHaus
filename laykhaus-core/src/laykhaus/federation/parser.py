"""
SQL Query Parser for LaykHaus Federation Engine.

Parses SQL queries and identifies data sources, operations, and optimization opportunities.
"""

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

import sqlparse
from sqlparse.sql import Identifier, IdentifierList, Token, Where

from laykhaus.connectors.base import PushdownType, SQLFeature
from laykhaus.core.logging import get_logger

logger = get_logger(__name__)


class QueryType(Enum):
    """Types of SQL queries."""
    SELECT = "select"
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    CREATE = "create"
    DROP = "drop"
    ALTER = "alter"


class JoinType(Enum):
    """Types of SQL joins."""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"
    CROSS = "cross"


@dataclass
class TableReference:
    """Represents a table reference in a query."""
    source: str  # Data source name (e.g., "postgres_main")
    schema: Optional[str]  # Schema name
    table: str  # Table name
    alias: Optional[str] = None  # Table alias
    
    @property
    def full_name(self) -> str:
        """Get full table name."""
        parts = []
        if self.source:
            parts.append(self.source)
        if self.schema:
            parts.append(self.schema)
        parts.append(self.table)
        return ".".join(parts)
    
    def __str__(self) -> str:
        """String representation."""
        return f"{self.full_name}{f' AS {self.alias}' if self.alias else ''}"


@dataclass
class JoinClause:
    """Represents a JOIN clause."""
    join_type: JoinType
    left_table: TableReference
    right_table: TableReference
    condition: str  # Join condition as string
    
    def __str__(self) -> str:
        """String representation."""
        return f"{self.join_type.value.upper()} JOIN {self.right_table} ON {self.condition}"


@dataclass
class ParsedQuery:
    """
    Parsed SQL query with metadata for federation planning.
    """
    raw_query: str
    query_type: QueryType
    tables: List[TableReference]
    columns: List[str]  # Selected columns
    where_clause: Optional[str] = None
    group_by: List[str] = field(default_factory=list)
    having_clause: Optional[str] = None
    order_by: List[Tuple[str, str]] = field(default_factory=list)  # (column, direction)
    limit: Optional[int] = None
    offset: Optional[int] = None
    joins: List[JoinClause] = field(default_factory=list)
    
    # Analysis results
    required_features: Set[SQLFeature] = field(default_factory=set)
    pushdown_opportunities: Set[PushdownType] = field(default_factory=set)
    cross_source: bool = False
    
    def get_source_tables(self) -> Dict[str, List[TableReference]]:
        """
        Group tables by their data source.
        
        Returns:
            Dictionary mapping source name to list of tables
        """
        source_tables = {}
        for table in self.tables:
            if table.source not in source_tables:
                source_tables[table.source] = []
            source_tables[table.source].append(table)
        return source_tables
    
    def is_federated(self) -> bool:
        """Check if query requires federation across multiple sources."""
        sources = set(table.source for table in self.tables if table.source)
        return len(sources) > 1


class QueryParser:
    """
    Parses SQL queries and extracts metadata for federation planning.
    """
    
    def __init__(self):
        """Initialize query parser."""
        self.logger = get_logger(__name__)
    
    def parse(self, query: str) -> ParsedQuery:
        """
        Parse a SQL query into structured components.
        
        Args:
            query: SQL query string
        
        Returns:
            ParsedQuery with extracted metadata
        """
        self.logger.debug(f"Parsing query: {query[:100]}...")
        
        # Clean and format query
        query = query.strip()
        formatted = sqlparse.format(query, reindent=True, keyword_case='upper')
        
        # Parse with sqlparse
        parsed = sqlparse.parse(formatted)[0]
        
        # Determine query type
        query_type = self._get_query_type(parsed)
        
        # Create base parsed query
        result = ParsedQuery(
            raw_query=query,
            query_type=query_type,
            tables=[],
            columns=[]
        )
        
        # Extract components based on query type
        if query_type == QueryType.SELECT:
            self._parse_select_query(parsed, result)
        else:
            # For Phase 2, focus on SELECT queries
            raise NotImplementedError(f"Query type {query_type} not yet supported")
        
        # Analyze query requirements
        self._analyze_requirements(result)
        
        # Check if cross-source
        result.cross_source = result.is_federated()
        
        self.logger.debug(f"Parsed query with {len(result.tables)} tables, cross_source={result.cross_source}")
        
        return result
    
    def _get_query_type(self, parsed) -> QueryType:
        """Determine the type of SQL query."""
        first_token = parsed.token_first(skip_ws=True, skip_cm=True)
        if first_token:
            token_str = str(first_token).upper()
            if token_str == "SELECT":
                return QueryType.SELECT
            elif token_str == "INSERT":
                return QueryType.INSERT
            elif token_str == "UPDATE":
                return QueryType.UPDATE
            elif token_str == "DELETE":
                return QueryType.DELETE
            elif token_str == "CREATE":
                return QueryType.CREATE
            elif token_str == "DROP":
                return QueryType.DROP
            elif token_str == "ALTER":
                return QueryType.ALTER
        
        raise ValueError(f"Unknown query type: {first_token}")
    
    def _parse_select_query(self, parsed, result: ParsedQuery):
        """Parse a SELECT query."""
        # Extract columns
        for token in parsed.tokens:
            if token.ttype is None and isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    result.columns.append(str(identifier).strip())
            elif token.ttype is None and isinstance(token, Identifier):
                result.columns.append(str(token).strip())
        
        # Extract FROM clause tables
        from_seen = False
        for token in parsed.tokens:
            if from_seen:
                if token.ttype is sqlparse.tokens.Keyword and str(token).upper() in ["WHERE", "GROUP", "ORDER", "LIMIT"]:
                    from_seen = False
                elif not token.is_whitespace:
                    self._extract_tables(token, result)
            elif token.ttype is sqlparse.tokens.Keyword and str(token).upper() == "FROM":
                from_seen = True
        
        # Extract WHERE clause
        where_token = self._find_where(parsed)
        if where_token:
            result.where_clause = str(where_token).replace("WHERE", "").strip()
        
        # Extract GROUP BY
        group_by = self._find_keyword_clause(parsed, "GROUP BY")
        if group_by:
            result.group_by = [col.strip() for col in group_by.split(",")]
        
        # Extract HAVING
        having = self._find_keyword_clause(parsed, "HAVING")
        if having:
            result.having_clause = having
        
        # Extract ORDER BY
        order_by = self._find_keyword_clause(parsed, "ORDER BY")
        if order_by:
            for item in order_by.split(","):
                parts = item.strip().split()
                column = parts[0]
                direction = parts[1] if len(parts) > 1 else "ASC"
                result.order_by.append((column, direction))
        
        # Extract LIMIT and OFFSET
        limit_match = re.search(r"LIMIT\s+(\d+)", str(parsed), re.IGNORECASE)
        if limit_match:
            result.limit = int(limit_match.group(1))
        
        offset_match = re.search(r"OFFSET\s+(\d+)", str(parsed), re.IGNORECASE)
        if offset_match:
            result.offset = int(offset_match.group(1))
    
    def _extract_tables(self, token, result: ParsedQuery):
        """Extract table references from a token."""
        token_str = str(token).strip()
        
        # Handle JOIN clauses
        join_match = re.search(
            r"(INNER|LEFT|RIGHT|FULL|CROSS)?\s*JOIN\s+(\S+)(?:\s+AS\s+(\w+)|\s+(\w+))?\s+ON\s+(.+)",
            token_str,
            re.IGNORECASE
        )
        
        if join_match:
            join_type = join_match.group(1) or "INNER"
            table_name = join_match.group(2)
            alias = join_match.group(3) or join_match.group(4)
            condition = join_match.group(5)
            
            # Parse table reference
            table_ref = self._parse_table_reference(table_name, alias)
            result.tables.append(table_ref)
            
            # Create join clause
            if result.tables:
                join = JoinClause(
                    join_type=JoinType(join_type.lower()),
                    left_table=result.tables[0],  # Assume first table is left
                    right_table=table_ref,
                    condition=condition
                )
                result.joins.append(join)
        else:
            # Simple table reference
            if "," in token_str:
                # Multiple tables
                for table_part in token_str.split(","):
                    table_ref = self._parse_table_reference(table_part.strip())
                    if table_ref:
                        result.tables.append(table_ref)
            else:
                # Single table
                table_ref = self._parse_table_reference(token_str)
                if table_ref:
                    result.tables.append(table_ref)
    
    def _parse_table_reference(self, table_str: str, alias: Optional[str] = None) -> Optional[TableReference]:
        """Parse a table reference string into TableReference object."""
        if not table_str:
            return None
        
        # Remove parentheses and extra spaces
        table_str = table_str.strip().strip("()")
        
        # Handle alias in the string
        if " AS " in table_str.upper():
            parts = re.split(r"\s+AS\s+", table_str, flags=re.IGNORECASE)
            table_str = parts[0]
            alias = parts[1] if not alias else alias
        elif " " in table_str and not any(kw in table_str.upper() for kw in ["JOIN", "WHERE", "ON"]):
            parts = table_str.split()
            table_str = parts[0]
            alias = parts[1] if len(parts) > 1 and not alias else alias
        
        # Parse table parts (source.schema.table)
        parts = table_str.split(".")
        
        if len(parts) == 3:
            # source.schema.table
            return TableReference(
                source=parts[0],
                schema=parts[1],
                table=parts[2],
                alias=alias
            )
        elif len(parts) == 2:
            # Could be source.table or schema.table
            # For now, assume it's schema.table
            return TableReference(
                source="postgres_main",  # Default source
                schema=parts[0],
                table=parts[1],
                alias=alias
            )
        elif len(parts) == 1:
            # Just table name
            return TableReference(
                source="postgres_main",  # Default source
                schema="public",  # Default schema
                table=parts[0],
                alias=alias
            )
        
        return None
    
    def _find_where(self, parsed) -> Optional[Where]:
        """Find WHERE clause in parsed query."""
        for token in parsed.tokens:
            if isinstance(token, Where):
                return token
        return None
    
    def _find_keyword_clause(self, parsed, keyword: str) -> Optional[str]:
        """Find a clause that starts with a keyword."""
        keyword_upper = keyword.upper()
        found = False
        result = []
        
        for token in parsed.tokens:
            if found:
                if token.ttype is sqlparse.tokens.Keyword:
                    break
                if not token.is_whitespace:
                    result.append(str(token))
            elif str(token).upper().startswith(keyword_upper):
                found = True
                # Extract the part after the keyword
                remainder = str(token)[len(keyword):].strip()
                if remainder:
                    result.append(remainder)
        
        return " ".join(result) if result else None
    
    def _analyze_requirements(self, query: ParsedQuery):
        """Analyze query to determine required features and pushdown opportunities."""
        # Required SQL features
        query.required_features.add(SQLFeature.SELECT)
        
        if query.where_clause:
            query.required_features.add(SQLFeature.WHERE)
            query.pushdown_opportunities.add(PushdownType.FILTER)
        
        if query.joins:
            query.required_features.add(SQLFeature.JOIN)
            query.pushdown_opportunities.add(PushdownType.JOIN)
        
        if query.group_by:
            query.required_features.add(SQLFeature.GROUP_BY)
            query.pushdown_opportunities.add(PushdownType.AGGREGATION)
        
        if query.having_clause:
            query.required_features.add(SQLFeature.HAVING)
        
        if query.order_by:
            query.required_features.add(SQLFeature.ORDER_BY)
            query.pushdown_opportunities.add(PushdownType.SORT)
        
        if query.limit:
            query.required_features.add(SQLFeature.LIMIT)
            query.pushdown_opportunities.add(PushdownType.LIMIT)
        
        if query.offset:
            query.required_features.add(SQLFeature.OFFSET)
        
        # Projection pushdown if specific columns selected
        if query.columns and query.columns != ["*"]:
            query.pushdown_opportunities.add(PushdownType.PROJECTION)