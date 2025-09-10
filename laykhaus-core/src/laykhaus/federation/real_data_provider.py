"""
Real data provider for production federation.
Connects to actual data sources (PostgreSQL, Kafka, REST APIs).
"""

import json
import requests
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
import structlog

logger = structlog.get_logger(__name__)


class RealDataProvider:
    """Provides real data connections for production federation."""
    
    def __init__(self, spark: SparkSession, connection_manager):
        self.spark = spark
        self.connection_manager = connection_manager
        
    def register_real_data_sources(self, referenced_tables: list) -> Dict[str, DataFrame]:
        """
        Register real data sources based on referenced tables in the query.
        
        Args:
            referenced_tables: List of table names referenced in the SQL query
            
        Returns:
            Dictionary of view_name -> DataFrame
            
        Raises:
            Exception: If any data source connection fails
        """
        views = {}
        
        for table_ref in referenced_tables:
            parts = table_ref.split('.')
            
            if parts[0] == 'postgres':
                view = self._register_postgres_table(table_ref)
                if view:
                    views.update(view)
                else:
                    raise Exception(f"Failed to connect to PostgreSQL table: {table_ref}")
                    
            elif parts[0] == 'kafka':
                view = self._register_kafka_topic(table_ref)
                if view:
                    views.update(view)
                else:
                    raise Exception(f"Failed to connect to Kafka topic: {table_ref}")
                    
            elif parts[0] == 'rest_api':
                view = self._register_rest_endpoint(table_ref)
                if view:
                    views.update(view)
                else:
                    raise Exception(f"Failed to connect to REST API endpoint: {table_ref}")
        
        logger.info(f"Successfully registered {len(views)} real data sources")
        return views
    
    def _register_postgres_table(self, table_ref: str) -> Optional[Dict[str, DataFrame]]:
        """
        Connect to real PostgreSQL table and register as Spark view.
        
        Args:
            table_ref: Table reference like 'postgres.schema.table'
            
        Returns:
            Dictionary with view_name -> DataFrame or None if connection fails
        """
        try:
            parts = table_ref.split('.')
            if len(parts) != 3:
                logger.error(f"Invalid PostgreSQL table reference: {table_ref}")
                return None
                
            datasource, schema, table = parts
            
            # Find a PostgreSQL connector (format: pgsql_*)
            connector = None
            connector_id = None
            
            for conn_id in self.connection_manager.list_connectors():
                if conn_id.startswith('pgsql_'):
                    connector_id = conn_id
                    connector = self.connection_manager.get_connector(conn_id)
                    if connector:
                        logger.info(f"Using PostgreSQL connector: {connector_id}")
                        break
            
            if not connector or not connector.is_connected:
                logger.error(f"PostgreSQL connector not found or not connected: {connector_id}")
                return None
            
            # Build JDBC connection properties
            # Access config attributes directly
            jdbc_url = f"jdbc:postgresql://{connector.config.host}:{connector.config.port}/{connector.config.database}"
            
            # Read the table using Spark JDBC
            df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", f"{schema}.{table}") \
                .option("user", connector.config.username) \
                .option("password", connector.config.password) \
                .option("driver", "org.postgresql.Driver") \
                .load()
            
            # Register as temporary view
            view_name = f"{datasource}_{schema}_{table}".replace('-', '_')
            df.createOrReplaceTempView(view_name)
            
            logger.info(f"Successfully registered PostgreSQL table: {table_ref} as {view_name}")
            return {view_name: df}
            
        except Exception as e:
            logger.error(f"Failed to register PostgreSQL table {table_ref}: {e}")
            return None
    
    def _register_kafka_topic(self, topic_ref: str) -> Optional[Dict[str, DataFrame]]:
        """
        Connect to real Kafka topic and register as Spark view.
        
        Args:
            topic_ref: Topic reference like 'kafka.topic-name'
            
        Returns:
            Dictionary with view_name -> DataFrame or None if connection fails
        """
        try:
            parts = topic_ref.split('.')
            if len(parts) != 2:
                logger.error(f"Invalid Kafka topic reference: {topic_ref}")
                return None
                
            datasource, topic = parts
            # Remove backticks if present
            topic = topic.replace('`', '')
            
            # Find a Kafka connector (format: kafka_*)
            connector = None
            connector_id = None
            
            for conn_id in self.connection_manager.list_connectors():
                if conn_id.startswith('kafka_'):
                    connector_id = conn_id
                    connector = self.connection_manager.get_connector(conn_id)
                    if connector:
                        logger.info(f"Using Kafka connector: {connector_id}")
                        break
            
            if not connector or not connector.is_connected:
                logger.error(f"Kafka connector not found or not connected: {connector_id}")
                return None
            
            # For batch processing, read the latest data from Kafka
            # In a real implementation, you might want to use structured streaming
            # Build bootstrap servers from host and port
            bootstrap_servers = f"{connector.config.host}:{connector.config.port}"
            
            df = self.spark.read \
                .format("kafka") \
                .option("kafka.bootstrap.servers", bootstrap_servers) \
                .option("subscribe", topic) \
                .option("startingOffsets", "earliest") \
                .option("endingOffsets", "latest") \
                .load()
            
            # Parse Kafka message value dynamically
            from pyspark.sql.functions import col, get_json_object
            import json
            
            # First, get a sample message to infer schema
            sample_df = df.limit(1).collect()
            if sample_df:
                sample_value = sample_df[0]['value']
                if sample_value:
                    # Try to parse as JSON to infer schema
                    try:
                        # Decode bytes/bytearray if necessary
                        if isinstance(sample_value, (bytes, bytearray)):
                            sample_value_str = sample_value.decode('utf-8')
                        else:
                            sample_value_str = str(sample_value)
                        
                        sample_json = json.loads(sample_value_str)
                        logger.info(f"Kafka message structure: {list(sample_json.keys()) if isinstance(sample_json, dict) else 'array'}")
                        
                        # Use Spark's automatic schema inference
                        from pyspark.sql.functions import from_json, schema_of_json
                        
                        # Infer schema from the sample (must be string)
                        json_schema = schema_of_json(sample_value_str)
                        
                        # Parse all messages with the inferred schema
                        parsed_df = df.select(
                            from_json(col("value").cast("string"), json_schema).alias("data")
                        ).select("data.*")
                        
                    except json.JSONDecodeError:
                        # If not JSON, keep as string
                        logger.warning(f"Kafka messages are not JSON, keeping as string")
                        parsed_df = df.select(col("value").cast("string").alias("message"))
                else:
                    logger.warning("Empty Kafka message found")
                    parsed_df = df
            else:
                logger.warning("No messages in Kafka topic")
                # Create empty DataFrame with value column
                parsed_df = df
            
            # Register as temporary view
            view_name = f"{datasource}_{topic}".replace('-', '_')
            parsed_df.createOrReplaceTempView(view_name)
            
            logger.info(f"Successfully registered Kafka topic: {topic_ref} as {view_name}")
            return {view_name: parsed_df}
            
        except Exception as e:
            logger.error(f"Failed to register Kafka topic {topic_ref}: {e}")
            return None
    
    def _register_rest_endpoint(self, endpoint_ref: str) -> Optional[Dict[str, DataFrame]]:
        """
        Connect to real REST API endpoint and register as Spark view.
        
        Args:
            endpoint_ref: Endpoint reference like 'rest_api.endpoint'
            
        Returns:
            Dictionary with view_name -> DataFrame or None if connection fails
        """
        try:
            parts = endpoint_ref.split('.')
            if len(parts) != 2:
                logger.error(f"Invalid REST API endpoint reference: {endpoint_ref}")
                return None
                
            datasource, endpoint = parts
            
            # Find a REST API connector (format: rest_*)
            connector = None
            connector_id = None
            
            for conn_id in self.connection_manager.list_connectors():
                if conn_id.startswith('rest_'):
                    connector_id = conn_id
                    connector = self.connection_manager.get_connector(conn_id)
                    if connector:
                        logger.info(f"Using REST API connector: {connector_id}")
                        break
            
            if not connector or not connector.is_connected:
                logger.error(f"REST API connector not found or not connected: {connector_id}")
                return None
            
            # Build base URL from host and port
            protocol = "https" if connector.config.port == 443 else "http"
            base_url = f"{protocol}://{connector.config.host}:{connector.config.port}"
            
            # Get endpoint path from schema if available
            endpoint_path = None
            if hasattr(connector.config, 'extra_params') and connector.config.extra_params:
                schema = connector.config.extra_params.get('schema', {})
                endpoints = schema.get('endpoints', {})
                if endpoint in endpoints:
                    endpoint_path = endpoints[endpoint].get('endpoint')
                    logger.info(f"Using endpoint path from schema: {endpoint_path}")
            
            # Fallback: Construct endpoint path dynamically if not in schema
            if not endpoint_path:
                # Convert underscores to slashes for nested paths (e.g., weather_current -> weather/current)
                endpoint_path = f"/api/{endpoint.replace('_', '/')}"
                logger.info(f"Using dynamic endpoint path: {endpoint_path}")
            
            # Make HTTP request to the endpoint
            endpoint_url = f"{base_url}{endpoint_path}"
            logger.info(f"Fetching data from REST API: {endpoint_url}")
            # Check if extra_params has headers
            headers = getattr(connector.config, 'extra_params', {}).get('headers', {})
            
            response = requests.get(endpoint_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Parse JSON response
            data = response.json()
            logger.info(f"REST API response type: {type(data).__name__}")
            
            # Convert to DataFrame - handle any structure dynamically
            if isinstance(data, list):
                # Direct array of objects
                df = self.spark.createDataFrame(data)
            elif isinstance(data, dict):
                # Try to find the first list in the response to use as data
                # This handles any wrapped response format dynamically
                list_found = False
                for key, value in data.items():
                    if isinstance(value, list) and len(value) > 0:
                        logger.info(f"Using '{key}' field as data source ({len(value)} items)")
                        df = self.spark.createDataFrame(value)
                        list_found = True
                        break
                
                if not list_found:
                    # No list found, treat the entire dict as a single row
                    logger.info("No array found in response, treating as single row")
                    df = self.spark.createDataFrame([data])
            else:
                logger.error(f"Unexpected response format from {endpoint_url}: {type(data)}")
                return None
            
            # Register as temporary view
            # Replace both hyphens and keep underscores
            view_name = f"{datasource}_{endpoint}".replace('-', '_').replace(' ', '_')
            df.createOrReplaceTempView(view_name)
            
            logger.info(f"Successfully registered REST API endpoint: {endpoint_ref} as {view_name}")
            return {view_name: df}
            
        except requests.RequestException as e:
            logger.error(f"Failed to fetch data from REST API {endpoint_ref}: {e}")
            return None
        except Exception as e:
            logger.error(f"Failed to register REST API endpoint {endpoint_ref}: {e}")
            return None