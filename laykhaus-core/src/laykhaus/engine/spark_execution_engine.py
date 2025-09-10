"""
Spark Execution Engine for LaykHaus
This is the core query execution engine that uses Apache Spark internally
to process federated queries, handle streaming, and run ML pipelines.
"""

from typing import Any, Dict, List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from pyspark.conf import SparkConf
import logging

from laykhaus.core.config import get_settings
from laykhaus.connectors.base import BaseConnector

logger = logging.getLogger(__name__)


class SparkExecutionEngine:
    """
    Internal Spark execution engine for LaykHaus.
    Uses Spark to execute federated queries across multiple data sources.
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.spark: Optional[SparkSession] = None
        self._initialize_spark()
    
    def _initialize_spark(self):
        """Initialize Spark session with optimized settings for federation."""
        try:
            conf = SparkConf()
            
            # Use configured SPARK_MASTER_URL or fallback to local mode
            master_url = self.settings.SPARK_MASTER_URL or "local[2]"
            
            conf.setAll([
                ("spark.app.name", "LaykHaus-Federation-Engine"),
                ("spark.master", master_url),
                ("spark.sql.adaptive.enabled", "true"),
                ("spark.sql.adaptive.coalescePartitions.enabled", "true"),
                ("spark.sql.cbo.enabled", "true"),
                ("spark.sql.cbo.joinReorder.enabled", "true"),
                ("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
                ("spark.sql.execution.arrow.pyspark.enabled", "true"),
                # Federation optimizations
                ("spark.sql.crossJoin.enabled", "true"),
                # Memory settings
                ("spark.driver.memory", "1g"),
                ("spark.driver.maxResultSize", "1g"),
                ("spark.sql.shuffle.partitions", "10"),
                # Enable UI for monitoring
                ("spark.ui.enabled", "true"),
                # JDBC and Kafka driver configuration
                ("spark.jars", "/app/jars/postgresql-42.7.1.jar,/app/jars/spark-sql-kafka-0-10_2.12-3.5.6.jar,/app/jars/kafka-clients-3.5.0.jar,/app/jars/commons-pool2-2.11.1.jar,/app/jars/spark-token-provider-kafka-0-10_2.12-3.5.6.jar"),
                ("spark.driver.extraClassPath", "/app/jars/*"),
                ("spark.executor.extraClassPath", "/app/jars/*"),
                # Event logging for History Server (commented out for now)
                # ("spark.eventLog.enabled", "true"),
                # ("spark.eventLog.dir", "/tmp/spark-events"),
                # Set executor configs for cluster mode
                ("spark.executor.instances", "1" if master_url.startswith("local") else "2"),
                ("spark.cores.max", "2" if master_url.startswith("local") else "4"),
            ])
            
            self.spark = SparkSession.builder.config(conf=conf).getOrCreate()
            self.spark.sparkContext.setLogLevel("WARN")
            
            logger.info("Spark execution engine initialized successfully")
            
        except Exception as e:
            logger.warning(f"Failed to initialize Spark (will continue without it): {e}")
            self.spark = None
    
    def execute_federated_query(self, sql: str, data_sources: Dict[str, BaseConnector]) -> DataFrame:
        """
        Execute a federated SQL query across multiple data sources using Spark.
        
        Args:
            sql: The SQL query to execute
            data_sources: Dictionary of data source connectors
            
        Returns:
            Spark DataFrame with query results
        """
        try:
            # Register each data source as a temporary view in Spark
            for source_name, connector in data_sources.items():
                df = self._load_data_source(connector)
                df.createOrReplaceTempView(source_name)
                logger.info(f"Registered data source '{source_name}' as Spark view")
            
            # Execute the federated query
            result_df = self.spark.sql(sql)
            
            # Optimize the query plan
            result_df = result_df.cache()
            
            logger.info(f"Executed federated query successfully")
            return result_df
            
        except Exception as e:
            logger.error(f"Failed to execute federated query: {e}")
            raise
    
    def _load_data_source(self, connector: BaseConnector) -> DataFrame:
        """Load data from a connector into a Spark DataFrame."""
        
        connector_type = connector.connector_type.value
        
        if connector_type == "postgresql":
            return self._load_jdbc_source(connector)
        elif connector_type == "kafka":
            return self._load_kafka_source(connector)
        elif connector_type == "rest_api":
            return self._load_rest_source(connector)
        else:
            raise ValueError(f"Unsupported connector type: {connector_type}")
    
    def _load_jdbc_source(self, connector: BaseConnector) -> DataFrame:
        """Load data from JDBC source (PostgreSQL, MySQL, etc.)."""
        config = connector.get_connection_params()
        
        return self.spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{config['host']}:{config['port']}/{config['database']}") \
            .option("dbtable", config.get("table", "public.tables")) \
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .load()
    
    def _load_kafka_source(self, connector: BaseConnector) -> DataFrame:
        """Load streaming data from Kafka."""
        config = connector.get_connection_params()
        
        return self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", config["bootstrap_servers"]) \
            .option("subscribe", config["topics"]) \
            .option("startingOffsets", config.get("starting_offsets", "latest")) \
            .load()
    
    def _load_rest_source(self, connector: BaseConnector) -> DataFrame:
        """Load data from REST API source."""
        # REST APIs require custom handling
        # Convert to RDD then DataFrame
        config = connector.get_connection_params()
        data = connector.fetch_data()  # Fetch from REST API
        
        if data:
            rdd = self.spark.sparkContext.parallelize(data)
            return self.spark.createDataFrame(rdd)
        else:
            # Return empty DataFrame with schema
            return self.spark.createDataFrame([], StructType([]))
    
    def execute_streaming_query(self, 
                              source_df: DataFrame, 
                              transformations: List[str],
                              output_mode: str = "append",
                              trigger_interval: str = "10 seconds") -> Any:
        """
        Execute a streaming query with transformations.
        
        Args:
            source_df: Source streaming DataFrame
            transformations: List of SQL transformations to apply
            output_mode: Output mode (append, complete, update)
            trigger_interval: Processing trigger interval
        """
        try:
            # Apply transformations
            transformed_df = source_df
            for transform_sql in transformations:
                source_df.createOrReplaceTempView("stream_data")
                transformed_df = self.spark.sql(transform_sql)
            
            # Start the streaming query
            query = transformed_df.writeStream \
                .outputMode(output_mode) \
                .trigger(processingTime=trigger_interval) \
                .format("memory") \
                .queryName("streaming_results") \
                .start()
            
            return query
            
        except Exception as e:
            logger.error(f"Failed to execute streaming query: {e}")
            raise
    
    def execute_ml_pipeline(self, df: DataFrame, pipeline_config: Dict[str, Any]) -> DataFrame:
        """
        Execute ML pipeline using Spark MLlib.
        
        Args:
            df: Input DataFrame
            pipeline_config: ML pipeline configuration
            
        Returns:
            DataFrame with predictions/transformations
        """
        from pyspark.ml import Pipeline
        from pyspark.ml.feature import VectorAssembler, StandardScaler
        from pyspark.ml.classification import LogisticRegression
        
        try:
            # Example ML pipeline - would be configured based on pipeline_config
            features = pipeline_config.get("features", [])
            target = pipeline_config.get("target", "label")
            
            # Feature engineering
            assembler = VectorAssembler(
                inputCols=features,
                outputCol="features"
            )
            
            # Scaling
            scaler = StandardScaler(
                inputCol="features",
                outputCol="scaled_features"
            )
            
            # Model (example with Logistic Regression)
            model_type = pipeline_config.get("model", "logistic_regression")
            if model_type == "logistic_regression":
                model = LogisticRegression(
                    featuresCol="scaled_features",
                    labelCol=target
                )
            
            # Build pipeline
            pipeline = Pipeline(stages=[assembler, scaler, model])
            
            # Fit and transform
            pipeline_model = pipeline.fit(df)
            predictions = pipeline_model.transform(df)
            
            return predictions
            
        except Exception as e:
            logger.error(f"Failed to execute ML pipeline: {e}")
            raise
    
    def optimize_query_plan(self, df: DataFrame) -> DataFrame:
        """
        Optimize the query execution plan using Spark's Catalyst optimizer.
        
        Args:
            df: DataFrame to optimize
            
        Returns:
            Optimized DataFrame
        """
        # Enable adaptive query execution
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        # Cache frequently accessed data
        df = df.cache()
        
        # Repartition for optimal parallelism
        optimal_partitions = self.spark.sparkContext.defaultParallelism * 2
        df = df.repartition(optimal_partitions)
        
        return df
    
    def federate_across_sources(self, query_plan: Dict[str, Any]) -> DataFrame:
        """
        Core federation logic - execute query across multiple heterogeneous sources.
        This is what makes LaykHaus like Zetaris.
        
        Args:
            query_plan: Parsed and optimized query plan
            
        Returns:
            Unified DataFrame with results from all sources
        """
        # Extract source tables from query plan
        source_tables = query_plan.get("sources", {})
        joins = query_plan.get("joins", [])
        filters = query_plan.get("filters", [])
        aggregations = query_plan.get("aggregations", [])
        
        # Load each source into Spark
        dataframes = {}
        for table_name, source_info in source_tables.items():
            connector = source_info["connector"]
            df = self._load_data_source(connector)
            
            # Apply source-specific filters (predicate pushdown)
            if source_info.get("filters"):
                for filter_expr in source_info["filters"]:
                    df = df.filter(filter_expr)
            
            dataframes[table_name] = df
        
        # Perform joins
        result_df = None
        for join_info in joins:
            left_table = join_info["left"]
            right_table = join_info["right"]
            join_condition = join_info["condition"]
            join_type = join_info.get("type", "inner")
            
            if result_df is None:
                result_df = dataframes[left_table]
            
            result_df = result_df.join(
                dataframes[right_table],
                on=join_condition,
                how=join_type
            )
        
        # Apply post-join filters
        for filter_expr in filters:
            result_df = result_df.filter(filter_expr)
        
        # Apply aggregations
        if aggregations:
            result_df = result_df.groupBy(*aggregations["group_by"]).agg(
                *aggregations["agg_functions"]
            )
        
        # Optimize the final plan
        result_df = self.optimize_query_plan(result_df)
        
        return result_df
    
    def get_execution_plan(self, df: DataFrame) -> str:
        """Get the physical execution plan for debugging/optimization."""
        return df.explain(extended=True)
    
    def stop(self):
        """Stop the Spark session."""
        if self.spark:
            self.spark.stop()
            logger.info("Spark execution engine stopped")