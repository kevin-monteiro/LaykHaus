"""
Spark configuration for LaykHaus.

Centralizes all Spark-related configuration including version compatibility.
"""

from typing import Dict, List
from dataclasses import dataclass


@dataclass
class SparkConfig:
    """Spark configuration with version compatibility rules."""
    
    # Version compatibility (MUST match exactly)
    SPARK_VERSION = "3.5.6"
    SCALA_VERSION = "2.12"
    
    # Required JAR versions
    SPARK_KAFKA_JAR = f"spark-sql-kafka-0-10_{SCALA_VERSION}-{SPARK_VERSION}.jar"
    SPARK_TOKEN_JAR = f"spark-token-provider-kafka-0-10_{SCALA_VERSION}-{SPARK_VERSION}.jar"
    KAFKA_CLIENTS_JAR = "kafka-clients-3.5.0.jar"  # Can be different version
    POSTGRES_JDBC_JAR = "postgresql-42.7.1.jar"    # Can be different version
    COMMONS_POOL_JAR = "commons-pool2-2.11.1.jar"  # Can be different version
    
    # JAR paths
    JAR_PATH = "/app/jars"
    
    @classmethod
    def get_jar_list(cls) -> List[str]:
        """Get list of required JAR files."""
        return [
            f"{cls.JAR_PATH}/{cls.POSTGRES_JDBC_JAR}",
            f"{cls.JAR_PATH}/{cls.SPARK_KAFKA_JAR}",
            f"{cls.JAR_PATH}/{cls.KAFKA_CLIENTS_JAR}",
            f"{cls.JAR_PATH}/{cls.COMMONS_POOL_JAR}",
            f"{cls.JAR_PATH}/{cls.SPARK_TOKEN_JAR}"
        ]
    
    @classmethod
    def get_spark_conf(cls, master_url: str = None) -> Dict[str, str]:
        """Get Spark configuration dictionary."""
        return {
            "spark.app.name": "LaykHaus-Federation-Engine",
            "spark.master": master_url or "local[2]",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.cbo.enabled": "true",
            "spark.sql.cbo.joinReorder.enabled": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.sql.crossJoin.enabled": "true",
            "spark.driver.memory": "1g",
            "spark.driver.maxResultSize": "1g",
            "spark.sql.shuffle.partitions": "10",
            "spark.ui.enabled": "true",
            "spark.jars": ",".join(cls.get_jar_list()),
            "spark.driver.extraClassPath": f"{cls.JAR_PATH}/*",
            "spark.executor.extraClassPath": f"{cls.JAR_PATH}/*",
        }