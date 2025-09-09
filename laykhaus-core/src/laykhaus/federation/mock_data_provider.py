"""
Mock data provider for demo/testing purposes.
This should only be used when USE_MOCK_DATA=true in environment.
"""

import random
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
import structlog

logger = structlog.get_logger(__name__)


class MockDataProvider:
    """Provides mock data for federation demos and testing."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    def register_all_mock_sources(self) -> Dict[str, DataFrame]:
        """
        Register all mock data sources as Spark views.
        Returns a dictionary of view_name -> DataFrame for reference.
        """
        views = {}
        
        # Register PostgreSQL mock data
        views.update(self._register_postgres_mock_data())
        
        # Register Kafka mock data
        views.update(self._register_kafka_mock_data())
        
        # Register REST API mock data
        views.update(self._register_rest_mock_data())
        
        logger.info(f"Registered {len(views)} mock data views")
        return views
    
    def _register_postgres_mock_data(self) -> Dict[str, DataFrame]:
        """Create mock PostgreSQL tables."""
        views = {}
        
        # Mock solar_panels table
        solar_panels_data = [
            ("PANEL001", "Solar Farm A", "SolarTech", "ST-5000", 5.0, "2023-01-15", "active"),
            ("PANEL002", "Solar Farm B", "SolarTech", "ST-5000", 5.0, "2023-02-20", "active"),
            ("PANEL003", "Solar Farm A", "GreenPower", "GP-4500", 4.5, "2023-03-10", "active"),
            ("PANEL004", "Solar Farm C", "SolarTech", "ST-5000", 5.0, "2023-04-05", "maintenance"),
            ("PANEL005", "Solar Farm B", "GreenPower", "GP-4500", 4.5, "2023-05-12", "active"),
        ]
        
        panels_df = self.spark.createDataFrame(
            solar_panels_data,
            ["panel_id", "location_name", "manufacturer", "model", "capacity_kw", "installation_date", "status"]
        )
        panels_df.createOrReplaceTempView("postgres_solar_solar_panels")
        views["postgres_solar_solar_panels"] = panels_df
        
        # Mock customers table
        customers_data = [
            ("CUST001", "Alice Johnson", "alice@example.com", "2023-01-01"),
            ("CUST002", "Bob Smith", "bob@example.com", "2023-01-15"),
            ("CUST003", "Charlie Brown", "charlie@example.com", "2023-02-01"),
        ]
        
        customers_df = self.spark.createDataFrame(
            customers_data,
            ["customer_id", "name", "email", "created_at"]
        )
        customers_df.createOrReplaceTempView("postgres_solar_customers")
        views["postgres_solar_customers"] = customers_df
        
        # Mock energy_production table
        energy_production_data = [
            ("PANEL001", "2024-01-01", 450.5, 0.92),
            ("PANEL002", "2024-01-01", 420.3, 0.89),
            ("PANEL003", "2024-01-01", 410.0, 0.91),
            ("PANEL001", "2024-01-02", 460.2, 0.93),
            ("PANEL002", "2024-01-02", 425.1, 0.90),
        ]
        
        production_df = self.spark.createDataFrame(
            energy_production_data,
            ["panel_id", "date", "energy_kwh", "efficiency"]
        )
        production_df.createOrReplaceTempView("postgres_solar_energy_production")
        views["postgres_solar_energy_production"] = production_df
        
        logger.info(f"Registered {len(views)} PostgreSQL mock tables")
        return views
    
    def _register_kafka_mock_data(self) -> Dict[str, DataFrame]:
        """Create mock Kafka streaming data."""
        views = {}
        
        # Mock solar panel telemetry (simulating real-time data)
        telemetry_data = []
        for panel_id in ["PANEL001", "PANEL002", "PANEL003", "PANEL004", "PANEL005"]:
            power = 0.0 if panel_id == "PANEL004" else 4000 + random.uniform(-500, 500)
            efficiency = 0.0 if panel_id == "PANEL004" else 0.85 + random.uniform(-0.05, 0.05)
            telemetry_data.append((
                panel_id,
                power,
                efficiency,
                "2025-09-09T13:00:00"
            ))
        
        telemetry_df = self.spark.createDataFrame(
            telemetry_data,
            ["panel_id", "power_output_watts", "efficiency", "timestamp"]
        )
        telemetry_df.createOrReplaceTempView("kafka_solar_panel_telemetry")
        views["kafka_solar_panel_telemetry"] = telemetry_df
        
        # Mock events stream
        events_data = [
            ("EVT001", "panel_alert", '{"panel_id": "PANEL004", "alert": "maintenance"}', "2025-09-09T12:00:00"),
            ("EVT002", "production_update", '{"total_kwh": 2500.5}', "2025-09-09T12:30:00"),
            ("EVT003", "weather_change", '{"condition": "cloudy"}', "2025-09-09T13:00:00"),
        ]
        
        events_df = self.spark.createDataFrame(
            events_data,
            ["event_id", "event_type", "payload", "timestamp"]
        )
        events_df.createOrReplaceTempView("kafka_events")
        views["kafka_events"] = events_df
        
        logger.info(f"Registered {len(views)} Kafka mock streams")
        return views
    
    def _register_rest_mock_data(self) -> Dict[str, DataFrame]:
        """Create mock REST API data."""
        views = {}
        
        # Mock weather current endpoint
        weather_data = [
            (25.3, 65.0, 850.5, 5.2),
        ]
        
        weather_df = self.spark.createDataFrame(
            weather_data,
            ["temperature_celsius", "humidity_percent", "solar_radiation_wm2", "wind_speed_ms"]
        )
        weather_df.createOrReplaceTempView("rest_api_weather_current")
        views["rest_api_weather_current"] = weather_df
        
        # Mock panel status endpoint
        panel_status_data = [
            ("PANEL001", "operational", 4509.9, 25.1, "2025-09-09T13:00:00"),
            ("PANEL002", "operational", 4281.5, 24.8, "2025-09-09T13:00:00"),
            ("PANEL003", "operational", 4143.6, 25.3, "2025-09-09T13:00:00"),
            ("PANEL004", "maintenance", 0.0, 23.0, "2025-09-09T13:00:00"),
            ("PANEL005", "operational", 4310.9, 25.0, "2025-09-09T13:00:00"),
        ]
        
        panel_status_df = self.spark.createDataFrame(
            panel_status_data,
            ["panel_id", "status", "current_output", "temperature", "last_update"]
        )
        panel_status_df.createOrReplaceTempView("rest_api_panels")
        views["rest_api_panels"] = panel_status_df
        
        logger.info(f"Registered {len(views)} REST API mock endpoints")
        return views