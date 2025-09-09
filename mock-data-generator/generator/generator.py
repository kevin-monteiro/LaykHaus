#!/usr/bin/env python3
"""
Real-time Data Generator for LaykHaus Demo
Generates continuous data for PostgreSQL, Kafka, and REST API
Theme: Solar Energy Monitoring System
"""

import os
import json
import time
import random
import psycopg2
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError
import requests
from faker import Faker

fake = Faker()

# Configuration from environment
KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'solar_energy_db')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'demo_user')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'demo_password')
REST_API_URL = os.getenv('REST_API_URL', 'http://localhost:8080')
INTERVAL = int(os.getenv('GENERATE_INTERVAL_SECONDS', '5'))

class SolarDataGenerator:
    def __init__(self):
        self.kafka_producer = None
        self.pg_conn = None
        self.panel_ids = []
        self.customer_ids = []
        self.station_ids = []
        self.init_connections()

    def init_connections(self):
        """Initialize all connections with retry logic."""
        print("🚀 Starting Solar Data Generator...")
        
        # Connect to Kafka
        self.connect_kafka()
        
        # Connect to PostgreSQL
        self.connect_postgres()
        
        # Create Kafka topics
        self.create_kafka_topics()
        
        print("✅ All connections established!")

    def connect_kafka(self, retries=10):
        """Connect to Kafka with retry logic."""
        for i in range(retries):
            try:
                print(f"Connecting to Kafka at {KAFKA_SERVERS}...")
                self.kafka_producer = KafkaProducer(
                    bootstrap_servers=KAFKA_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',
                    retries=3
                )
                print("✅ Kafka connected!")
                return
            except Exception as e:
                print(f"❌ Kafka connection attempt {i+1} failed: {e}")
                time.sleep(5)
        raise Exception("Failed to connect to Kafka")

    def connect_postgres(self, retries=10):
        """Connect to PostgreSQL with retry logic."""
        for i in range(retries):
            try:
                print(f"Connecting to PostgreSQL at {POSTGRES_HOST}:{POSTGRES_PORT}...")
                self.pg_conn = psycopg2.connect(
                    host=POSTGRES_HOST,
                    port=POSTGRES_PORT,
                    database=POSTGRES_DB,
                    user=POSTGRES_USER,
                    password=POSTGRES_PASSWORD
                )
                self.pg_conn.autocommit = True
                
                # Load IDs for reference
                cur = self.pg_conn.cursor()
                
                cur.execute("SELECT panel_id FROM solar.solar_panels WHERE status = 'active' LIMIT 50")
                self.panel_ids = [row[0] for row in cur.fetchall()]
                
                cur.execute("SELECT customer_id FROM solar.customers LIMIT 30")
                self.customer_ids = [row[0] for row in cur.fetchall()]
                
                cur.execute("SELECT station_id FROM solar.weather_stations")
                self.station_ids = [row[0] for row in cur.fetchall()]
                
                cur.close()
                
                print(f"✅ PostgreSQL connected! Loaded {len(self.panel_ids)} panels, {len(self.customer_ids)} customers, {len(self.station_ids)} stations")
                return
            except Exception as e:
                print(f"❌ PostgreSQL connection attempt {i+1} failed: {e}")
                time.sleep(5)
        raise Exception("Failed to connect to PostgreSQL")

    def create_kafka_topics(self):
        """Create Kafka topics for streaming data."""
        topics = [
            'solar-panel-telemetry',     # Real-time panel data
            'weather-stream',             # Weather updates
            'energy-consumption',         # Customer consumption
            'system-alerts',              # System alerts
            'maintenance-events'          # Maintenance events
        ]
        
        for topic in topics:
            try:
                # Send a test message to create topic
                self.kafka_producer.send(topic, {'test': 'message'})
                print(f"📊 Created/verified Kafka topic: {topic}")
            except Exception as e:
                print(f"Warning creating topic {topic}: {e}")

    def generate_panel_telemetry(self):
        """Generate real-time solar panel telemetry data."""
        data = []
        current_hour = datetime.now().hour
        is_daylight = 6 <= current_hour <= 18
        
        for panel_id in random.sample(self.panel_ids, min(10, len(self.panel_ids))):
            telemetry = {
                'timestamp': datetime.now().isoformat(),
                'panel_id': panel_id,
                'power_output_watts': random.uniform(200, 400) if is_daylight else 0,
                'voltage': random.uniform(380, 420) if is_daylight else 0,
                'current_amps': random.uniform(5, 15) if is_daylight else 0,
                'temperature_celsius': random.uniform(25, 45),
                'efficiency_percentage': random.uniform(15, 22) if is_daylight else 0,
                'inverter_status': 'active' if is_daylight else 'standby',
                'grid_frequency_hz': random.uniform(49.9, 50.1),
                'daily_energy_kwh': random.uniform(10, 50)
            }
            data.append(telemetry)
            
            # Send to Kafka
            try:
                self.kafka_producer.send('solar-panel-telemetry', telemetry)
            except KafkaError as e:
                print(f"Error sending telemetry to Kafka: {e}")
        
        return data

    def generate_weather_data(self):
        """Generate weather stream data."""
        data = []
        current_hour = datetime.now().hour
        
        for station_id in self.station_ids:
            weather = {
                'timestamp': datetime.now().isoformat(),
                'station_id': station_id,
                'temperature_celsius': random.uniform(20, 40),
                'humidity_percentage': random.uniform(20, 80),
                'wind_speed_ms': random.uniform(0, 20),
                'wind_direction_degrees': random.randint(0, 360),
                'solar_radiation_wm2': random.uniform(500, 1000) if 6 <= current_hour <= 18 else 0,
                'cloud_cover_percentage': random.randint(0, 100),
                'uv_index': random.uniform(0, 11) if 6 <= current_hour <= 18 else 0,
                'visibility_km': random.uniform(5, 20),
                'pressure_hpa': random.uniform(1000, 1030)
            }
            data.append(weather)
            
            # Send to Kafka
            try:
                self.kafka_producer.send('weather-stream', weather)
            except KafkaError as e:
                print(f"Error sending weather to Kafka: {e}")
        
        return data

    def generate_consumption_data(self):
        """Generate energy consumption data."""
        data = []
        
        for customer_id in random.sample(self.customer_ids, min(5, len(self.customer_ids))):
            consumption = {
                'timestamp': datetime.now().isoformat(),
                'customer_id': customer_id,
                'current_usage_kw': random.uniform(0.5, 10),
                'daily_consumption_kwh': random.uniform(10, 100),
                'peak_demand_kw': random.uniform(5, 20),
                'grid_import_kw': random.uniform(0, 5),
                'solar_generation_kw': random.uniform(0, 8),
                'battery_charge_percentage': random.uniform(20, 100),
                'cost_per_hour': random.uniform(0.5, 5)
            }
            data.append(consumption)
            
            # Send to Kafka
            try:
                self.kafka_producer.send('energy-consumption', consumption)
            except KafkaError as e:
                print(f"Error sending consumption to Kafka: {e}")
        
        return data

    def generate_alerts(self):
        """Generate system alerts randomly."""
        if random.random() > 0.9:  # 10% chance of alert
            panel_id = random.choice(self.panel_ids) if self.panel_ids else 1
            
            alert_types = [
                ('low_efficiency', 'warning', 'Panel efficiency below threshold'),
                ('high_temperature', 'warning', 'Panel temperature exceeds safe limit'),
                ('inverter_fault', 'critical', 'Inverter communication lost'),
                ('maintenance_due', 'info', 'Scheduled maintenance required'),
                ('grid_fault', 'critical', 'Grid connection issue detected')
            ]
            
            alert_type, severity, message = random.choice(alert_types)
            
            alert = {
                'timestamp': datetime.now().isoformat(),
                'alert_id': fake.uuid4(),
                'panel_id': panel_id,
                'alert_type': alert_type,
                'severity': severity,
                'message': message,
                'details': {
                    'threshold': random.uniform(10, 20),
                    'actual_value': random.uniform(5, 15),
                    'location': f"Panel {panel_id}"
                }
            }
            
            # Send to Kafka
            try:
                self.kafka_producer.send('system-alerts', alert)
                print(f"🚨 Alert generated: {alert_type} - {message}")
            except KafkaError as e:
                print(f"Error sending alert to Kafka: {e}")
            
            # Also insert into PostgreSQL
            try:
                cur = self.pg_conn.cursor()
                cur.execute("""
                    INSERT INTO solar.alerts (panel_id, alert_type, severity, message, triggered_at)
                    VALUES (%s, %s, %s, %s, %s)
                """, (panel_id, alert_type, severity, message, datetime.now()))
                cur.close()
            except Exception as e:
                print(f"Error inserting alert to PostgreSQL: {e}")

    def update_postgres_data(self, telemetry_data):
        """Update PostgreSQL with latest telemetry."""
        if not telemetry_data:
            return
        
        try:
            cur = self.pg_conn.cursor()
            
            for telemetry in telemetry_data[:5]:  # Insert subset to avoid overload
                cur.execute("""
                    INSERT INTO solar.energy_production 
                    (panel_id, timestamp, power_output_watts, voltage, current_amps, temperature_celsius, efficiency_actual)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    telemetry['panel_id'],
                    datetime.now(),
                    telemetry['power_output_watts'],
                    telemetry['voltage'],
                    telemetry['current_amps'],
                    telemetry['temperature_celsius'],
                    telemetry['efficiency_percentage']
                ))
            
            cur.close()
            print(f"📝 Updated PostgreSQL with {len(telemetry_data[:5])} records")
        except Exception as e:
            print(f"Error updating PostgreSQL: {e}")
            self.connect_postgres()  # Reconnect if needed

    def send_to_rest_api(self, data_type, data):
        """Send data to REST API endpoints."""
        endpoints = {
            'telemetry': f'{REST_API_URL}/api/telemetry',
            'weather': f'{REST_API_URL}/api/weather',
            'consumption': f'{REST_API_URL}/api/consumption',
            'alerts': f'{REST_API_URL}/api/alerts'
        }
        
        if data_type in endpoints and data:
            try:
                response = requests.post(
                    endpoints[data_type],
                    json=data,
                    timeout=5
                )
                if response.status_code == 200:
                    print(f"📡 Sent {len(data)} {data_type} records to REST API")
            except Exception as e:
                print(f"REST API error for {data_type}: {e}")

    def run(self):
        """Main loop to generate continuous data."""
        print("\n🌟 Solar Data Generator is running!")
        print(f"📊 Generating data every {INTERVAL} seconds...")
        print("=" * 50)
        
        iteration = 0
        while True:
            try:
                iteration += 1
                print(f"\n⚡ Iteration {iteration} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                
                # Generate different types of data
                telemetry = self.generate_panel_telemetry()
                weather = self.generate_weather_data()
                consumption = self.generate_consumption_data()
                
                # Random alerts
                self.generate_alerts()
                
                # Update PostgreSQL
                self.update_postgres_data(telemetry)
                
                # Send to REST API
                self.send_to_rest_api('telemetry', telemetry)
                self.send_to_rest_api('weather', weather)
                self.send_to_rest_api('consumption', consumption)
                
                # Flush Kafka producer
                self.kafka_producer.flush()
                
                print(f"✅ Generated: {len(telemetry)} telemetry, {len(weather)} weather, {len(consumption)} consumption records")
                
                # Sleep before next iteration
                time.sleep(INTERVAL)
                
            except KeyboardInterrupt:
                print("\n👋 Shutting down generator...")
                break
            except Exception as e:
                print(f"❌ Error in main loop: {e}")
                time.sleep(INTERVAL)

    def cleanup(self):
        """Clean up connections."""
        if self.kafka_producer:
            self.kafka_producer.close()
        if self.pg_conn:
            self.pg_conn.close()
        print("🔚 Generator stopped")

if __name__ == "__main__":
    generator = SolarDataGenerator()
    try:
        generator.run()
    finally:
        generator.cleanup()