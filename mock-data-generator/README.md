# LaykHaus Demo Data Generator 🌟

This is the third component of the LaykHaus platform that generates realistic demo data to showcase the federated query capabilities.

## 📊 What It Generates

### Theme: Solar Energy Monitoring System
A comprehensive IoT/Energy data ecosystem with:

1. **PostgreSQL Database** (`solar_energy_db`)
   - 100 solar panels with specifications
   - 50 customers (residential, commercial, industrial)
   - 5 weather stations
   - Historical energy production data (30 days)
   - Energy consumption patterns
   - Maintenance logs and alerts

2. **Kafka Streaming Topics**
   - `solar-panel-telemetry` - Real-time panel metrics
   - `weather-stream` - Live weather data
   - `energy-consumption` - Customer usage patterns
   - `system-alerts` - System notifications
   - `maintenance-events` - Maintenance updates

3. **REST API Endpoints** (Port 8080)
   - `/api/panels` - Solar panel information
   - `/api/weather/current` - Current weather data
   - `/api/consumption/summary` - Energy consumption stats
   - `/api/analytics/production` - Production analytics
   - `/api/alerts/recent` - Recent system alerts

## 🚀 Quick Start

```bash
# Start all data generators
make start

# Stop all services
make stop

# View logs
make logs

# Clean up everything
make clean
```

## 📡 Data Flow

```
┌─────────────────────────────────────────────────────────┐
│                  Data Generator Service                   │
│                                                          │
│  Generates data every 5 seconds:                        │
│  • Panel telemetry (10 panels/iteration)                │
│  • Weather updates (5 stations)                         │
│  • Consumption data (5 customers)                       │
│  • Random alerts (10% probability)                      │
└──────────┬──────────────┬──────────────┬────────────────┘
           │              │              │
           ▼              ▼              ▼
    ┌──────────┐   ┌──────────┐   ┌──────────┐
    │PostgreSQL│   │  Kafka   │   │ REST API │
    │  Port:   │   │  Port:   │   │  Port:   │
    │   5432   │   │   9092   │   │   8080   │
    └──────────┘   └──────────┘   └──────────┘
```

## 🔗 Connect to LaykHaus

### 1. Configure PostgreSQL Connector in LaykHaus UI
```
Host: localhost
Port: 5432
Database: solar_energy_db
Username: demo_user
Password: demo_password
```

### 2. Configure Kafka Connector
```
Brokers: localhost:9092
Topics: solar-panel-telemetry, weather-stream, energy-consumption
```

### 3. Configure REST API Connector
```
Base URL: http://localhost:8080
Endpoints: /api/panels, /api/weather/current, etc.
```

## 📊 Sample Queries

Once connected, you can run federated queries like:

```sql
-- Real-time panel efficiency with weather correlation
SELECT 
    p.panel_id,
    p.location_name,
    k.power_output_watts as current_power,
    w.solar_radiation_wm2 as solar_radiation,
    w.temperature_celsius as ambient_temp,
    (k.power_output_watts / p.capacity_watts * 100) as efficiency_percentage
FROM 
    postgres.solar.solar_panels p
    JOIN kafka.solar_panel_telemetry k ON p.panel_id = k.panel_id
    JOIN postgres.solar.weather_data w ON w.timestamp = k.timestamp
WHERE 
    k.timestamp > NOW() - INTERVAL '1 hour'
ORDER BY 
    efficiency_percentage DESC;

-- Customer consumption vs solar generation
SELECT 
    c.name as customer_name,
    c.customer_type,
    SUM(cons.consumption_kwh) as total_consumption,
    SUM(prod.total_energy_kwh) as solar_generation,
    (SUM(prod.total_energy_kwh) / SUM(cons.consumption_kwh) * 100) as self_sufficiency
FROM 
    postgres.solar.customers c
    JOIN postgres.solar.customer_panels cp ON c.customer_id = cp.customer_id
    JOIN postgres.solar.daily_production_summary prod ON cp.panel_id = prod.panel_id
    JOIN postgres.solar.energy_consumption cons ON c.customer_id = cons.customer_id
WHERE 
    prod.date >= CURRENT_DATE - 7
GROUP BY 
    c.customer_id, c.name, c.customer_type
ORDER BY 
    self_sufficiency DESC;

-- Real-time alerts with panel status
SELECT 
    a.alert_type,
    a.severity,
    a.message,
    p.serial_number,
    p.location_name,
    rest.current_output_watts,
    rest.temperature
FROM 
    kafka.system_alerts a
    JOIN postgres.solar.solar_panels p ON a.panel_id = p.panel_id
    JOIN rest_api.panels rest ON rest.panel_id = p.panel_id
WHERE 
    a.severity IN ('warning', 'critical')
    AND a.timestamp > NOW() - INTERVAL '1 hour';
```

## 📈 Monitoring

- **PostgreSQL**: Check table sizes and query performance
- **Kafka**: Monitor topics at http://localhost:9092
- **REST API**: Swagger docs at http://localhost:8080/docs

## 🛠️ Configuration

Environment variables in `docker-compose.yml`:
- `GENERATE_INTERVAL_SECONDS`: Data generation frequency (default: 5)
- `POSTGRES_DB`: Database name
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka connection

## 📝 Data Characteristics

- **Realistic patterns**: Solar output follows daylight hours
- **Correlations**: Weather affects panel efficiency
- **Anomalies**: Random alerts and maintenance events
- **Scale**: 100 panels × 24 hours × 30 days = 72,000+ records
- **Streaming**: ~20 events/second across all topics

## 🧪 Testing Integration

1. Start the data generator: `make start`
2. Wait 30 seconds for initialization
3. Open LaykHaus UI at http://localhost:3000
4. Configure the three connectors
5. Run federated queries combining all sources
6. Watch real-time updates in dashboards

## 🐛 Troubleshooting

- **Kafka not connecting**: Wait for Zookeeper to initialize (30s)
- **PostgreSQL errors**: Check if port 5432 is already in use
- **REST API down**: Check logs with `docker logs demo-rest-api`

## 📚 Data Schema

See `postgres/init.sql` for complete database schema including:
- `solar_panels` - Panel specifications
- `energy_production` - Time-series production data
- `weather_data` - Environmental conditions
- `customers` - Customer information
- `energy_consumption` - Usage patterns
- And more...