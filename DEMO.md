# 🚀 LaykHaus Demo - Complete Setup Guide

A federated data platform that enables querying across multiple heterogeneous data sources using standard SQL.

## Prerequisites

- Docker Desktop installed and running
- 8GB+ available RAM  
- Ports available: 3000, 8000, 8081, 5432, 9092, 8080

## 🎯 Quick Start - One Command

```bash
# Complete setup with platform and demo data
make demo
```

This single command will:
1. Create environment files from examples
2. Start the LaykHaus platform (Core + UI + Spark)
3. Start demo data services (PostgreSQL, Kafka, REST API)
4. Display connector configurations

**That's it!** The complete platform will be running in 2-3 minutes.

## 📋 Environment Setup

The platform uses minimal environment configuration. Environment files are automatically created from examples when you run `make setup` or `make demo`.

### Core Environment Variables

**`.env`** (created from `.env.example`):
```env
# Environment mode
ENVIRONMENT=development

# Internal Docker network URLs
SPARK_MASTER_URL=spark://laykhaus-spark-master:7077
LAYKHAUS_API_URL=http://laykhaus-core:8000
```

### UI Environment Variables

**`laykhaus-ui/.env.local`** (created from `.env.example`):
```env
# Internal API URL for server-side rendering
LAYKHAUS_INTERNAL_API_URL=http://laykhaus-core:8000

# Public URLs for client-side access
NEXT_PUBLIC_API_URL=http://localhost:8000
NEXT_PUBLIC_GRAPHQL_URL=http://localhost:8000/graphql
NEXT_PUBLIC_WS_URL=ws://localhost:8000/ws
```

### Manual Environment Setup

If you need to set up environment files manually:

```bash
# Create root .env file
cp .env.example .env

# Create UI .env.local file
cp laykhaus-ui/.env.example laykhaus-ui/.env.local

# Or use the make command
make setup
```

## 🚀 Step-by-Step Setup

### Step 1: Start the Platform

```bash
# Option A: Start everything at once (recommended)
make demo

# Option B: Start services separately
make start        # Start platform (Core + UI + Spark)
make data-start   # Start demo data services
```

**Access Points:**
- 🖥️ LaykHaus UI: http://localhost:3000
- 📡 Core API: http://localhost:8000/docs
- 🔗 GraphQL: http://localhost:8000/graphql
- ⚡ Spark UI: http://localhost:8081

### Step 2: Configure Data Connectors

After starting, the console will display connector configurations. Navigate to http://localhost:3000/connectors and add:

#### PostgreSQL Connector
- **Name**: Solar Energy Database
- **Type**: PostgreSQL
- **Host**: demo-postgres (for Docker) or localhost (for local)
- **Port**: 5432
- **Database**: solar_energy_db
- **Username**: demo_user
- **Password**: demo_password
- **Schema**: solar

#### Kafka Connector
- **Name**: Solar Telemetry Stream
- **Type**: Kafka
- **Brokers**: kafka:29092 (for Docker) or localhost:9092 (for local)
- **Topics**: solar-panel-telemetry,weather-stream,energy-consumption
- **Group ID**: laykhaus-consumer

#### REST API Connector
- **Name**: Solar Analytics API
- **Type**: REST API
- **Base URL**: http://demo-rest-api:8080 (for Docker) or http://localhost:8080 (for local)
- **Auth Type**: None

##### REST API Schema Configuration (Optional)

The REST API connector supports three modes:

1. **Automatic Discovery**: If your API provides OpenAPI/Swagger spec at `/openapi.json` or `/swagger.json`
2. **Manual Schema**: Provide explicit schema in connector configuration
3. **Default**: Basic endpoint discovery without detailed schema

For manual schema configuration in the UI (Advanced tab), use this JSON format:

```json
{
  "endpoints": {
    "panels": {
      "description": "Solar panel data",
      "endpoint": "/api/panels",
      "method": "GET",
      "columns": {
        "panel_id": "integer",
        "serial_number": "varchar",
        "location": "varchar",
        "status": "varchar",
        "capacity_watts": "integer",
        "current_output_watts": "integer",
        "efficiency": "float",
        "temperature": "float",
        "last_updated": "timestamp"
      }
    }
  }
}
```

**Note**: The demo REST API has been simplified to focus on the core panels endpoint that works seamlessly with the federation engine.

Note: When using the API directly, wrap this in `config.extra_params.schema`.

This keeps domain-specific schema separate from the platform core, making LaykHaus truly generic.

### Step 3: Verify Schema Explorer

1. Go to Query Builder: http://localhost:3000/query
2. The Schema Explorer on the left should show:
   - **postgres** → solar → tables (solar_panels, energy_production, etc.)
   - **kafka** → topics (solar-panel-telemetry, weather-stream, energy-consumption, etc.)
   - **rest_api** → endpoints (panels, weather, consumption, etc.)

#### Test Individual Data Sources

**PostgreSQL Query:**
```sql
-- Get active solar panels with their specifications
SELECT panel_id, manufacturer, model, capacity_watts, efficiency_percentage, location_name
FROM postgres.solar.solar_panels 
WHERE status = 'active' 
LIMIT 5;
```

**Kafka Streaming Query:**
```sql
-- Get real-time solar panel telemetry
SELECT panel_id, power_output_watts, temperature_celsius, 
       efficiency_percentage, timestamp
FROM kafka.`solar-panel-telemetry`
LIMIT 5;

-- Get current weather conditions
SELECT station_id, temperature_celsius, solar_radiation_wm2, 
       cloud_cover_percentage, timestamp
FROM kafka.`weather-stream`
LIMIT 5;
```

**REST API Query:**
```sql
-- Get panel data from REST API (simplified demo)
SELECT panel_id, serial_number, location, status,
       capacity_watts, current_output_watts, efficiency, temperature
FROM rest_api.panels
WHERE status = 'active'
LIMIT 10;
```

### Step 4: Execute Federated Queries

#### Simple Federated Query (PostgreSQL + Kafka)
```sql
-- Join static panel data with real-time telemetry
SELECT 
    p.panel_id,
    p.location_name,
    p.capacity_watts,
    k.power_output_watts,
    k.temperature_celsius,
    k.timestamp
FROM postgres.solar.solar_panels p
JOIN kafka.`solar-panel-telemetry` k ON p.panel_id = k.panel_id
WHERE p.status = 'active'
LIMIT 10;
```

#### Complex Aggregation Query
```sql
-- Analyze panel performance by manufacturer
SELECT 
    p.manufacturer,
    COUNT(DISTINCT p.panel_id) as panel_count,
    AVG(k.temperature_celsius) as avg_temperature,
    MAX(k.power_output_watts) as max_power_output,
    AVG(k.efficiency_percentage) as avg_efficiency
FROM postgres.solar.solar_panels p
JOIN kafka.`solar-panel-telemetry` k ON p.panel_id = k.panel_id
WHERE p.status = 'active'
GROUP BY p.manufacturer
ORDER BY panel_count DESC;
```

#### Multi-Source Federation (PostgreSQL + Kafka Weather + Kafka Telemetry)
```sql
-- Correlate panel performance with weather conditions
SELECT 
    k.panel_id,
    k.power_output_watts,
    k.temperature_celsius as panel_temp,
    w.temperature_celsius as ambient_temp,
    w.solar_radiation_wm2,
    w.cloud_cover_percentage
FROM kafka.`solar-panel-telemetry` k
JOIN kafka.`weather-stream` w ON 1=1
WHERE k.panel_id IN (
    SELECT panel_id 
    FROM postgres.solar.solar_panels 
    WHERE manufacturer = 'SunPower'
)
LIMIT 10;
```

#### Customer Energy Analysis (PostgreSQL + Kafka Consumption)
```sql
-- Analyze customer energy usage patterns
SELECT 
    c.customer_id,
    c.name as customer_name,
    c.plan_type,
    ec.current_usage_kw,
    ec.solar_generation_kw,
    ec.battery_charge_percentage,
    ec.cost_per_hour
FROM postgres.solar.customers c
JOIN kafka.`energy-consumption` ec ON c.customer_id = ec.customer_id
WHERE c.status = 'active'
LIMIT 10;
```

### Step 5: Advanced Federation Features

#### Time-Series Analysis
```sql
-- Analyze energy production trends
SELECT 
    DATE(ep.timestamp) as date,
    COUNT(DISTINCT ep.panel_id) as active_panels,
    SUM(ep.power_output_watts) as total_power,
    AVG(ep.efficiency_actual) as avg_efficiency
FROM postgres.solar.energy_production ep
WHERE ep.timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE(ep.timestamp)
ORDER BY date DESC;
```

#### Real-Time Alert Monitoring
```sql
-- Monitor critical alerts with panel details
SELECT 
    a.alert_id,
    p.panel_id,
    p.location_name,
    a.alert_type,
    a.severity,
    a.message,
    a.triggered_at
FROM postgres.solar.alerts a
JOIN postgres.solar.solar_panels p ON a.panel_id = p.panel_id
WHERE a.severity IN ('critical', 'warning')
  AND a.triggered_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
ORDER BY a.triggered_at DESC
LIMIT 20;
```

#### REST API + Kafka Federation
```sql
-- Compare real-time telemetry with REST API panel data
SELECT 
    p.panel_id,
    p.location,
    p.status,
    p.capacity_watts,
    k.power_output_watts as current_power,
    k.temperature_celsius as current_temp,
    p.efficiency as rated_efficiency,
    k.efficiency_percentage as current_efficiency
FROM rest_api.panels p
JOIN kafka.`solar-panel-telemetry` k ON CAST(p.panel_id AS STRING) = k.panel_id
WHERE p.status = 'active'
LIMIT 10;
```

#### Three-Way Federation (PostgreSQL + Kafka + REST API)
```sql
-- Combine static data, streaming data, and REST API data
SELECT 
    pg.panel_id,
    pg.manufacturer,
    pg.installation_date,
    p.location,
    p.status,
    k.power_output_watts as current_power,
    k.temperature_celsius as current_temp,
    p.efficiency as rated_efficiency,
    k.efficiency_percentage as current_efficiency
FROM postgres.solar.solar_panels pg
JOIN rest_api.panels p ON pg.panel_id = CAST(p.panel_id AS INTEGER)
JOIN kafka.`solar-panel-telemetry` k ON CAST(pg.panel_id AS STRING) = k.panel_id
WHERE pg.status = 'active' AND p.status = 'active'
LIMIT 10;
```

#### Performance Benchmarking
```sql
-- Compare REST API data with Kafka streaming data
SELECT 
    'REST API' as source,
    COUNT(*) as record_count,
    AVG(efficiency) as avg_efficiency
FROM rest_api.panels
UNION ALL
SELECT 
    'Kafka Stream' as source,
    COUNT(*) as record_count,
    AVG(efficiency_percentage) as avg_efficiency
FROM kafka.`solar-panel-telemetry`;
```
