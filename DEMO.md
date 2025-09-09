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

### Step 3: Verify Schema Explorer

1. Go to Query Builder: http://localhost:3000/query
2. The Schema Explorer on the left should show:
   - **postgres** → solar → tables (solar_panels, energy_production, etc.)
   - **kafka** → topics (solar_panel_telemetry, weather_stream, etc.)
   - **rest_api** → endpoints (panels, weather_current, etc.)

### Step 4: Execute Federated Queries

Try this example query that joins data from all three sources:

```sql
-- Real-time panel performance with weather correlation
SELECT 
    p.panel_id,
    p.location_name,
    k.power_output_watts as current_power,
    w.temperature_celsius as current_temp,
    w.solar_radiation_wm2 as solar_radiation
FROM postgres.solar.solar_panels p
JOIN kafka.solar_panel_telemetry k ON p.panel_id = k.panel_id
JOIN rest_api.weather_current w ON TRUE
WHERE p.status = 'active'
LIMIT 10;
```
