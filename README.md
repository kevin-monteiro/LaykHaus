# 🚀 LaykHaus - Federated Data Platform

LaykHaus is a modern federated data platform that enables seamless querying across multiple heterogeneous data sources using standard SQL. It provides a unified interface for accessing data from databases, streaming platforms, REST APIs, and more - all through a single query interface.

## 🌟 Key Features

- **🔗 Federated Query Engine**: Query multiple data sources with a single SQL statement
- **🎯 Smart Query Optimization**: Automatic query pushdown and optimization
- **🌊 Real-time Streaming**: Native support for Kafka and streaming data
- **📊 Visual Query Builder**: User-friendly interface with Schema Explorer
- **🔐 Enterprise Security**: Built-in RBAC and data masking capabilities
- **⚡ Apache Spark Integration**: Distributed processing for large-scale analytics
- **🎨 Modern UI**: React/Next.js interface with real-time updates
- **🔌 Extensible Connectors**: PostgreSQL, Kafka, REST API, and more

## ⚡ Quick Start

```bash
# 1. Clone the repository
git clone git@github.com:kevin-monteiro/LaykHaus.git
cd LaykHaus

# 2. Start everything with one command
make demo

# 3. Access the UI
# Open http://localhost:3000 in your browser
```

**That's it!** The complete platform with demo data will be running in 2-3 minutes.

## 🌐 Access Points

- **🖥️ LaykHaus UI**: http://localhost:3000
- **📡 Core API**: http://localhost:8000/docs
- **🔗 GraphQL**: http://localhost:8000/graphql
- **⚡ Spark UI**: http://localhost:8081

## 📦 Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     LaykHaus UI (Next.js)                   │
│                   http://localhost:3000                     │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                  LaykHaus Core (FastAPI)                    │
│                  http://localhost:8000                      │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  • Federation Engine  • Connector Manager           │    │
│  │  • REST Gateway       • GraphQL Gateway             │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              Apache Spark Execution Engine                  │
│                  http://localhost:8081                      │
└─────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  PostgreSQL  │     │    Kafka     │     │   REST API   │
│   Database   │     │   Streams    │     │   Endpoints  │
└──────────────┘     └──────────────┘     └──────────────┘
```

## 📋 Available Commands

### Quick Start
```bash
make demo          # Complete setup (platform + demo data)
make demo-stop     # Stop everything
make demo-clean    # Clean everything
```

### Platform Management
```bash
make setup         # Initial setup (create env files)
make start         # Start LaykHaus platform
make stop          # Stop platform
make restart       # Restart platform
make clean         # Clean up containers and data
```

### Demo Data Services
```bash
make data-start    # Start demo data services
make data-stop     # Stop demo data services
make data-clean    # Clean demo data
make data-logs     # View demo data logs
```

### Monitoring & Debugging
```bash
make status        # Check service status
make health        # Check platform health
make logs          # View all logs
make logs-core     # View core logs
make logs-ui       # View UI logs
make logs-spark    # View Spark logs
```

## 🔧 Configuration

### Environment Setup

The platform uses minimal environment configuration. Run `make setup` to create env files from examples:

**`.env`** (automatically created)
```env
ENVIRONMENT=development
SPARK_MASTER_URL=spark://laykhaus-spark-master:7077
LAYKHAUS_API_URL=http://laykhaus-core:8000
```

**`laykhaus-ui/.env.local`** (automatically created)
```env
LAYKHAUS_INTERNAL_API_URL=http://laykhaus-core:8000
NEXT_PUBLIC_API_URL=http://localhost:8000
NEXT_PUBLIC_GRAPHQL_URL=http://localhost:8000/graphql
NEXT_PUBLIC_WS_URL=ws://localhost:8000/ws
```

## 💻 Using the Platform

### Step 1: Add Data Connectors

After starting the platform, navigate to http://localhost:3000/connectors and add these demo connectors:

#### PostgreSQL Connector
- **Name**: Solar Energy Database
- **Type**: PostgreSQL
- **Host**: demo-postgres
- **Port**: 5432
- **Database**: solar_energy_db
- **Username**: demo_user
- **Password**: demo_password
- **Schema**: solar

#### Kafka Connector
- **Name**: Solar Telemetry Stream
- **Type**: Kafka
- **Brokers**: kafka:29092
- **Topics**: solar-panel-telemetry,weather-stream,energy-consumption
- **Group ID**: laykhaus-consumer

#### REST API Connector
- **Name**: Solar Analytics API
- **Type**: REST API
- **Base URL**: http://demo-rest-api:8080
- **Auth Type**: None

### Step 2: Explore Your Data

1. Go to **Query Builder** (http://localhost:3000/query)
2. Use the **Schema Explorer** on the left to browse available tables
3. Click on any table to see its columns
4. Double-click to add tables to your query

### Step 3: Execute Federated Queries

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

More example queries:

```sql
-- Simple PostgreSQL query
SELECT * FROM postgres.solar.solar_panels LIMIT 5;

-- Kafka streaming data
SELECT * FROM kafka.solar_panel_telemetry LIMIT 10;

-- REST API data
SELECT * FROM rest_api.panels;
```

## 📊 Demo Data Services

The platform includes a complete solar energy management system demo:

### PostgreSQL Database
- **12 tables** with historical data
- Solar panels, energy production, weather data
- Customer information and maintenance logs
- ~100K+ sample records

### Kafka Streaming
- **3 real-time topics**:
  - `solar-panel-telemetry`: Live panel metrics
  - `weather-stream`: Real-time weather updates
  - `energy-consumption`: Energy usage events
- Continuous data generation

### REST API
- Current weather conditions
- Panel status endpoints
- Consumption summaries
- Real-time updates every 5 seconds

## 🏗️ Project Structure

```
LaykHaus/
├── laykhaus-core/          # Core federation engine
│   ├── src/laykhaus/       # Python source code
│   │   ├── federation/     # Query federation logic
│   │   ├── connectors/     # Data source connectors
│   │   ├── streaming/      # Kafka & Spark integration
│   │   ├── ml/             # ML framework
│   │   └── security/       # RBAC & data masking
│   └── Dockerfile          # Production Dockerfile
│
├── laykhaus-ui/            # React/Next.js UI
│   ├── app/                # Next.js app directory
│   ├── components/         # React components
│   │   ├── connectors/     # Connector management
│   │   ├── query/          # Query builder
│   │   └── layout/         # Layout components
│   └── Dockerfile          # Production Dockerfile
│
├── mock-data-generator/    # Demo data services
│   ├── postgres/           # PostgreSQL with sample data
│   ├── rest-api/           # Mock REST API service
│   └── generator/          # Kafka data generator
│
├── docker-compose.yml      # Main platform services
├── docker-compose.data.yml # Demo data services
├── Makefile                # Convenience commands
├── .gitignore              # Git ignore rules
├── .dockerignore           # Docker ignore rules
└── README.md               # This file
```

## 🐛 Troubleshooting

### Platform won't start
```bash
# Clean everything and restart
make demo-clean
make demo
```

### Can't see data in Schema Explorer
1. Ensure connectors are added via UI
2. Check connector status (green = connected)
3. Refresh the page
4. Check logs: `make logs-core`

### Port conflicts
Ensure these ports are available:
- 3000 (UI)
- 8000 (Core API)
- 8081 (Spark UI)
- 5432 (PostgreSQL)
- 9092 (Kafka)
- 8080 (REST API)

### Low memory issues
- Ensure Docker has at least 8GB RAM allocated
- Stop unnecessary services: `make stop`
- Use `docker system prune` to free space

## 📚 API Documentation

### REST API
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

### GraphQL
- **Playground**: http://localhost:8000/graphql
- Schema documentation available in playground

### Key Endpoints
```
GET  /api/v1/connectors        # List all connectors
POST /api/v1/connectors        # Add new connector
POST /api/v1/query/execute     # Execute federated query
GET  /api/v1/catalog/schemas   # Get schema information
GET  /health                   # Platform health check
```

