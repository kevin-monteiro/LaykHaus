# 🚀 LaykHaus - Federated Data Platform

LaykHaus is a modern federated data platform that enables seamless querying across multiple heterogeneous data sources using standard SQL. It provides a unified interface for accessing data from databases, streaming platforms, REST APIs, and more - all through a single query interface.

## 🌟 Key Features

- **🔗 Federated Query Engine**: Query multiple data sources with a single SQL statement
- **⚡ Apache Spark Integration**: Distributed processing for large-scale analytics
- **📊 Visual Query Builder**: User-friendly interface with Schema Explorer
- **🎨 Modern UI**: React/Next.js interface with real-time updates
- **🔌 Extensible Connectors**: PostgreSQL, Kafka, REST API with intelligent connection testing
- **🔗 GraphQL Interface**: GraphQL API for flexible data queries (future migration path)

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
│  • React Query for state management                         │
│  • Inline notification system                               │
│  • Streamlined connector management                         │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                  LaykHaus Core (FastAPI)                    │
│                  http://localhost:8000                      │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  • Federation Engine (DataFrames)                   │    │
│  │  • Connection Manager & Factory                     │    │
│  │  • REST API Routes (/api/v1/*)                     │    │
│  │  • Real-time Data Provider                         │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              Apache Spark Execution Engine                  │
│                  http://localhost:8081                      │
│  • DataFrame-based federated queries                        │
│  • Catalyst optimizer & Tungsten execution                  │
│  • Cross-source JOIN operations                             │
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
├── laykhaus-core/              # Core federation engine (FastAPI)
│   ├── src/laykhaus/
│   │   ├── api/                # API routes and endpoints
│   │   │   └── routes/         # Modular route handlers
│   │   ├── app.py              # FastAPI application entry point
│   │   ├── connectors/         # Data source connector implementations
│   │   │   ├── base.py         # Base connector interfaces
│   │   │   ├── connection_manager.py  # Connection lifecycle management
│   │   │   └── factory.py      # Connector factory pattern
│   │   ├── core/               # Core configuration and utilities
│   │   ├── federation/         # Query federation and execution
│   │   │   ├── data_provider.py        # Real-time data provider
│   │   │   └── spark_federated_executor.py  # Spark execution engine
│   │   └── integrations/       # External service integrations
│   │       └── spark/          # Apache Spark integration
│   └── Dockerfile
│
├── laykhaus-ui/                # Frontend application (Next.js 14)
│   ├── app/                    # Next.js app router
│   ├── components/
│   │   ├── connectors/         # Connector management UI
│   │   │   └── ConnectorDialog.tsx  # Single-page connector form
│   │   ├── query/              # Query builder interface
│   │   ├── layout/             # Application layout
│   │   └── ui/                 # Reusable UI components
│   │       └── inline-notification.tsx  # Contextual notifications
│   ├── lib/
│   │   ├── api/                # API client utilities
│   │   ├── hooks/              # React Query hooks
│   │   │   └── useConnectors.ts    # Connector state management
│   │   └── types/              # TypeScript type definitions
│   └── Dockerfile
│
├── mock-data-generator/        # Demo data services
│   ├── postgres/               # PostgreSQL with sample data
│   ├── rest-api/               # Mock REST API service
│   └── generator/              # Kafka data generator
│
├── docker-compose.yml          # Main platform services
├── docker-compose.data.yml     # Demo data services
├── Makefile                    # Development commands
└── README.md                   # Documentation
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
# Connector Management
GET  /api/v1/connectors              # List all connectors with status
POST /api/v1/connectors              # Create new connector
GET  /api/v1/connectors/{id}         # Get connector details
PUT  /api/v1/connectors/{id}         # Update connector configuration
DELETE /api/v1/connectors/{id}       # Remove connector
POST /api/v1/connectors/{id}/test    # Test connector connection
GET  /api/v1/connectors/{id}/schema  # Get connector schema
GET  /api/v1/connectors/stats        # Get connector statistics

# Query Execution
POST /api/v1/query/execute           # Execute federated SQL query
GET  /api/v1/catalog/schemas         # Get available schemas

# Health & Monitoring
GET  /health                         # Platform health check
```

