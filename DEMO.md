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

## 🔧 Service Management

### Platform Commands
```bash
make setup         # Create env files from examples
make start         # Start LaykHaus platform
make stop          # Stop platform
make restart       # Restart platform
make clean         # Clean up containers and data
```

### Demo Data Commands
```bash
make data-start    # Start demo data services
make data-stop     # Stop demo data services
make data-clean    # Clean demo data
make data-logs     # View demo data logs
```

### Quick Commands
```bash
make demo          # Complete setup (platform + data)
make demo-stop     # Stop everything
make demo-clean    # Clean everything
```

### Monitoring Commands
```bash
make status        # Check service status
make health        # Check platform health
make logs          # View all logs
make logs-core     # View core logs
make logs-ui       # View UI logs
make logs-spark    # View Spark logs
```

## 🐛 Troubleshooting

### Environment Issues

**Problem**: Services can't connect to each other
```bash
# Verify environment files exist
ls -la .env
ls -la laykhaus-ui/.env.local

# Recreate environment files
make setup

# Restart everything
make demo-clean
make demo
```

### Port Conflicts

**Problem**: Services fail to start due to port conflicts
```bash
# Check for conflicting services
lsof -i :3000  # UI port
lsof -i :8000  # API port
lsof -i :8081  # Spark UI port
lsof -i :5432  # PostgreSQL port
lsof -i :9092  # Kafka port

# Kill conflicting processes or change ports in docker-compose.yml
```

### Schema Explorer Empty

**Problem**: No schemas visible in Query Builder
1. Verify connectors are added and showing green status
2. Check connector logs: `make logs-core`
3. Refresh the page (Ctrl+R or Cmd+R)
4. Verify demo data services are running: `make status`

### Connection Errors

**Problem**: Cannot connect to services
```bash
# Ensure all services are healthy
make health

# Check Docker network
docker network ls | grep laykhaus

# Restart with clean state
make demo-clean
make demo
```

### Memory Issues

**Problem**: Services crashing or slow performance
- Ensure Docker Desktop has at least 8GB RAM allocated
- Go to Docker Desktop → Settings → Resources → Memory
- Increase to 8GB or more
- Restart Docker Desktop

## 📊 Demo Data Overview

### PostgreSQL Database
- **12 tables** with historical solar energy data
- ~100K+ sample records
- Tables include: solar_panels, energy_production, weather_data, customers, maintenance_logs

### Kafka Streaming
- **3 real-time topics**:
  - `solar-panel-telemetry`: Live panel metrics (every 5 seconds)
  - `weather-stream`: Real-time weather updates
  - `energy-consumption`: Energy usage events

### REST API
- Current weather conditions endpoint
- Panel status endpoint
- Consumption summaries endpoint
- Updates every 5 seconds

## 🎯 Demo Scenarios

### Scenario 1: Basic Connector Test
```sql
-- Test PostgreSQL
SELECT COUNT(*) FROM postgres.solar.solar_panels;

-- Test Kafka
SELECT * FROM kafka.solar_panel_telemetry LIMIT 5;

-- Test REST API
SELECT * FROM rest_api.weather_current;
```

### Scenario 2: Federated Query
```sql
-- Join historical and real-time data
SELECT 
    p.panel_id,
    p.capacity_kw,
    AVG(k.power_output_watts) as avg_output,
    COUNT(k.panel_id) as reading_count
FROM postgres.solar.solar_panels p
LEFT JOIN kafka.solar_panel_telemetry k ON p.panel_id = k.panel_id
GROUP BY p.panel_id, p.capacity_kw
LIMIT 10;
```

### Scenario 3: Complex Analytics
```sql
-- Panel efficiency with weather correlation
SELECT 
    p.location_name,
    COUNT(DISTINCT p.panel_id) as panel_count,
    AVG(k.power_output_watts) as avg_power,
    AVG(k.efficiency_percentage) as avg_efficiency,
    w.temperature_celsius,
    w.solar_radiation_wm2
FROM postgres.solar.solar_panels p
JOIN kafka.solar_panel_telemetry k ON p.panel_id = k.panel_id
JOIN rest_api.weather_current w ON TRUE
WHERE p.status = 'active'
GROUP BY p.location_name, w.temperature_celsius, w.solar_radiation_wm2
ORDER BY avg_efficiency DESC
LIMIT 20;
```

## 📚 Additional Resources

- **API Documentation**: http://localhost:8000/docs
- **GraphQL Playground**: http://localhost:8000/graphql
- **Spark Monitoring**: http://localhost:8081
- **Main README**: [README.md](./README.md)

## 🛠️ Development Tips

### Viewing Logs
```bash
# Follow all logs
make logs

# Follow specific service
docker-compose logs -f laykhaus-core
docker-compose logs -f laykhaus-ui
docker-compose logs -f spark-master
```

### Rebuilding Services
```bash
# Rebuild and restart specific service
docker-compose up -d --build laykhaus-core

# Rebuild everything
make clean
make demo
```

### Data Persistence
- Connector configurations are stored in `/data/connectors.json`
- This file persists across container restarts
- To reset connectors: `rm /data/connectors.json` and restart

## ⚡ Performance Tips

1. **Query Optimization**
   - Use LIMIT clauses for initial testing
   - Add appropriate WHERE conditions
   - Leverage query pushdown for filtering

2. **Resource Management**
   - Monitor Spark UI for job execution
   - Check memory usage: `docker stats`
   - Scale Spark workers if needed

3. **Streaming Performance**
   - Kafka consumer groups enable parallel processing
   - Adjust batch sizes for throughput vs latency

## 🔐 Security Notes

- Default setup is for demo/development only
- All services use default credentials
- For production:
  - Enable authentication on all services
  - Use secure passwords
  - Configure TLS/SSL
  - Implement proper RBAC

---

**Quick Reference Card:**

| Command | Description |
|---------|-------------|
| `make demo` | Complete setup with demo data |
| `make setup` | Create environment files |
| `make start` | Start platform only |
| `make data-start` | Start demo data only |
| `make status` | Check all services |
| `make health` | Health check |
| `make logs` | View all logs |
| `make demo-stop` | Stop everything |
| `make demo-clean` | Clean everything |

**Need Help?**
- Check logs: `make logs-core`
- Verify status: `make status`
- Clean restart: `make demo-clean && make demo`