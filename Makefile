.PHONY: help start stop clean setup

# Default target
help: ## Show available commands
	@echo '🚀 LaykHaus Federated Data Platform Commands'
	@echo ''
	@echo '🔧 SETUP:'
	@echo '  setup          - Initial setup (create env files from examples)'
	@echo ''
	@echo '📦 MAIN COMMANDS:'
	@echo '  start          - Start LaykHaus platform (Core + UI + Spark)'
	@echo '  stop           - Stop LaykHaus platform'
	@echo '  restart        - Restart LaykHaus platform'
	@echo '  clean          - Clean up containers and data'
	@echo ''
	@echo '📊 DEMO DATA SERVICES:'
	@echo '  data-start     - Start demo data services (PostgreSQL, Kafka, REST API)'
	@echo '  data-stop      - Stop demo data services'
	@echo '  data-clean     - Clean up demo data services'
	@echo '  data-logs      - View demo data service logs'
	@echo ''
	@echo '🔍 MONITORING:'
	@echo '  logs           - View all logs'
	@echo '  logs-core      - View LaykHaus core logs'
	@echo '  logs-ui        - View LaykHaus UI logs'
	@echo '  logs-spark     - View Spark logs'
	@echo '  status         - Check service status'
	@echo '  health         - Check platform health'
	@echo ''
	@echo '🌐 ACCESS POINTS:'
	@echo '  LaykHaus UI:   http://localhost:3000'
	@echo '  Core API:      http://localhost:8000/docs'
	@echo '  GraphQL:       http://localhost:8000/graphql'
	@echo '  Spark UI:      http://localhost:8081'

# =============================================================================
# SETUP COMMANDS
# =============================================================================

setup: ## Initial setup - create env files from examples
	@echo "🔧 Setting up LaykHaus environment..."
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "✅ Created .env from .env.example"; \
	else \
		echo "⚠️  .env already exists, skipping..."; \
	fi
	@if [ ! -f laykhaus-ui/.env.local ]; then \
		cp laykhaus-ui/.env.example laykhaus-ui/.env.local; \
		echo "✅ Created laykhaus-ui/.env.local from .env.example"; \
	else \
		echo "⚠️  laykhaus-ui/.env.local already exists, skipping..."; \
	fi
	@echo "✅ Setup complete! You can now run 'make start'"

# =============================================================================
# MAIN PLATFORM COMMANDS
# =============================================================================

start: ## Start LaykHaus platform (Core + UI + Spark)
	@echo "🚀 Starting LaykHaus Platform..."
	@docker-compose up -d --build
	@echo "⏳ Waiting for services to initialize..."
	@sleep 10
	@echo "✅ Platform ready!"
	@echo ""
	@echo "📍 Access Points:"
	@echo "   UI:          http://localhost:3000"
	@echo "   API:         http://localhost:8000/docs"
	@echo "   GraphQL:     http://localhost:8000/graphql"
	@echo "   Spark UI:    http://localhost:8081"
	@echo ""
	@echo "💡 To start demo data services, run: make data-start"

stop: ## Stop LaykHaus platform
	@echo "🛑 Stopping LaykHaus Platform..."
	@docker-compose down
	@echo "✅ Platform stopped"

restart: ## Restart LaykHaus platform
	@echo "🔄 Restarting LaykHaus Platform..."
	@docker-compose restart
	@echo "✅ Platform restarted"

clean: ## Clean up containers and data
	@echo "🧹 Cleaning up containers, volumes, and generated files..."
	@docker-compose down -v
	@docker-compose -f docker-compose.data.yml down -v 2>/dev/null || true
	@rm -rf laykhaus-core/__pycache__ laykhaus-core/src/laykhaus/__pycache__
	@rm -rf laykhaus-ui/.next laykhaus-ui/node_modules
	@rm -f /data/connectors.json
	@echo "✅ Cleanup complete"

# =============================================================================
# DEMO DATA SERVICES COMMANDS
# =============================================================================

data-start: ## Start demo data services
	@echo "📊 Starting Demo Data Services..."
	@docker-compose -f docker-compose.data.yml up -d --build
	@echo "⏳ Waiting for services to initialize..."
	@sleep 15
	@echo "✅ Demo data services started!"
	@echo ""
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "📋 CONNECTOR CONFIGURATIONS FOR LAYKHAUS UI:"
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo ""
	@echo "🐳 DOCKER ENVIRONMENT (Container-to-Container):"
	@echo ""
	@echo "1️⃣  PostgreSQL Connector:"
	@echo "   Name:        Solar Energy Database"
	@echo "   Type:        PostgreSQL"
	@echo "   Host:        demo-postgres"
	@echo "   Port:        5432"
	@echo "   Database:    solar_energy_db"
	@echo "   Username:    demo_user"
	@echo "   Password:    demo_password"
	@echo "   Schema:      solar"
	@echo ""
	@echo "2️⃣  Kafka Connector:"
	@echo "   Name:        Solar Telemetry Stream"
	@echo "   Type:        Kafka"
	@echo "   Brokers:     kafka:29092"
	@echo "   Topics:      solar-panel-telemetry,weather-stream,energy-consumption"
	@echo "   Group ID:    laykhaus-consumer"
	@echo ""
	@echo "3️⃣  REST API Connector:"
	@echo "   Name:        Solar Analytics API"
	@echo "   Type:        REST API"
	@echo "   Base URL:    http://demo-rest-api:8080"
	@echo "   Auth Type:   None"
	@echo ""
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo ""
	@echo "🌐 Go to: http://localhost:3000/connectors"
	@echo "📝 Click 'Add Connector' and enter the configurations above"

data-stop: ## Stop demo data services
	@echo "🛑 Stopping demo data services..."
	@docker-compose -f docker-compose.data.yml down
	@echo "✅ Demo data services stopped"

data-clean: ## Clean up demo data services
	@echo "🧹 Cleaning up demo data services..."
	@docker-compose -f docker-compose.data.yml down -v
	@echo "✅ Demo data services cleaned"

data-logs: ## View demo data service logs
	@docker-compose -f docker-compose.data.yml logs -f

# =============================================================================
# MONITORING COMMANDS
# =============================================================================

logs: ## View all logs
	@docker-compose logs -f

logs-core: ## View LaykHaus core logs
	@docker-compose logs -f laykhaus-core

logs-ui: ## View LaykHaus UI logs
	@docker-compose logs -f laykhaus-ui

logs-spark: ## View Spark logs
	@docker-compose logs -f spark-master spark-worker-1

status: ## Check service status
	@echo "📊 LaykHaus Platform Status:"
	@echo "============================"
	@docker-compose ps
	@echo ""
	@echo "📊 Demo Data Services Status:"
	@echo "============================="
	@docker-compose -f docker-compose.data.yml ps 2>/dev/null || echo "Demo data services not running"
	@echo ""
	@echo "🌐 Network Status:"
	@docker network ls | grep laykhaus || echo "No LaykHaus networks found"

health: ## Check platform health
	@echo "🏥 Checking LaykHaus Platform Health..."
	@echo ""
	@echo "Core API Health:"
	@curl -s http://localhost:8000/health | jq '.' 2>/dev/null || echo "❌ Core API not responding"
	@echo ""
	@echo "UI Health:"
	@curl -s -o /dev/null -w "Status: %{http_code}\n" http://localhost:3000 2>/dev/null || echo "❌ UI not responding"
	@echo ""
	@echo "Spark Master Health:"
	@curl -s -o /dev/null -w "Status: %{http_code}\n" http://localhost:8081 2>/dev/null || echo "❌ Spark not responding"

# =============================================================================
# QUICK START COMMANDS
# =============================================================================

demo: setup start data-start ## Complete demo setup (setup + start platform + start data)
	@echo ""
	@echo "🎉 LaykHaus Demo Environment Ready!"
	@echo ""
	@echo "📍 Access the UI at: http://localhost:3000"
	@echo "📝 Add connectors using the configurations shown above"
	@echo "🔍 Try the Schema Explorer and Query Builder"
	@echo ""
	@echo "💡 Sample Query to try:"
	@echo "   SELECT * FROM solar.solar_panels LIMIT 10;"

demo-stop: stop data-stop ## Stop everything (platform + data)
	@echo "✅ All services stopped"

demo-clean: clean data-clean ## Clean everything (platform + data)
	@echo "✅ Full cleanup complete"