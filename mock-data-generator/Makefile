.PHONY: help start stop restart logs clean status test

# Default target
.DEFAULT_GOAL := help

help: ## Show this help message
	@echo "LaykHaus Demo Data Generator"
	@echo "============================"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

start: ## Start all data generators (PostgreSQL, Kafka, REST API)
	@echo "🚀 Starting Demo Data Generators..."
	@echo "NOTE: Please run 'make data-start' from the parent directory instead"
	@cd .. && docker-compose -f docker-compose.data.yml up -d
	@echo "⏳ Waiting for services to initialize..."
	@sleep 10
	@echo "✅ Data generators started!"
	@echo ""
	@echo "Services available at:"
	@echo "  • PostgreSQL: localhost:5432 (DB: solar_energy_db)"
	@echo "  • Kafka: localhost:9092"
	@echo "  • REST API: http://localhost:8080"
	@echo "  • API Docs: http://localhost:8080/docs"
	@echo ""
	@echo "Configure these in LaykHaus UI to start querying!"

stop: ## Stop all data generators
	@echo "🛑 Stopping data generators..."
	@echo "NOTE: Please run 'make data-stop' from the parent directory instead"
	@cd .. && docker-compose -f docker-compose.data.yml down
	@echo "✅ Data generators stopped"

restart: ## Restart all services
	@make stop
	@make start

logs: ## Show logs from all services
	@docker-compose logs -f

logs-generator: ## Show only data generator logs
	@docker-compose logs -f data-generator

logs-kafka: ## Show only Kafka logs
	@docker-compose logs -f kafka

logs-api: ## Show only REST API logs
	@docker-compose logs -f rest-api-mock

status: ## Check status of all services
	@echo "📊 Service Status:"
	@echo "=================="
	@docker-compose ps
	@echo ""
	@echo "📈 Data Generation Stats:"
	@docker exec demo-postgres psql -U demo_user -d solar_energy_db -c "SELECT 'Solar Panels' as type, COUNT(*) as count FROM solar.solar_panels UNION SELECT 'Energy Records', COUNT(*) FROM solar.energy_production UNION SELECT 'Weather Records', COUNT(*) FROM solar.weather_data;" 2>/dev/null || echo "Database not ready yet"

clean: ## Clean up all containers and volumes
	@echo "🧹 Cleaning up..."
	@echo "NOTE: Please run 'make data-clean' from the parent directory instead"
	@cd .. && docker-compose -f docker-compose.data.yml down -v
	@echo "✅ Cleanup complete"

test-postgres: ## Test PostgreSQL connection
	@echo "🔍 Testing PostgreSQL connection..."
	@docker exec demo-postgres psql -U demo_user -d solar_energy_db -c "SELECT version();" || echo "❌ PostgreSQL not accessible"

test-kafka: ## Test Kafka connection
	@echo "🔍 Testing Kafka..."
	@docker exec demo-kafka kafka-topics --bootstrap-server localhost:9092 --list || echo "❌ Kafka not accessible"

test-api: ## Test REST API
	@echo "🔍 Testing REST API..."
	@curl -s http://localhost:8080/health | jq '.' || echo "❌ REST API not accessible"

kafka-topics: ## List all Kafka topics
	@echo "📋 Kafka Topics:"
	@docker exec demo-kafka kafka-topics --bootstrap-server localhost:9092 --list

kafka-consume: ## Consume messages from solar-panel-telemetry topic
	@echo "📡 Consuming from solar-panel-telemetry (Ctrl+C to stop)..."
	@docker exec -it demo-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic solar-panel-telemetry --from-beginning

psql: ## Connect to PostgreSQL CLI
	@docker exec -it demo-postgres psql -U demo_user -d solar_energy_db

api-docs: ## Open REST API documentation
	@echo "Opening API docs at http://localhost:8080/docs"
	@open http://localhost:8080/docs 2>/dev/null || xdg-open http://localhost:8080/docs 2>/dev/null || echo "Please open http://localhost:8080/docs in your browser"

quick-test: ## Quick test of all components
	@echo "🧪 Running quick test..."
	@make test-postgres
	@echo ""
	@make test-kafka
	@echo ""
	@make test-api
	@echo ""
	@echo "✅ Quick test complete!"