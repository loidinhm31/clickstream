# Clickstream Analytics - Development Makefile
# Orchestrates all services: Infrastructure (Docker) + Application Services (Maven/NPM)
#
# Prerequisites:
# - Docker & Docker Compose
# - Java 17+ & Maven 3.9+
# - Node.js 18+ & npm
#
# Usage:
#   make help          - Show available targets
#   make start-all     - Start all services (infrastructure + apps)
#   make stop-all      - Stop all services
#   make build-all     - Build all Maven modules and frontend
#   make clean-all     - Clean all build artifacts and Docker volumes

.PHONY: help start-all stop-all build-all clean-all status logs

# Default target
.DEFAULT_GOAL := help

# Color output
CYAN := \033[0;36m
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

# Service ports
INGESTION_API_PORT := 9051
REALTIME_ANALYTICS_PORT := 9052
RAW_ARCHIVER_PORT := 9053
FRONTEND_PORT := 9059
KAFKA_UI_PORT := 9050
MONGODB_PORT := 9055
KAFKA_PORT := 9056

# PID files for background processes
PID_DIR := .pids
INGESTION_PID := $(PID_DIR)/ingestion-api.pid
REALTIME_PID := $(PID_DIR)/realtime-analytics.pid
ARCHIVER_PID := $(PID_DIR)/raw-archiver.pid
FRONTEND_PID := $(PID_DIR)/frontend.pid

# Log files
LOG_DIR := logs
INGESTION_LOG := $(LOG_DIR)/ingestion-api.log
REALTIME_LOG := $(LOG_DIR)/realtime-analytics.log
ARCHIVER_LOG := $(LOG_DIR)/raw-archiver.log
FRONTEND_LOG := $(LOG_DIR)/frontend.log

##@ Help

help: ## Display this help
	@echo "$(CYAN)Clickstream Analytics - Development Commands$(NC)"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"; printf "Usage:\n  make $(CYAN)<target>$(NC)\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  $(CYAN)%-20s$(NC) %s\n", $$1, $$2 } /^##@/ { printf "\n$(YELLOW)%s$(NC)\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Build

build-all: build-maven build-frontend ## Build all services (Maven + Frontend)
	@echo "$(GREEN)✓ All services built successfully$(NC)"

build-maven: ## Build all Maven modules
	@echo "$(CYAN)Building Maven modules...$(NC)"
	mvn clean install -DskipTests --settings .mvn/settings.xml
	@echo "$(GREEN)✓ Maven build complete$(NC)"

build-maven-test: ## Build all Maven modules with tests
	@echo "$(CYAN)Building Maven modules with tests...$(NC)"
	mvn clean install --settings .mvn/settings.xml
	@echo "$(GREEN)✓ Maven build with tests complete$(NC)"

build-spark-etl: ## Build Spark ETL JAR for Docker
	@echo "$(CYAN)Building Spark ETL module (with dependencies)...$(NC)"
	@echo "$(YELLOW)Installing parent POM first...$(NC)"
	mvn install -N -DskipTests --settings .mvn/settings.xml
	mvn clean install -pl shared-models -DskipTests --settings .mvn/settings.xml
	mvn clean package -pl spark-etl -DskipTests --settings .mvn/settings.xml
	@echo "$(GREEN)✓ Spark ETL JAR built$(NC)"

build-frontend: ## Build frontend application
	@echo "$(CYAN)Building frontend...$(NC)"
	cd frontend && npm install && npm run build
	@echo "$(GREEN)✓ Frontend build complete$(NC)"

##@ Infrastructure

start-infra: ## Start infrastructure services (Kafka, MongoDB, Kafka-UI, Spark ETL) in Docker
	@echo "$(CYAN)Starting infrastructure services (Docker)...$(NC)"
	docker compose up -d kafka mongodb kafka-ui
	@echo "$(YELLOW)Waiting for services to be healthy...$(NC)"
	@sleep 15
	docker compose ps
	@echo "$(GREEN)✓ Infrastructure services started$(NC)"
	@echo "$(CYAN)Services available at:$(NC)"
	@echo "  - Kafka UI:  http://localhost:$(KAFKA_UI_PORT)"
	@echo "  - Kafka:     localhost:$(KAFKA_PORT)"
	@echo "  - MongoDB:   mongodb://localhost:$(MONGODB_PORT)/clickstream_db"

start-spark: build-spark-etl ## Build and start Spark ETL in Docker
	@echo "$(CYAN)Starting Spark ETL service (Docker)...$(NC)"
	docker compose up -d --build spark-etl
	@echo "$(GREEN)✓ Spark ETL started$(NC)"
	@echo "$(YELLOW)Check logs: docker compose logs -f spark-etl$(NC)"

stop-infra: ## Stop infrastructure services
	@echo "$(RED)Stopping infrastructure services...$(NC)"
	docker compose down
	@echo "$(GREEN)✓ Infrastructure stopped$(NC)"

stop-infra-clean: ## Stop infrastructure and remove volumes
	@echo "$(RED)Stopping infrastructure and removing volumes...$(NC)"
	docker compose down -v
	@echo "$(GREEN)✓ Infrastructure stopped and cleaned$(NC)"

##@ Application Services

start-ingestion-api: ## Start Ingestion API (Spring Boot)
	@echo "$(CYAN)Starting Ingestion API on port $(INGESTION_API_PORT)...$(NC)"
	@mkdir -p $(PID_DIR) $(LOG_DIR)
	@cd ingestion-api && (mvn spring-boot:run \
		-Dspring-boot.run.jvmArguments="-Dserver.port=$(INGESTION_API_PORT)" \
		> ../$(INGESTION_LOG) 2>&1 & echo $$! > ../$(INGESTION_PID))
	@echo "$(GREEN)✓ Ingestion API started (PID: $$(cat $(INGESTION_PID)))$(NC)"
	@echo "$(CYAN)  API: http://localhost:$(INGESTION_API_PORT)$(NC)"
	@echo "$(CYAN)  Logs: tail -f $(INGESTION_LOG)$(NC)"

start-realtime-analytics: ## Start Real-time Analytics (Spring Boot)
	@echo "$(CYAN)Starting Real-time Analytics on port $(REALTIME_ANALYTICS_PORT)...$(NC)"
	@mkdir -p $(PID_DIR) $(LOG_DIR)
	@cd realtime-analytics && (mvn spring-boot:run \
		-Dspring-boot.run.jvmArguments="-Dserver.port=$(REALTIME_ANALYTICS_PORT)" \
		> ../$(REALTIME_LOG) 2>&1 & echo $$! > ../$(REALTIME_PID))
	@echo "$(GREEN)✓ Real-time Analytics started (PID: $$(cat $(REALTIME_PID)))$(NC)"
	@echo "$(CYAN)  WebSocket: ws://localhost:$(REALTIME_ANALYTICS_PORT)/ws/metrics$(NC)"
	@echo "$(CYAN)  Logs: tail -f $(REALTIME_LOG)$(NC)"

start-raw-archiver: ## Start Raw Archiver (Spring Boot)
	@echo "$(CYAN)Starting Raw Archiver on port $(RAW_ARCHIVER_PORT)...$(NC)"
	@mkdir -p $(PID_DIR) $(LOG_DIR)
	@cd raw-archiver && (mvn spring-boot:run \
		-Dspring-boot.run.jvmArguments="-Dserver.port=$(RAW_ARCHIVER_PORT)" \
		> ../$(ARCHIVER_LOG) 2>&1 & echo $$! > ../$(ARCHIVER_PID))
	@echo "$(GREEN)✓ Raw Archiver started (PID: $$(cat $(ARCHIVER_PID)))$(NC)"
	@echo "$(CYAN)  Health: http://localhost:$(RAW_ARCHIVER_PORT)/actuator/health$(NC)"
	@echo "$(CYAN)  Logs: tail -f $(ARCHIVER_LOG)$(NC)"

start-frontend: ## Start Frontend (Vite dev server)
	@echo "$(CYAN)Starting Frontend on port $(FRONTEND_PORT)...$(NC)"
	@mkdir -p $(PID_DIR) $(LOG_DIR)
	@cd frontend && (npm run dev \
		> ../$(FRONTEND_LOG) 2>&1 & echo $$! > ../$(FRONTEND_PID))
	@echo "$(GREEN)✓ Frontend started (PID: $$(cat $(FRONTEND_PID)))$(NC)"
	@echo "$(CYAN)  UI: http://localhost:$(FRONTEND_PORT)$(NC)"
	@echo "$(CYAN)  Logs: tail -f $(FRONTEND_LOG)$(NC)"

stop-app-services: ## Stop all application services
	@echo "$(RED)Stopping application services...$(NC)"
	@if [ -f $(INGESTION_PID) ]; then \
		kill -15 $$(cat $(INGESTION_PID)) 2>/dev/null || true; \
		rm $(INGESTION_PID); \
		echo "  ✓ Ingestion API stopped"; \
	fi
	@if [ -f $(REALTIME_PID) ]; then \
		kill -15 $$(cat $(REALTIME_PID)) 2>/dev/null || true; \
		rm $(REALTIME_PID); \
		echo "  ✓ Real-time Analytics stopped"; \
	fi
	@if [ -f $(ARCHIVER_PID) ]; then \
		kill -15 $$(cat $(ARCHIVER_PID)) 2>/dev/null || true; \
		rm $(ARCHIVER_PID); \
		echo "  ✓ Raw Archiver stopped"; \
	fi
	@if [ -f $(FRONTEND_PID) ]; then \
		kill -15 $$(cat $(FRONTEND_PID)) 2>/dev/null || true; \
		rm $(FRONTEND_PID); \
		echo "  ✓ Frontend stopped"; \
	fi
	@echo "$(GREEN)✓ All application services stopped$(NC)"

##@ Orchestration

start-all: start-infra start-spark start-app-services-bg start-frontend ## Start all services (Infrastructure + Apps + Frontend)
	@echo ""
	@echo "$(GREEN)═══════════════════════════════════════════════════════════$(NC)"
	@echo "$(GREEN)✓ All services started successfully!$(NC)"
	@echo "$(GREEN)═══════════════════════════════════════════════════════════$(NC)"
	@echo ""
	@echo "$(CYAN)Infrastructure:$(NC)"
	@echo "  Kafka:           localhost:$(KAFKA_PORT)"
	@echo "  MongoDB:         localhost:$(MONGODB_PORT)"
	@echo "  Kafka UI:        http://localhost:$(KAFKA_UI_PORT)"
	@echo ""
	@echo "$(CYAN)Application Services:$(NC)"
	@echo "  Ingestion API:   http://localhost:$(INGESTION_API_PORT)"
	@echo "  Realtime Analytics: ws://localhost:$(REALTIME_ANALYTICS_PORT)/ws/metrics"
	@echo "  Raw Archiver:    http://localhost:$(RAW_ARCHIVER_PORT)/actuator/health"
	@echo "  Spark ETL:       docker logs spark-etl"
	@echo ""
	@echo "$(CYAN)Frontend:$(NC)"
	@echo "  UI:              http://localhost:$(FRONTEND_PORT)"
	@echo ""
	@echo "$(YELLOW)Commands:$(NC)"
	@echo "  make status      - Check service status"
	@echo "  make logs        - View all logs"
	@echo "  make stop-all    - Stop all services"
	@echo ""

free-app-ports: ## Kill any processes holding application service ports
	@for port in $(INGESTION_API_PORT) $(REALTIME_ANALYTICS_PORT) $(RAW_ARCHIVER_PORT) $(FRONTEND_PORT); do \
		pid=$$(ss -tlnp "sport = :$$port" 2>/dev/null | awk 'NR>1 {match($$0,/pid=([0-9]+)/,a); if(a[1]) print a[1]}'); \
		if [ -n "$$pid" ]; then \
			echo "  Killing stale process $$pid on port $$port"; \
			kill -15 $$pid 2>/dev/null || true; \
		fi; \
	done
	@sleep 1

start-app-services-bg: free-app-ports ## Start all Spring Boot application services in background
	@$(MAKE) start-ingestion-api
	@sleep 5
	@$(MAKE) start-realtime-analytics
	@sleep 5
	@$(MAKE) start-raw-archiver
	@sleep 3

stop-all: stop-app-services stop-infra ## Stop all services (Apps + Infrastructure)
	@echo "$(GREEN)✓ All services stopped$(NC)"

restart-all: stop-all start-all ## Restart all services

##@ Status & Logs

status: ## Check status of all services
	@echo "$(CYAN)Service Status$(NC)"
	@echo ""
	@echo "$(YELLOW)Infrastructure (Docker):$(NC)"
	@docker compose ps
	@echo ""
	@echo "$(YELLOW)Application Services:$(NC)"
	@if [ -f $(INGESTION_PID) ] && ps -p $$(cat $(INGESTION_PID)) > /dev/null 2>&1; then \
		echo "  $(GREEN)✓$(NC) Ingestion API    (PID: $$(cat $(INGESTION_PID)))"; \
	else \
		echo "  $(RED)✗$(NC) Ingestion API    (not running)"; \
	fi
	@if [ -f $(REALTIME_PID) ] && ps -p $$(cat $(REALTIME_PID)) > /dev/null 2>&1; then \
		echo "  $(GREEN)✓$(NC) Realtime Analytics (PID: $$(cat $(REALTIME_PID)))"; \
	else \
		echo "  $(RED)✗$(NC) Realtime Analytics (not running)"; \
	fi
	@if [ -f $(ARCHIVER_PID) ] && ps -p $$(cat $(ARCHIVER_PID)) > /dev/null 2>&1; then \
		echo "  $(GREEN)✓$(NC) Raw Archiver     (PID: $$(cat $(ARCHIVER_PID)))"; \
	else \
		echo "  $(RED)✗$(NC) Raw Archiver     (not running)"; \
	fi
	@if [ -f $(FRONTEND_PID) ] && ps -p $$(cat $(FRONTEND_PID)) > /dev/null 2>&1; then \
		echo "  $(GREEN)✓$(NC) Frontend         (PID: $$(cat $(FRONTEND_PID)))"; \
	else \
		echo "  $(RED)✗$(NC) Frontend         (not running)"; \
	fi

logs: ## Tail all application logs
	@echo "$(CYAN)Tailing application logs (Ctrl+C to stop)...$(NC)"
	tail -f $(LOG_DIR)/*.log 2>/dev/null || echo "No logs available yet"

logs-ingestion: ## Tail Ingestion API logs
	tail -f $(INGESTION_LOG)

logs-realtime: ## Tail Real-time Analytics logs
	tail -f $(REALTIME_LOG)

logs-archiver: ## Tail Raw Archiver logs
	tail -f $(ARCHIVER_LOG)

logs-frontend: ## Tail Frontend logs
	tail -f $(FRONTEND_LOG)

logs-spark: ## Tail Spark ETL logs (Docker)
	docker compose logs -f spark-etl

logs-kafka: ## Tail Kafka logs (Docker)
	docker compose logs -f kafka

##@ Maintenance

clean-all: stop-all clean-maven clean-frontend clean-logs ## Stop all services and clean build artifacts
	@echo "$(GREEN)✓ All cleaned$(NC)"

clean-maven: ## Clean Maven build artifacts
	@echo "$(CYAN)Cleaning Maven artifacts...$(NC)"
	mvn clean --settings .mvn/settings.xml
	@echo "$(GREEN)✓ Maven cleaned$(NC)"

clean-frontend: ## Clean Frontend build artifacts
	@echo "$(CYAN)Cleaning Frontend artifacts...$(NC)"
	rm -rf frontend/node_modules frontend/dist
	@echo "$(GREEN)✓ Frontend cleaned$(NC)"

clean-logs: ## Clean all log files
	@echo "$(CYAN)Cleaning log files...$(NC)"
	rm -rf $(LOG_DIR)
	rm -rf $(PID_DIR)
	@echo "$(GREEN)✓ Logs cleaned$(NC)"

reset: stop-infra-clean clean-all ## Full reset (stop all, clean all, remove volumes)
	@echo "$(GREEN)✓ Full reset complete$(NC)"
	@echo "$(YELLOW)Run 'make build-all && make start-all' to start fresh$(NC)"

##@ Testing

test-all: test-maven test-frontend ## Run all tests

test-maven: ## Run Maven tests
	@echo "$(CYAN)Running Maven tests...$(NC)"
	mvn test --settings .mvn/settings.xml
	@echo "$(GREEN)✓ Maven tests complete$(NC)"

test-frontend: ## Run Frontend tests
	@echo "$(CYAN)Running Frontend tests...$(NC)"
	cd frontend && npm test
	@echo "$(GREEN)✓ Frontend tests complete$(NC)"

test-e2e: ## Run E2E Playwright tests (requires all services running via make start-all)
	@echo "$(CYAN)Running E2E tests (Playwright)...$(NC)"
	bash scripts/run-e2e.sh
	@echo "$(GREEN)✓ E2E tests complete$(NC)"

test-e2e-headed: ## Run E2E Playwright tests in headed (visible browser) mode
	@echo "$(CYAN)Running E2E tests (headed)...$(NC)"
	bash scripts/run-e2e.sh --headed
	@echo "$(GREEN)✓ E2E tests complete$(NC)"

##@ Development Helpers

dev: build-all start-all ## Full development setup (build + start all)

verify-setup: ## Verify development environment setup
	@echo "$(CYAN)Verifying development environment...$(NC)"
	@echo "Checking Docker..."
	@docker --version || echo "$(RED)✗ Docker not found$(NC)"
	@echo "Checking Java..."
	@java -version 2>&1 | head -n 1 || echo "$(RED)✗ Java not found$(NC)"
	@echo "Checking Maven..."
	@mvn --version | head -n 1 || echo "$(RED)✗ Maven not found$(NC)"
	@echo "Checking Node.js..."
	@node --version || echo "$(RED)✗ Node.js not found$(NC)"
	@echo "Checking npm..."
	@npm --version || echo "$(RED)✗ npm not found$(NC)"
	@echo "$(GREEN)✓ Environment verification complete$(NC)"

quick-start: start-infra start-spark ## Quick start (infrastructure + Spark only, no app services)
	@echo "$(GREEN)✓ Quick start complete$(NC)"
	@echo "$(YELLOW)Infrastructure is ready. Use 'make start-app-services-bg' to start application services.$(NC)"
