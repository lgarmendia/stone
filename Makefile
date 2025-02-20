# Makefile for stone_use_case

# Variables
DOCKER_COMPOSE_FILE := docker-compose.yaml
IMAGE_NAME := stone_use_case_image
CONTAINER_NAME := stone_use_case_container
DOCKER_NETWORK := stone_network
PYTHON_FILES := src

# Default target
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  build-image       - Build the Docker image"
	@echo "  up                - Start the sermvices using Docker Compose"
	@echo "  down              - Stop and remove all services"
	@echo "  clean             - Clean up Docker containers, images, and networks"
	@echo "  logs              - View logs from the containers"
	@echo "  shell-webserver   - Open a bash shell in the Airflow webserver container"
	@echo "  shell-spark       - Open a bash shell in the Spark master container"
	@echo "  restart           - Restart the Docker services"

# Build the Docker image
.PHONY: build-image
build-image:
	@echo "Building Docker image..."
	docker-compose build

# Start the services using Docker Compose
.PHONY: up
image-up:
	@echo "Starting Docker services..."
	docker-compose -f $(DOCKER_COMPOSE_FILE) up -d

# Stop and remove all services
.PHONY: down
image-down:
	@echo "Stopping and removing Docker services..."
	docker-compose -f $(DOCKER_COMPOSE_FILE) down

# Clean up Docker containers, images, and networks
.PHONY: clean
image-clean:
	@echo "Cleaning up Docker resources..."
	docker-compose -f $(DOCKER_COMPOSE_FILE) down --volumes --remove-orphans
	docker rmi $(IMAGE_NAME) || true
	docker network rm $(DOCKER_NETWORK) || true

# View logs from the containers
.PHONY: logs
image-logs:
	@echo "Fetching logs from Docker containers..."
	docker-compose logs -f

# Open a bash shell in the Airflow webserver container
.PHONY: shell-webserver
image-shell-webserver:
	@echo "Opening shell in Airflow webserver container..."
	docker exec -it $(shell docker-compose ps -q webserver) bash

# Open a bash shell in the Spark master container
.PHONY: shell-spark
image-shell-spark:
	@echo "Opening shell in Spark master container..."
	docker exec -it $(shell docker-compose ps -q spark-master) bash

# Restart Docker services
.PHONY: restart
image-restart: down up
	@echo "Docker services restarted."


# Target: lint
# Description: Run code linters for the specified package
# Run linting using ruff
lint:
	ruff check $(PYTHON_FILES) --fix

# Target: style
# Description: Run code linters for the specified package
# Run linting using ruff
style:
	ruff format $(PYTHON_FILES)

