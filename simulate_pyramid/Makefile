# Makefile for Event Handlers Example

.PHONY: help install run test clean logs stats

help: ## Show this help message
	@echo "Event Handlers Example - Available commands:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies
	pip install -e ..

run: ## Start the Pyramid application
	python -m pyramid.scripts.pserve development.ini

test: ## Run the event handlers test script
	python test_handlers.py

test-help: ## Show test script help
	python test_handlers.py --help

clean: ## Clean up generated files
	rm -f task_events.log
	rm -f *.pyc
	rm -rf __pycache__

logs: ## Show the task events log
	@if [ -f task_events.log ]; then \
		echo "=== Task Events Log ==="; \
		tail -20 task_events.log; \
	else \
		echo "No task_events.log file found. Run 'make test' first."; \
	fi

stats: ## Show application statistics (requires running app)
	@echo "=== Application Statistics ==="
	@curl -s http://localhost:6543/stats | python -m json.tool 2>/dev/null || \
		echo "Application not running. Start with 'make run' first."

handlers: ## Show handlers information (requires running app)
	@echo "=== Handlers Information ==="
	@curl -s http://localhost:6543/handlers | python -m json.tool 2>/dev/null || \
		echo "Application not running. Start with 'make run' first."

health: ## Check application health (requires running app)
	@echo "=== Application Health ==="
	@curl -s http://localhost:6543/health | python -m json.tool 2>/dev/null || \
		echo "Application not running. Start with 'make run' first."

demo: ## Run a complete demonstration
	@echo "🚀 Starting Event Handlers Demo..."
	@echo ""
	@echo "1. Starting Pyramid application in background..."
	@python -m pyramid.scripts.pserve development.ini
	@sleep 3
	@echo "2. Running event handlers test..."
	@python test_handlers.py
	@echo ""
	@echo "3. Showing application statistics..."
	@make stats
	@echo ""
	@echo "4. Showing recent logs..."
	@make logs
	@echo ""
	@echo "5. Stopping application..."
	@pkill -f "pyramid.scripts.pserve" || true
	@echo ""
	@echo "✅ Demo completed!"

dev: ## Start development server with auto-reload
	python -m pyramid.scripts.pserve development.ini --reload
