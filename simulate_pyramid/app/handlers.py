"""
Custom event handlers for the simulate_pyramid application.

This module demonstrates how to use the Event Handlers extension
to customize behavior for ConductorTask success and failure events.
"""

import json
import logging
from datetime import datetime
from typing import Any

from conductor_celery.ext.event_handlers import TaskFailureEvent, TaskSuccessEvent

logger = logging.getLogger(__name__)


class TaskEventLogger:
    """Handler class for logging task events to a file."""

    def __init__(self, log_file: str = "task_events.log"):
        self.log_file = log_file

    def log_success(self, event: TaskSuccessEvent) -> None:
        """Log successful task completion."""
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "event_type": "success",
            "task_name": event.task_name,
            "task_id": event.task_id,
            "workflow_instance_id": event.workflow_instance_id,
            "worker_id": event.worker_id,
            "result": str(event.result)[:100],  # Truncate long results
            "args": list(event.args),
            "kwargs": event.kwargs,
        }

        with open(self.log_file, "a") as f:
            f.write(json.dumps(log_entry) + "\n")

        logger.info(f"Task success logged to {self.log_file}: {event.task_name}")

    def log_failure(self, event: TaskFailureEvent) -> None:
        """Log task failures."""
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "event_type": "failure",
            "task_name": event.task_name,
            "task_id": event.task_id,
            "workflow_instance_id": event.workflow_instance_id,
            "worker_id": event.worker_id,
            "error": event.error,
            "args": list(event.args),
            "kwargs": event.kwargs,
        }

        with open(self.log_file, "a") as f:
            f.write(json.dumps(log_entry) + "\n")

        logger.error(f"Task failure logged to {self.log_file}: {event.task_name}")


class TaskMetricsCollector:
    """Handler class for collecting task metrics."""

    def __init__(self):
        self.success_count = 0
        self.failure_count = 0
        self.task_stats = {}

    def collect_success_metrics(self, event: TaskSuccessEvent) -> None:
        """Collect metrics for successful tasks."""
        self.success_count += 1

        if event.task_name not in self.task_stats:
            self.task_stats[event.task_name] = {"success": 0, "failure": 0}

        self.task_stats[event.task_name]["success"] += 1

        logger.info(f"Success metrics updated for {event.task_name}. Total successes: {self.success_count}")

    def collect_failure_metrics(self, event: TaskFailureEvent) -> None:
        """Collect metrics for failed tasks."""
        self.failure_count += 1

        if event.task_name not in self.task_stats:
            self.task_stats[event.task_name] = {"success": 0, "failure": 0}

        self.task_stats[event.task_name]["failure"] += 1

        logger.error(f"Failure metrics updated for {event.task_name}. Total failures: {self.failure_count}")

    def get_stats(self) -> dict[str, Any]:
        """Get current statistics."""
        return {"total_success": self.success_count, "total_failure": self.failure_count, "task_stats": self.task_stats}


class TaskNotifier:
    """Handler class for sending notifications about task events."""

    # noIA: Simulando uma notificação da aplicação no nosso caso simulate_pyramid pode ser benefits ou legal-entity tanto faz..
    def __init__(self, notification_level: str = "error"):
        self.notification_level = notification_level
        self.notification_count = 0

    def send_success_notification(self, event: TaskSuccessEvent) -> None:
        """Send notification for successful task completion."""
        # Only send notifications for important tasks or if level is 'all'
        if self.notification_level == "all" or event.task_name in [
            "payment_process",
            "order_fulfillment",
            "email_send",
        ]:
            notification = {
                "type": "success",
                "task_name": event.task_name,
                "task_id": event.task_id,
                "workflow_id": event.workflow_instance_id,
                "message": f"Task {event.task_name} completed successfully",
            }

            # In a real application, this would send to Slack, email, etc.
            logger.info(f"SUCCESS NOTIFICATION: {json.dumps(notification)}")
            self.notification_count += 1

    def send_failure_notification(self, event: TaskFailureEvent) -> None:
        """Send notification for task failures."""
        notification = {
            "type": "failure",
            "task_name": event.task_name,
            "task_id": event.task_id,
            "workflow_id": event.workflow_instance_id,
            "error": event.error,
            "message": f"Task {event.task_name} failed: {event.error}",
        }

        # In a real application, this would send to Slack, email, etc.
        logger.error(f"FAILURE NOTIFICATION: {json.dumps(notification)}")
        self.notification_count += 1


class ConditionalTaskHandler:
    """Handler class for conditional task processing based on task name."""

    # noIA: Mesmo principio de simulação, mas para Condicional das tasks em questão.
    def handle_success_conditionally(self, event: TaskSuccessEvent) -> None:
        """Handle success based on task name patterns."""
        if event.task_name.startswith("email_"):
            logger.info(f"📧 Email task {event.task_name} completed successfully")
            # Could trigger email-specific actions

        elif event.task_name.startswith("payment_"):
            logger.info(f"💳 Payment task {event.task_name} completed successfully")
            # Could trigger payment-specific actions

        elif event.task_name.startswith("order_"):
            logger.info(f"📦 Order task {event.task_name} completed successfully")
            # Could trigger order-specific actions

        else:
            logger.info(f"✅ Generic task {event.task_name} completed successfully")

    def handle_failure_conditionally(self, event: TaskFailureEvent) -> None:
        """Handle failure based on task name patterns."""
        if event.task_name.startswith("email_"):
            logger.error(f"📧 Email task {event.task_name} failed: {event.error}")
            # Could trigger email system alert

        elif event.task_name.startswith("payment_"):
            logger.error(f"💳 Payment task {event.task_name} failed: {event.error}")
            # Could trigger payment system alert

        elif event.task_name.startswith("order_"):
            logger.error(f"📦 Order task {event.task_name} failed: {event.error}")
            # Could trigger order system alert

        else:
            logger.error(f"❌ Generic task {event.task_name} failed: {event.error}")


# Global instances for use across the application
task_logger = TaskEventLogger()
metrics_collector = TaskMetricsCollector()
task_notifier = TaskNotifier(notification_level="error")
conditional_handler = ConditionalTaskHandler()


# Handler functions that can be registered with the extension
# noIA: Aqui é onde o Conductor chama as funções de handlers para cada task.
def pyramid_success_handler(event: TaskSuccessEvent) -> None:
    """Main success handler for Pyramid application."""
    logger.info(f"🎉 Pyramid app: Task {event.task_name} completed successfully!")

    # Call all our custom handlers
    task_logger.log_success(event)
    metrics_collector.collect_success_metrics(event)
    task_notifier.send_success_notification(event)
    conditional_handler.handle_success_conditionally(event)


def pyramid_failure_handler(event: TaskFailureEvent) -> None:
    """Main failure handler for Pyramid application."""
    logger.error(f"💥 Pyramid app: Task {event.task_name} failed!")

    # Call all our custom handlers
    task_logger.log_failure(event)
    metrics_collector.collect_failure_metrics(event)
    task_notifier.send_failure_notification(event)
    conditional_handler.handle_failure_conditionally(event)


# noIA: Pegando as estatisticas das tasks.
def get_application_stats() -> dict[str, Any]:
    """Get current application statistics."""
    return {
        "metrics": metrics_collector.get_stats(),
        "notifications_sent": task_notifier.notification_count,
        "log_file": task_logger.log_file,
    }
