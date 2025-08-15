#!/usr/bin/env python3
"""
Test script to simulate ConductorTask events and demonstrate the event handlers.

This script simulates both success and failure events to show how the
custom handlers work in the Pyramid application.
"""

import os
import random
import sys
import time

from conductor_celery.ext.event_handlers import (
    TaskFailureEvent,
    TaskSuccessEvent,
    enable_event_handlers,
    get_event_handlers_status,
    register_failure_handler,
    register_success_handler,
)

# Add the parent directory to the path so we can import conductor_celery
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def test_success_handler(event: TaskSuccessEvent):
    """Test handler for success events."""
    print(f"🎉 TEST: Task {event.task_name} completed successfully!")
    print(f"   Task ID: {event.task_id}")
    print(f"   Workflow: {event.workflow_instance_id}")
    print(f"   Result: {event.result}")
    print()


def test_failure_handler(event: TaskFailureEvent):
    """Test handler for failure events."""
    print(f"💥 TEST: Task {event.task_name} failed!")
    print(f"   Task ID: {event.task_id}")
    print(f"   Workflow: {event.workflow_instance_id}")
    print(f"   Error: {event.error}")
    print()


def simulate_task_events():
    """Simulate various task events to test the handlers."""

    # Register test handlers
    register_success_handler(test_success_handler)
    register_failure_handler(test_failure_handler)
    enable_event_handlers()

    print("🚀 Starting ConductorTask event simulation...")
    print("=" * 50)

    # Sample task names for different scenarios
    task_names = [
        "email_send_welcome",
        "email_send_notification",
        "payment_process_credit_card",
        "payment_process_paypal",
        "order_fulfillment_shipping",
        "order_fulfillment_delivery",
        "data_processing_analytics",
        "data_processing_backup",
        "user_registration_verification",
        "user_registration_activation",
    ]

    # Sample error messages
    error_messages = [
        "Connection timeout",
        "Invalid input data",
        "Database connection failed",
        "External API error",
        "Insufficient permissions",
        "Resource not found",
        "Validation failed",
        "Rate limit exceeded",
    ]

    # Simulate success events
    print("📈 Simulating SUCCESS events...")
    for i in range(5):
        task_name = random.choice(task_names)
        event = TaskSuccessEvent(
            task_name=task_name,
            task_id=f"task_success_{i}_{int(time.time())}",
            workflow_instance_id=f"workflow_{i}_{int(time.time())}",
            worker_id=f"worker_{random.randint(1, 5)}",
            result={"status": "completed", "data": f"result_data_{i}"},
            args=(f"arg_{i}",),
            kwargs={"param": f"value_{i}"},
        )

        # Trigger the success handler
        from conductor_celery.ext.manager import get_manager

        manager = get_manager()
        event_handlers = manager.get_extension("event_handlers")
        if event_handlers:
            event_handlers.handle_success(event)

        time.sleep(0.5)  # Small delay between events

    print()

    # Simulate failure events
    print("📉 Simulating FAILURE events...")
    for i in range(3):
        task_name = random.choice(task_names)
        event = TaskFailureEvent(
            task_name=task_name,
            task_id=f"task_failure_{i}_{int(time.time())}",
            workflow_instance_id=f"workflow_{i}_{int(time.time())}",
            worker_id=f"worker_{random.randint(1, 5)}",
            error=random.choice(error_messages),
            args=(f"arg_{i}",),
            kwargs={"param": f"value_{i}"},
        )

        # Trigger the failure handler
        from conductor_celery.ext.manager import get_manager

        manager = get_manager()
        event_handlers = manager.get_extension("event_handlers")
        if event_handlers:
            event_handlers.handle_failure(event)

        time.sleep(0.5)  # Small delay between events

    print()
    print("✅ Event simulation completed!")
    print("=" * 50)

    # Show final status
    status = get_event_handlers_status()
    print("📊 Final Status:")
    print(f"   Handlers enabled: {status['enabled']}")
    print(f"   Success handlers: {status['success_handlers_count']}")
    print(f"   Failure handlers: {status['failure_handlers_count']}")


def show_usage():
    """Show usage information."""
    print("ConductorTask Event Handlers Test Script")
    print("=" * 40)
    print()
    print("This script demonstrates how the Event Handlers extension works:")
    print()
    print("1. Registers test handlers for success and failure events")
    print("2. Simulates various ConductorTask events")
    print("3. Shows how handlers process different types of tasks")
    print("4. Demonstrates conditional handling based on task names")
    print()
    print("Features demonstrated:")
    print("  ✅ Success event handling")
    print("  ❌ Failure event handling")
    print("  📧 Email task specific handling")
    print("  💳 Payment task specific handling")
    print("  📦 Order task specific handling")
    print("  📊 Metrics collection")
    print("  📝 Event logging")
    print("  🔔 Notifications")
    print()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] in ["-h", "--help", "help"]:
        show_usage()
    else:
        simulate_task_events()
