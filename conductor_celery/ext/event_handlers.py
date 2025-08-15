"""
Event Handlers extension for conductor_celery.

This extension provides a way to register custom handlers for ConductorTask
success and failure events without modifying the core library code.
"""

from dataclasses import dataclass
from typing import Any, Callable

from .base import BaseExtension


@dataclass
class TaskSuccessEvent:
    """Event data for task success."""

    task_name: str
    task_id: str
    workflow_instance_id: str
    worker_id: str
    result: Any
    args: tuple
    kwargs: dict


@dataclass
class TaskFailureEvent:
    """Event data for task failure."""

    task_name: str
    task_id: str
    workflow_instance_id: str
    worker_id: str
    error: str
    args: tuple
    kwargs: dict


class EventHandlersExtension(BaseExtension):
    """Event handlers extension for conductor_celery."""

    def __init__(self, config: dict[str, Any] | None = None):
        """Initialize Event Handlers extension."""
        super().__init__(config)
        self._success_handlers: list[Callable[[TaskSuccessEvent], None]] = []
        self._failure_handlers: list[Callable[[TaskFailureEvent], None]] = []
        self._enabled = self.get_config("enabled", True)

    def is_available(self) -> bool:
        """Check if Event Handlers extension is available."""
        return True  # Always available

    def initialize(self) -> None:
        """Initialize the extension."""
        # Load handlers from configuration if specified
        success_handlers_config = self.get_config("success_handlers", [])
        failure_handlers_config = self.get_config("failure_handlers", [])

        # Register handlers from config (if they exist)
        for handler_name in success_handlers_config:
            handler = self._load_handler(handler_name)
            if handler:
                self.register_success_handler(handler)

        for handler_name in failure_handlers_config:
            handler = self._load_handler(handler_name)
            if handler:
                self.register_failure_handler(handler)

    def cleanup(self) -> None:
        """Clean up the extension."""
        self._success_handlers.clear()
        self._failure_handlers.clear()

    def _load_handler(self, handler_name: str) -> Callable | None:
        """Load a handler function by name."""
        # Try to import the handler function
        module_name, function_name = handler_name.rsplit(".", 1)
        module = __import__(module_name, fromlist=[function_name])
        return getattr(module, function_name)

    def register_success_handler(self, handler: Callable[[TaskSuccessEvent], None]) -> None:
        """Register a handler for task success events."""
        if handler not in self._success_handlers:
            self._success_handlers.append(handler)

    def register_failure_handler(self, handler: Callable[[TaskFailureEvent], None]) -> None:
        """Register a handler for task failure events."""
        if handler not in self._failure_handlers:
            self._failure_handlers.append(handler)

    def unregister_success_handler(self, handler: Callable[[TaskSuccessEvent], None]) -> None:
        """Unregister a success handler."""
        if handler in self._success_handlers:
            self._success_handlers.remove(handler)

    def unregister_failure_handler(self, handler: Callable[[TaskFailureEvent], None]) -> None:
        """Unregister a failure handler."""
        if handler in self._failure_handlers:
            self._failure_handlers.remove(handler)

    def handle_success(self, event: TaskSuccessEvent) -> None:
        """Handle a task success event."""
        if not self._enabled:
            return

        for handler in self._success_handlers:
            try:
                handler(event)
            except Exception as e:
                # Log error but don't fail the task
                import logging

                logger = logging.getLogger(__name__)
                logger.error(f"Error in success handler {handler.__name__}: {e}")

    def handle_failure(self, event: TaskFailureEvent) -> None:
        """Handle a task failure event."""
        if not self._enabled:
            return

        for handler in self._failure_handlers:
            try:
                handler(event)
            except Exception as e:
                # Log error but don't fail the task
                import logging

                logger = logging.getLogger(__name__)
                logger.error(f"Error in failure handler {handler.__name__}: {e}")

    def enable(self) -> None:
        """Enable the extension."""
        self._enabled = True

    def disable(self) -> None:
        """Disable the extension."""
        self._enabled = False

    @property
    def is_enabled(self) -> bool:
        """Check if the extension is enabled."""
        return self._enabled

    @property
    def success_handlers_count(self) -> int:
        """Get the number of registered success handlers."""
        return len(self._success_handlers)

    @property
    def failure_handlers_count(self) -> int:
        """Get the number of registered failure handlers."""
        return len(self._failure_handlers)


# Convenience functions for easy usage
def register_success_handler(handler: Callable[[TaskSuccessEvent], None]) -> None:
    """Register a success handler using the global extension manager."""
    from .manager import get_manager

    manager = get_manager()
    event_handlers = manager.get_extension("event_handlers")
    if event_handlers:
        event_handlers.register_success_handler(handler)
    else:
        # Create the extension if it doesn't exist
        event_handlers = manager.create_extension("event_handlers")
        if event_handlers:
            event_handlers.register_success_handler(handler)


def register_failure_handler(handler: Callable[[TaskFailureEvent], None]) -> None:
    """Register a failure handler using the global extension manager."""
    from .manager import get_manager

    manager = get_manager()
    event_handlers = manager.get_extension("event_handlers")
    if event_handlers:
        event_handlers.register_failure_handler(handler)
    else:
        # Create the extension if it doesn't exist
        event_handlers = manager.create_extension("event_handlers")
        if event_handlers:
            event_handlers.register_failure_handler(handler)


def unregister_success_handler(handler: Callable[[TaskSuccessEvent], None]) -> None:
    """Unregister a success handler using the global extension manager."""
    from .manager import get_manager

    manager = get_manager()
    event_handlers = manager.get_extension("event_handlers")
    if event_handlers:
        event_handlers.unregister_success_handler(handler)


def unregister_failure_handler(handler: Callable[[TaskFailureEvent], None]) -> None:
    """Unregister a failure handler using the global extension manager."""
    from .manager import get_manager

    manager = get_manager()
    event_handlers = manager.get_extension("event_handlers")
    if event_handlers:
        event_handlers.unregister_failure_handler(handler)


def enable_event_handlers() -> None:
    """Enable event handlers using the global extension manager."""
    from .manager import get_manager

    manager = get_manager()
    event_handlers = manager.get_extension("event_handlers")
    if event_handlers:
        event_handlers.enable()
    else:
        # Create the extension if it doesn't exist
        event_handlers = manager.create_extension("event_handlers")
        if event_handlers:
            event_handlers.enable()


def disable_event_handlers() -> None:
    """Disable event handlers using the global extension manager."""
    from .manager import get_manager

    manager = get_manager()
    event_handlers = manager.get_extension("event_handlers")
    if event_handlers:
        event_handlers.disable()


def get_event_handlers_status() -> dict[str, Any]:
    """Get the status of event handlers using the global extension manager."""
    from .manager import get_manager

    manager = get_manager()
    event_handlers = manager.get_extension("event_handlers")
    if event_handlers:
        return {
            "enabled": event_handlers.is_enabled,
            "success_handlers_count": event_handlers.success_handlers_count,
            "failure_handlers_count": event_handlers.failure_handlers_count,
        }
    return {"enabled": False, "success_handlers_count": 0, "failure_handlers_count": 0}
