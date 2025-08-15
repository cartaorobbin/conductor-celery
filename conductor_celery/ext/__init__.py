"""
Extensions for conductor_celery library.

This module provides optional integrations with external systems like Kafka and Pyramid.
Extensions are loaded dynamically based on available dependencies.
"""
# Re-export manager utilities so callers can import from `conductor_celery.ext`
from .manager import (
    ExtensionManager,
    cleanup_extension,
    create_extension,
    get_extension_instance,
    get_manager,
    initialize_extension,
)

# Registry for available extensions
_EXTENSIONS: dict[str, type] = {}


def register_extension(name: str, extension_class: type) -> None:
    """Register an extension class."""
    _EXTENSIONS[name] = extension_class


def get_extension(name: str) -> type | None:
    """Get an extension by name."""
    return _EXTENSIONS.get(name)


def list_extensions() -> list[str]:
    """List all registered extensions."""
    return list(_EXTENSIONS.keys())


def load_extensions() -> None:
    """Load all available extensions."""
    # Try to load Kafka extension
    try:
        from .kafka import KafkaExtension

        register_extension("kafka", KafkaExtension)
    except ImportError:
        pass  # Kafka not available

    # Try to load Pyramid extension
    try:
        from .pyramid import PyramidExtension

        register_extension("pyramid", PyramidExtension)
    except ImportError:
        pass  # Pyramid not available

    # Load Event Handlers extension (always available)
    try:
        from .event_handlers import EventHandlersExtension

        register_extension("event_handlers", EventHandlersExtension)
    except ImportError:
        pass  # Event handlers not available


# Auto-load extensions when module is imported
load_extensions()

__all__ = [
    # Registry helpers
    "register_extension",
    "get_extension",
    "list_extensions",
    "load_extensions",
    # Manager utilities
    "ExtensionManager",
    "get_manager",
    "create_extension",
    "get_extension_instance",
    "initialize_extension",
    "cleanup_extension",
]
