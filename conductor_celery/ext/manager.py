"""
Extension manager for conductor_celery.

This module provides a convenient way to manage and use extensions.
"""

from typing import Any

from . import get_extension, list_extensions, register_extension
from .base import BaseExtension


class ExtensionManager:
    """Manager for conductor_celery extensions."""

    def __init__(self):
        """Initialize the extension manager."""
        self._extensions: dict[str, BaseExtension] = {}
        self._configs: dict[str, dict[str, Any]] = {}

    def register_extension(self, name: str, extension_class: type) -> None:
        """Register an extension class."""
        register_extension(name, extension_class)

    def create_extension(self, name: str, config: dict[str, Any] | None = None) -> BaseExtension | None:
        """Create an extension instance."""
        extension_class = get_extension(name)
        if not extension_class:
            return None

        extension = extension_class(config or {})
        self._extensions[name] = extension
        self._configs[name] = config or {}
        return extension

    def get_extension(self, name: str) -> BaseExtension | None:
        """Get an extension instance."""
        return self._extensions.get(name)

    def initialize_extension(self, name: str) -> bool:
        """Initialize an extension."""
        extension = self.get_extension(name)
        if not extension:
            return False

        if not extension.initialize():
            return False

        return True

    def cleanup_extension(self, name: str) -> bool:
        """Clean up an extension."""
        extension = self.get_extension(name)
        if not extension:
            return False

        if not extension.cleanup():
            return False

        return True

    def initialize_all(self) -> dict[str, bool]:
        """Initialize all extensions."""
        results = {}
        for name in self._extensions:
            results[name] = self.initialize_extension(name)
        return results

    def cleanup_all(self) -> dict[str, bool]:
        """Clean up all extensions."""
        results = {}
        for name in self._extensions:
            results[name] = self.cleanup_extension(name)
        return results

    def list_available_extensions(self) -> list[str]:
        """List all available extensions."""
        return list_extensions()

    def list_loaded_extensions(self) -> list[str]:
        """List all loaded extensions."""
        return list(self._extensions.keys())

    def is_extension_available(self, name: str) -> bool:
        """Check if an extension is available."""
        extension_class = get_extension(name)
        if not extension_class:
            return False

        extension = extension_class()
        return extension.is_available()

    def get_extension_config(self, name: str) -> dict[str, Any]:
        """Get the configuration for an extension."""
        return self._configs.get(name, {})

    def set_extension_config(self, name: str, config: dict[str, Any]) -> None:
        """Set the configuration for an extension."""
        self._configs[name] = config
        extension = self.get_extension(name)
        if extension:
            extension.config = config

    def __enter__(self):
        """Context manager entry."""
        self.initialize_all()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.cleanup_all()


# Global extension manager instance
_manager = ExtensionManager()


def get_manager() -> ExtensionManager:
    """Get the global extension manager."""
    return _manager


def create_extension(name: str, config: dict[str, Any] | None = None) -> BaseExtension | None:
    """Create an extension using the global manager."""
    return _manager.create_extension(name, config)


def get_extension_instance(name: str) -> BaseExtension | None:
    """Get an extension instance using the global manager."""
    return _manager.get_extension(name)


def initialize_extension(name: str) -> bool:
    """Initialize an extension using the global manager."""
    return _manager.initialize_extension(name)


def cleanup_extension(name: str) -> bool:
    """Clean up an extension using the global manager."""
    return _manager.cleanup_extension(name)
