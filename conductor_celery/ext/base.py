"""
Base extension class for conductor_celery extensions.

All extensions should inherit from this class and implement the required methods.
"""

from abc import ABC, abstractmethod
from typing import Any


class BaseExtension(ABC):
    """Base class for all conductor_celery extensions."""

    def __init__(self, config: dict[str, Any] | None = None):
        """Initialize the extension with optional configuration."""
        self.config = config or {}
        self._initialized = False

    @abstractmethod
    def is_available(self) -> bool:
        """
        Check if the extension can be used.

        Returns:
            bool: True if the extension is available and can be used
        """
        pass

    @abstractmethod
    def initialize(self) -> None:
        """
        Initialize the extension.

        This method should set up any necessary connections, configurations,
        or resources required by the extension.
        """
        pass

    @abstractmethod
    def cleanup(self) -> None:
        """
        Clean up resources used by the extension.

        This method should close connections, release resources, etc.
        """
        pass

    def get_config(self, key: str, default: Any = None) -> Any:
        """Get a configuration value."""
        return self.config.get(key, default)

    def set_config(self, key: str, value: Any) -> None:
        """Set a configuration value."""
        self.config[key] = value

    @property
    def name(self) -> str:
        """Get the extension name."""
        return self.__class__.__name__.lower().replace("extension", "")

    def __enter__(self):
        """Context manager entry."""
        if not self._initialized:
            self.initialize()
            self._initialized = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.cleanup()
        self._initialized = False
