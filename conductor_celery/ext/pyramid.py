"""
Pyramid extension for conductor_celery.

This extension provides Pyramid integration for the conductor_celery library.
It will only be available if pyramid is installed.
"""

from typing import Any, Callable

from .base import BaseExtension


class PyramidExtension(BaseExtension):
    """Pyramid integration extension for conductor_celery."""

    def __init__(self, config: dict[str, Any] | None = None):
        """Initialize Pyramid extension."""
        super().__init__(config)
        self._app = None
        self._registry = None
        self._request = None

    def is_available(self) -> bool:
        """Check if Pyramid is available."""
        try:
            return True
        except ImportError:
            return False

    def initialize(self) -> None:
        """Initialize Pyramid integration."""
        if not self.is_available():
            raise RuntimeError("Pyramid is not available. Install pyramid to use this extension.")

        # This will be called when the extension is actually used
        # The actual initialization depends on the Pyramid app context
        pass

    def cleanup(self) -> None:
        """Clean up Pyramid resources."""
        self._app = None
        self._registry = None
        self._request = None

    def set_app(self, app) -> None:
        """Set the Pyramid application instance."""
        self._app = app
        if app:
            self._registry = app.registry

    def set_request(self, request) -> None:
        """Set the current Pyramid request."""
        self._request = request

    def get_setting(self, name: str, default: Any = None) -> Any:
        """Get a Pyramid setting."""
        if self._registry:
            return self._registry.settings.get(name, default)
        return default

    def add_route(self, name: str, pattern: str, view: Callable | None = None) -> None:
        """Add a route to the Pyramid application."""
        if not self._app:
            raise RuntimeError("Pyramid app not set")

        from pyramid.config import Configurator

        if isinstance(self._app, Configurator):
            self._app.add_route(name, pattern)
            if view:
                self._app.add_view(view, route_name=name)

    def get_current_request(self):
        """Get the current Pyramid request."""
        if self._request:
            return self._request

        # Try to get from thread local storage
        from pyramid.threadlocal import get_current_request

        return get_current_request()

    def get_current_registry(self):
        """Get the current Pyramid registry."""
        if self._registry:
            return self._registry

        # Try to get from thread local storage
        from pyramid.threadlocal import get_current_registry

        return get_current_registry()

    def include_package(self, package_name: str) -> None:
        """Include a Pyramid package."""
        if not self._app:
            raise RuntimeError("Pyramid app not set")

        if hasattr(self._app, "include"):
            self._app.include(package_name)

    @property
    def app(self):
        """Get the Pyramid application instance."""
        return self._app

    @property
    def registry(self):
        """Get the Pyramid registry."""
        return self._registry


def includeme(config) -> None:
    """Pyramid hook to include conductor_celery integration.

    Usage in your Pyramid app:
        config.include('conductor_celery.ext.pyramid')
    """
    try:
        from .manager import get_manager
    except Exception:
        return

    settings = {}
    try:
        # Pyramid Configurator provides get_settings()
        settings = getattr(config, "get_settings", lambda: {})() or {}
    except Exception:
        settings = {}

    manager = get_manager()

    # Create or fetch the Pyramid extension, attach the app/configurator
    extension_config = {
        "app_name": settings.get("conductor_celery.app_name", "conductor_celery"),
        "settings": settings,
    }
    pyramid_ext = manager.create_extension("pyramid", extension_config)
    if pyramid_ext is None:
        return

    pyramid_ext.set_app(config)
    # Optionally expose the extension on requests as `request.conductor_celery`
    config.add_request_method(lambda r: pyramid_ext, "conductor_celery", reify=True)
