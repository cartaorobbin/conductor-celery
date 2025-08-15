"""
Conductor Celery - A library for integrating Conductor with Celery.

This library provides a flexible way to use Conductor with Celery, with optional
extensions for Kafka and Pyramid integration.
"""

__version__ = "0.0.3"
__author__ = "Tomas Correa"
__email__ = "ftomas.correa@gmail.com"

# Import core functionality
from . import tasks, utils, wrapper

# Import extension system
try:
    EXTENSIONS_AVAILABLE = True
except ImportError:
    EXTENSIONS_AVAILABLE = False

__all__ = [
    "tasks",
    "wrapper",
    "utils",
    "EXTENSIONS_AVAILABLE",
]

# Add extension functions to __all__ if available
if EXTENSIONS_AVAILABLE:
    __all__.extend(
        [
            "create_extension",
            "get_extension_instance",
            "initialize_extension",
            "cleanup_extension",
            "get_manager",
            "list_extensions",
        ]
    )
