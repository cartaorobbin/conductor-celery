"""
Tests for the extension system.
"""

from unittest.mock import MagicMock, patch

import pytest

from conductor_celery.ext import (
    BaseExtension,
    ExtensionManager,
    KafkaExtension,
    PyramidExtension,
    create_extension,
    get_manager,
    list_extensions,
)


class TestBaseExtension:
    """Test the base extension class."""

    def test_base_extension_initialization(self):
        """Test base extension initialization."""
        config = {"test": "value"}
        ext = BaseExtension(config)
        assert ext.config == config
        assert not ext._initialized

    def test_base_extension_config_methods(self):
        """Test configuration methods."""
        ext = BaseExtension()

        # Test get_config with default
        assert ext.get_config("missing", "default") == "default"

        # Test set_config and get_config
        ext.set_config("key", "value")
        assert ext.get_config("key") == "value"

    def test_base_extension_name_property(self):
        """Test the name property."""
        ext = BaseExtension()
        assert ext.name == "base"

        class TestExtension(BaseExtension):
            pass

        test_ext = TestExtension()
        assert test_ext.name == "test"


class TestKafkaExtension:
    """Test the Kafka extension."""

    def test_kafka_extension_initialization(self):
        """Test Kafka extension initialization."""
        ext = KafkaExtension()
        assert ext._producer is None
        assert ext._consumer is None
        assert ext._topics == []

    @patch("conductor_celery.ext.kafka.importlib.import_module")
    def test_kafka_extension_available(self, mock_import):
        """Test Kafka extension availability check."""
        ext = KafkaExtension()

        # Test when kafka is available
        mock_import.return_value = MagicMock()
        assert ext.is_available() is True

        # Test when kafka is not available
        mock_import.side_effect = ImportError()
        assert ext.is_available() is False

    def test_kafka_extension_not_available_initialization(self):
        """Test Kafka extension initialization when kafka is not available."""
        with patch("conductor_celery.ext.kafka.importlib.import_module", side_effect=ImportError()):
            ext = KafkaExtension()
            with pytest.raises(RuntimeError, match="Kafka is not available"):
                ext.initialize()


class TestPyramidExtension:
    """Test the Pyramid extension."""

    def test_pyramid_extension_initialization(self):
        """Test Pyramid extension initialization."""
        ext = PyramidExtension()
        assert ext._app is None
        assert ext._registry is None
        assert ext._request is None

    @patch("conductor_celery.ext.pyramid.importlib.import_module")
    def test_pyramid_extension_available(self, mock_import):
        """Test Pyramid extension availability check."""
        ext = PyramidExtension()

        # Test when pyramid is available
        mock_import.return_value = MagicMock()
        assert ext.is_available() is True

        # Test when pyramid is not available
        mock_import.side_effect = ImportError()
        assert ext.is_available() is False

    def test_pyramid_extension_set_app(self):
        """Test setting Pyramid app."""
        ext = PyramidExtension()
        mock_app = MagicMock()
        mock_app.registry = MagicMock()

        ext.set_app(mock_app)
        assert ext._app == mock_app
        assert ext._registry == mock_app.registry

    def test_pyramid_extension_get_setting(self):
        """Test getting Pyramid settings."""
        ext = PyramidExtension()
        mock_registry = MagicMock()
        mock_registry.settings = {"debug": True}
        ext._registry = mock_registry

        assert ext.get_setting("debug") is True
        assert ext.get_setting("missing", "default") == "default"


class TestExtensionManager:
    """Test the extension manager."""

    def test_extension_manager_initialization(self):
        """Test extension manager initialization."""
        manager = ExtensionManager()
        assert manager._extensions == {}
        assert manager._configs == {}

    def test_extension_manager_create_extension(self):
        """Test creating extensions."""
        manager = ExtensionManager()

        # Mock extension class
        class MockExtension(BaseExtension):
            pass

        with patch("conductor_celery.ext.manager.get_extension", return_value=MockExtension):
            ext = manager.create_extension("mock", {"test": "config"})
            assert isinstance(ext, MockExtension)
            assert ext.config == {"test": "config"}
            assert "mock" in manager._extensions
            assert manager._configs["mock"] == {"test": "config"}

    def test_extension_manager_get_extension(self):
        """Test getting extension instances."""
        manager = ExtensionManager()
        mock_ext = MagicMock()
        manager._extensions["test"] = mock_ext

        assert manager.get_extension("test") == mock_ext
        assert manager.get_extension("missing") is None

    def test_extension_manager_list_extensions(self):
        """Test listing extensions."""
        manager = ExtensionManager()
        manager._extensions["ext1"] = MagicMock()
        manager._extensions["ext2"] = MagicMock()

        loaded = manager.list_loaded_extensions()
        assert "ext1" in loaded
        assert "ext2" in loaded

    def test_extension_manager_context_manager(self):
        """Test extension manager as context manager."""
        manager = ExtensionManager()
        mock_ext = MagicMock()
        manager._extensions["test"] = mock_ext

        with manager:
            mock_ext.initialize.assert_called_once()

        mock_ext.cleanup.assert_called_once()


class TestExtensionSystem:
    """Test the overall extension system."""

    def test_get_manager_singleton(self):
        """Test that get_manager returns a singleton."""
        manager1 = get_manager()
        manager2 = get_manager()
        assert manager1 is manager2

    def test_list_extensions(self):
        """Test listing available extensions."""
        extensions = list_extensions()
        assert isinstance(extensions, list)

    def test_create_extension_global(self):
        """Test creating extension using global manager."""
        with patch("conductor_celery.ext.manager.get_extension", return_value=BaseExtension):
            ext = create_extension("test", {"config": "value"})
            assert isinstance(ext, BaseExtension)
            assert ext.config == {"config": "value"}


if __name__ == "__main__":
    pytest.main([__file__])
