from datetime import datetime

from pyramid.config import Configurator

from conductor_celery.ext.event_handlers import (
    enable_event_handlers,
    get_event_handlers_status,
    register_failure_handler,
    register_success_handler,
)
from conductor_celery.ext.manager import get_manager


def main(global_config, **settings):
    config = Configurator(settings=settings)

    # Include conductor_celery Pyramid integration (exposes request.conductor_celery)
    config.include("conductor_celery.ext.pyramid")

    # Optionally create Kafka extension from .ini settings
    # noIA: Aqui foi criado um mecanismo de usar ou não o Kafka Extension.
    if str(settings.get("kafka.enabled", "false")).lower() == "true":
        bootstrap = [s.strip() for s in settings.get("kafka.bootstrap_servers", "").split(",") if s.strip()]
        topics = [t.strip() for t in settings.get("kafka.topics", "").split(",") if t.strip()]
        get_manager().create_extension(
            "kafka",
            {
                "bootstrap_servers": bootstrap,
                "topics": topics,
            },
        )

    # Configure event handlers for ConductorTask events
    # noIA: Aqui setamos as funções de handlers para cada task.
    setup_event_handlers(settings)

    # Routes and views
    config.add_route("health", "/health")
    config.add_view(health_view, route_name="health", renderer="json")

    config.add_route("settings", "/settings")
    config.add_view(settings_view, route_name="settings", renderer="json")

    config.add_route("send", "/send")
    config.add_view(send_view, route_name="send", renderer="json")

    config.add_route("stats", "/stats")
    config.add_view(stats_view, route_name="stats", renderer="json")

    config.add_route("handlers", "/handlers")
    config.add_view(handlers_view, route_name="handlers", renderer="json")

    return config.make_wsgi_app()


def health_view(request):
    return {"ok": True}


# noIA: Configuração do Conductor
def settings_view(request):
    ext = request.conductor_celery
    return {
        "conductor_url": ext.get_setting("conductor.server_url", None),
        "celery_broker": ext.get_setting("celery.broker_url", None),
        "celery_backend": ext.get_setting("celery.result_backend", None),
    }


# noIA: Implementacao do envio de mensagens para o Kafka
def send_view(request):
    manager = get_manager()
    kafka_ext = manager.get_extension("kafka")
    if not kafka_ext:
        return {"sent": False, "reason": "kafka not configured"}

    with kafka_ext:
        kafka_ext.send_message("dev-events", b"hello from pyramid")
    return {"sent": True}


# noIA: Setup dos eventos handlers
def setup_event_handlers(settings):
    """Set up event handlers for ConductorTask events."""
    # Import our custom handlers
    from .handlers import pyramid_failure_handler, pyramid_success_handler

    # Register the handlers
    register_success_handler(pyramid_success_handler)
    register_failure_handler(pyramid_failure_handler)

    # Enable the event handlers extension
    enable_event_handlers()

    # Log the setup
    import logging

    logger = logging.getLogger(__name__)
    logger.info("Event handlers configured for Pyramid application")


# noIA: Stats view para monitoramento dos eventos handlers
def stats_view(request):
    """View to display application statistics."""
    from .handlers import get_application_stats

    stats = get_application_stats()
    handlers_status = get_event_handlers_status()

    return {"application_stats": stats, "handlers_status": handlers_status, "timestamp": datetime.now().isoformat()}


# noIA: View para monitoramento dos eventos handlers
def handlers_view(request):
    """View to display information about configured handlers."""
    handlers_status = get_event_handlers_status()

    return {
        "handlers_enabled": handlers_status["enabled"],
        "success_handlers_count": handlers_status["success_handlers_count"],
        "failure_handlers_count": handlers_status["failure_handlers_count"],
        "description": "Custom event handlers for ConductorTask events",
        "features": [
            "Task event logging to file",
            "Metrics collection",
            "Conditional notifications",
            "Task-specific handling",
        ],
    }
