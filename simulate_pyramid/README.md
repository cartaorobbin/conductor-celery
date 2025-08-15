# simulate_pyramid

Minimal Pyramid app demonstrating integration with `conductor_celery` via `config.include('conductor_celery.ext.pyramid')`.

## Event Handlers Extension Example

This application also demonstrates the **Event Handlers Extension** for controlling ConductorTask success and failure events without modifying the library code.

### Quick Start

```bash
# Run the simple example
python simple_example.py

# Run the complete example
make demo

# Or start the application and test manually
make run
# In another terminal:
python test_handlers.py
```

### Features Demonstrated

- ✅ **Custom Event Handlers**: Logging, metrics, notifications
- ✅ **Conditional Processing**: Different actions based on task names
- ✅ **Multiple Handlers**: Coordinated processing of events
- ✅ **REST Endpoints**: `/stats`, `/handlers` for monitoring
- ✅ **Integration**: Seamless integration with Pyramid application

See [EVENT_HANDLERS_EXAMPLE.md](EVENT_HANDLERS_EXAMPLE.md) for detailed documentation.

## Requirements

Você pode rodar sem Waitress usando o servidor embutido `wsgiref` para desenvolvimento:

```bash
pip install pyramid plaster_pastedeploy
```

## Run (development)

```bash
cd /workspace
pserve simulate_pyramid/development.ini --reload
```

- Health check: GET http://localhost:6543/health
- Settings view: GET http://localhost:6543/settings
- Kafka send (optional): GET http://localhost:6543/send
- Event handlers stats: GET http://localhost:6543/stats
- Event handlers info: GET http://localhost:6543/handlers

To enable Kafka in dev, set in `simulate_pyramid/development.ini`:

```
kafka.enabled = true
kafka.bootstrap_servers = localhost:9092
kafka.topics = dev-events
```

## Run (production)

```bash
cd /workspace
pserve simulate_pyramid/production.ini

Alternatively, you can run with Gunicorn (no Waitress):

```bash
pip install gunicorn plaster_pastedeploy
gunicorn --paste simulate_pyramid/production.ini -b 0.0.0.0:6543
```
```

This app reads all configuration from the Pyramid `.ini` files; no use of `get_environment_config` is required.

