# Extensões do Conductor Celery

Este módulo fornece um sistema de extensões opcionais para a biblioteca `conductor_celery`, permitindo integração com sistemas externos como Kafka e Pyramid.

## Visão Geral

O sistema de extensões permite que você use a biblioteca `conductor_celery` com ou sem dependências específicas. As extensões são carregadas dinamicamente baseadas na disponibilidade das dependências.

## Extensões Disponíveis

### Kafka Extension
Fornece integração com Apache Kafka para envio e recebimento de mensagens.

**Dependência**: `kafka-python`

**Instalação**:
```bash
poetry install -E kafka
```

**Uso**:
```python
from conductor_celery.ext import create_extension

# Criar extensão Kafka
kafka_ext = create_extension("kafka", {
    'bootstrap_servers': ['localhost:9092'],
    'topics': ['my-topic']
})

# Inicializar
kafka_ext.initialize()

# Enviar mensagem
kafka_ext.send_message("my-topic", b"Hello, Kafka!")

# Receber mensagens
messages = kafka_ext.get_messages(timeout_ms=1000)

# Limpar recursos
kafka_ext.cleanup()
```

### Pyramid Extension
Fornece integração com o framework Pyramid para aplicações web.

**Dependência**: `pyramid`

**Instalação**:
```bash
poetry install -E pyramid
```

**Uso**:
```python
from conductor_celery.ext import create_extension

# Criar extensão Pyramid
pyramid_ext = create_extension("pyramid", {
    'app_name': 'my_app'
})

# Configurar aplicação Pyramid
pyramid_ext.set_app(my_pyramid_app)

# Obter configurações
debug_mode = pyramid_ext.get_setting('debug', False)

# Adicionar rotas
pyramid_ext.add_route('home', '/', home_view)
```

## Gerenciador de Extensões

O `ExtensionManager` fornece uma interface conveniente para gerenciar múltiplas extensões:

```python
from conductor_celery.ext import get_manager

manager = get_manager()

# Verificar extensões disponíveis
available = manager.list_available_extensions()
print(f"Extensões disponíveis: {available}")

# Criar e gerenciar extensões
kafka_ext = manager.create_extension("kafka", kafka_config)
pyramid_ext = manager.create_extension("pyramid", pyramid_config)

# Inicializar todas as extensões
with manager:
    # Todas as extensões são inicializadas automaticamente
    pass
# Todas as extensões são limpas automaticamente
```

## Verificação de Disponibilidade

Você pode verificar se uma extensão está disponível antes de usá-la:

```python
from conductor_celery.ext import get_manager

manager = get_manager()

if manager.is_extension_available("kafka"):
    # Usar extensão Kafka
    kafka_ext = manager.create_extension("kafka", config)
else:
    print("Extensão Kafka não está disponível")
```

## Configuração

Cada extensão aceita configurações específicas:

### Kafka Configuration
```python
kafka_config = {
    'bootstrap_servers': ['localhost:9092'],
    'topics': ['my-topic'],
    'producer_config': {
        'value_serializer': lambda v: v.encode('utf-8'),
        'key_serializer': lambda k: k.encode('utf-8') if k else None
    },
    'consumer_config': {
        'group_id': 'my-group',
        'auto_offset_reset': 'earliest'
    }
}
```

### Pyramid Configuration
```python
pyramid_config = {
    'app_name': 'my_app',
    'settings': {
        'debug': True,
        'reload_templates': True
    }
}
```

## Context Manager

Todas as extensões suportam o protocolo de context manager:

```python
from conductor_celery.ext import create_extension

kafka_ext = create_extension("kafka", config)

with kafka_ext:
    # Extensão é inicializada automaticamente
    kafka_ext.send_message("topic", b"message")
# Extensão é limpa automaticamente
```

## Desenvolvimento

Para adicionar uma nova extensão:

1. Crie uma nova classe que herda de `BaseExtension`
2. Implemente os métodos obrigatórios: `is_available()`, `initialize()`, `cleanup()`
3. Registre a extensão no `__init__.py` do módulo `ext`
4. Adicione as dependências opcionais no `pyproject.toml`

```python
from .base import BaseExtension

class MyExtension(BaseExtension):
    def is_available(self) -> bool:
        try:
            import my_dependency
            return True
        except ImportError:
            return False
    
    def initialize(self) -> None:
        # Configurar recursos
        pass
    
    def cleanup(self) -> None:
        # Limpar recursos
        pass
```
