# Event Handlers Extension

A extensão Event Handlers permite que as aplicações registrem handlers personalizados para eventos de sucesso e falha do ConductorTask sem modificar o código da biblioteca.

## Visão Geral

Esta extensão resolve o problema de ter eventos hardcoded na classe `ConductorTask`, permitindo que cada aplicação defina seus próprios handlers para:

- **Eventos de Sucesso**: Quando uma task é executada com sucesso
- **Eventos de Falha**: Quando uma task falha durante a execução

## Características

- ✅ **Não invasivo**: Não requer modificações no código da biblioteca
- ✅ **Flexível**: Permite múltiplos handlers por tipo de evento
- ✅ **Configurável**: Suporte a configuração via código ou arquivo de configuração
- ✅ **Robusto**: Handlers com erro não quebram a execução da task
- ✅ **Backward Compatible**: Mantém a funcionalidade original intacta

## Instalação

A extensão é automaticamente carregada quando você importa o módulo `conductor_celery.ext`.

## Uso Básico

### 1. Registrar Handlers Simples

```python
from conductor_celery.ext.event_handlers import (
    TaskSuccessEvent,
    TaskFailureEvent,
    register_success_handler,
    register_failure_handler,
    enable_event_handlers
)

# Handler para sucesso
def my_success_handler(event: TaskSuccessEvent):
    print(f"Task {event.task_name} completed successfully!")
    print(f"Result: {event.result}")

# Handler para falha
def my_failure_handler(event: TaskFailureEvent):
    print(f"Task {event.task_name} failed!")
    print(f"Error: {event.error}")

# Registrar os handlers
register_success_handler(my_success_handler)
register_failure_handler(my_failure_handler)

# Habilitar a extensão
enable_event_handlers()
```

### 2. Handler com Logging

```python
import logging

logger = logging.getLogger(__name__)

def log_success(event: TaskSuccessEvent):
    logger.info(
        f"Task {event.task_name} ({event.task_id}) completed successfully "
        f"for workflow {event.workflow_instance_id}"
    )

def log_failure(event: TaskFailureEvent):
    logger.error(
        f"Task {event.task_name} ({event.task_id}) failed "
        f"for workflow {event.workflow_instance_id}: {event.error}"
    )

register_success_handler(log_success)
register_failure_handler(log_failure)
enable_event_handlers()
```

### 3. Handler Condicional

```python
def conditional_handler(event: TaskSuccessEvent):
    if event.task_name.startswith("email_"):
        print("Email task completed!")
    elif event.task_name.startswith("payment_"):
        print("Payment task completed!")
    else:
        print("Generic task completed!")

register_success_handler(conditional_handler)
enable_event_handlers()
```

## Estrutura dos Eventos

### TaskSuccessEvent

```python
@dataclass
class TaskSuccessEvent:
    task_name: str              # Nome da task
    task_id: str               # ID da task
    workflow_instance_id: str  # ID da instância do workflow
    worker_id: str             # ID do worker
    result: Any                # Resultado da task
    args: tuple                # Argumentos passados para a task
    kwargs: dict               # Argumentos nomeados passados para a task
```

### TaskFailureEvent

```python
@dataclass
class TaskFailureEvent:
    task_name: str              # Nome da task
    task_id: str               # ID da task
    workflow_instance_id: str  # ID da instância do workflow
    worker_id: str             # ID do worker
    error: str                 # Mensagem de erro
    args: tuple                # Argumentos passados para a task
    kwargs: dict               # Argumentos nomeados passados para a task
```

## API Completa

### Funções de Registro

```python
# Registrar handlers
register_success_handler(handler_function)
register_failure_handler(handler_function)

# Remover handlers
unregister_success_handler(handler_function)
unregister_failure_handler(handler_function)
```

### Funções de Controle

```python
# Habilitar/desabilitar a extensão
enable_event_handlers()
disable_event_handlers()

# Verificar status
status = get_event_handlers_status()
print(status)
# Output: {
#     "enabled": True,
#     "success_handlers_count": 2,
#     "failure_handlers_count": 1
# }
```

## Exemplos Avançados

### 1. Múltiplos Handlers

```python
def log_handler(event: TaskSuccessEvent):
    logger.info(f"Task {event.task_name} completed")

def metrics_handler(event: TaskSuccessEvent):
    # Enviar métricas para Prometheus/StatsD
    pass

def notification_handler(event: TaskSuccessEvent):
    # Enviar notificação Slack/Email
    pass

# Registrar múltiplos handlers
register_success_handler(log_handler)
register_success_handler(metrics_handler)
register_success_handler(notification_handler)
enable_event_handlers()
```

### 2. Handler com Banco de Dados

```python
def log_to_database(event: TaskFailureEvent):
    # Inserir log de erro no banco de dados
    error_log = {
        "task_name": event.task_name,
        "task_id": event.task_id,
        "workflow_id": event.workflow_instance_id,
        "error": event.error,
        "timestamp": datetime.now()
    }
    # db.insert("task_errors", error_log)

register_failure_handler(log_to_database)
enable_event_handlers()
```

### 3. Handler com Notificações

```python
def send_slack_notification(event: TaskFailureEvent):
    if event.task_name in ["payment_process", "order_fulfillment"]:
        # Enviar alerta para Slack
        slack_message = f"🚨 Task {event.task_name} failed: {event.error}"
        # slack.send_message(slack_message)

register_failure_handler(send_slack_notification)
enable_event_handlers()
```

## Integração com Frameworks

### Pyramid

```python
# Em __init__.py da aplicação
def main(global_config, **settings):
    config = Configurator(settings=settings)
    
    # Configurar event handlers
    from conductor_celery.ext.event_handlers import (
        register_success_handler, 
        register_failure_handler,
        enable_event_handlers
    )
    
    def pyramid_success_handler(event):
        # Handler específico para Pyramid
        pass
    
    register_success_handler(pyramid_success_handler)
    enable_event_handlers()
    
    return config.make_wsgi_app()
```

## Configuração via Arquivo

Você também pode configurar handlers via arquivo de configuração:

```python
# config.py
CONDUCTOR_CELERY_EXTENSIONS = {
    "event_handlers": {
        "enabled": True,
        "success_handlers": [
            "myapp.handlers.log_success",
            "myapp.handlers.send_notification"
        ],
        "failure_handlers": [
            "myapp.handlers.log_failure",
            "myapp.handlers.send_alert"
        ]
    }
}
```

## Boas Práticas

1. **Sempre trate exceções**: Handlers com erro não quebram a execução da task
2. **Use logging**: Registre informações importantes nos handlers
3. **Seja específico**: Crie handlers específicos para diferentes tipos de tasks
4. **Teste os handlers**: Certifique-se de que os handlers funcionam corretamente
5. **Monitore performance**: Handlers muito pesados podem afetar a performance

## Troubleshooting

### Handler não está sendo chamado

1. Verifique se a extensão está habilitada:
   ```python
   status = get_event_handlers_status()
   print(status["enabled"])  # Deve ser True
   ```

2. Verifique se o handler está registrado:
   ```python
   status = get_event_handlers_status()
   print(status["success_handlers_count"])  # Deve ser > 0
   ```

3. Verifique se há erros no log da aplicação

### Handler com erro

Handlers com erro são automaticamente logados e não quebram a execução da task. Verifique os logs para identificar o problema.

## Migração do Código Existente

Se você já tem código que modifica diretamente a classe `ConductorTask`, você pode migrar gradualmente:

1. **Fase 1**: Mantenha o código existente e adicione os novos handlers
2. **Fase 2**: Mova a lógica para handlers personalizados
3. **Fase 3**: Remova o código modificado da classe `ConductorTask`

## Contribuindo

Para contribuir com melhorias na extensão:

1. Mantenha a compatibilidade com versões anteriores
2. Adicione testes para novas funcionalidades
3. Atualize a documentação
4. Siga as convenções de código existentes
