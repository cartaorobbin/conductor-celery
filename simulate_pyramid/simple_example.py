#!/usr/bin/env python3
"""
Exemplo Simples - Event Handlers Extension

Este exemplo mostra como usar a extensão Event Handlers de forma
simples e direta, sem a complexidade do exemplo completo.
"""

import os
import sys

from conductor_celery.ext.event_handlers import (
    TaskFailureEvent,
    TaskSuccessEvent,
    enable_event_handlers,
    get_event_handlers_status,
    register_failure_handler,
    register_success_handler,
)

# Adicionar o diretório pai ao path para importar conductor_celery
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def simple_success_handler(event: TaskSuccessEvent):
    """Handler simples para eventos de sucesso."""
    print(f"✅ Task {event.task_name} completed successfully!")
    print(f"   Result: {event.result}")
    print(f"   Workflow: {event.workflow_instance_id}")
    print()


def simple_failure_handler(event: TaskFailureEvent):
    """Handler simples para eventos de falha."""
    print(f"❌ Task {event.task_name} failed!")
    print(f"   Error: {event.error}")
    print(f"   Workflow: {event.workflow_instance_id}")
    print()


def conditional_handler(event: TaskSuccessEvent):
    """Handler condicional baseado no nome da task."""
    if event.task_name.startswith("email_"):
        print(f"📧 Email task {event.task_name} completed!")
    elif event.task_name.startswith("payment_"):
        print(f"💳 Payment task {event.task_name} completed!")
    elif event.task_name.startswith("order_"):
        print(f"📦 Order task {event.task_name} completed!")
    else:
        print(f"🔧 Generic task {event.task_name} completed!")


def main():
    """Função principal do exemplo."""
    print("🚀 Exemplo Simples - Event Handlers Extension")
    print("=" * 50)

    # 1. Registrar os handlers
    print("1. Registrando handlers...")
    register_success_handler(simple_success_handler)
    register_success_handler(conditional_handler)
    register_failure_handler(simple_failure_handler)

    # 2. Habilitar a extensão
    print("2. Habilitando extensão...")
    enable_event_handlers()

    # 3. Verificar status
    status = get_event_handlers_status()
    print(f"3. Status: {status}")
    print()

    # 4. Simular eventos
    print("4. Simulando eventos...")

    # Evento de sucesso
    success_event = TaskSuccessEvent(
        task_name="email_send_welcome",
        task_id="task_123",
        workflow_instance_id="workflow_456",
        worker_id="worker_1",
        result={"status": "sent", "recipient": "user@example.com"},
        args=(),
        kwargs={"template": "welcome"},
    )

    # Evento de falha
    failure_event = TaskFailureEvent(
        task_name="payment_process",
        task_id="task_789",
        workflow_instance_id="workflow_456",
        worker_id="worker_2",
        error="Insufficient funds",
        args=(),
        kwargs={"amount": 100.00},
    )

    # 5. Disparar os eventos
    print("5. Disparando eventos...")

    # Disparar evento de sucesso
    from conductor_celery.ext.manager import get_manager

    manager = get_manager()
    event_handlers = manager.get_extension("event_handlers")

    if event_handlers:
        print("\n--- Evento de Sucesso ---")
        event_handlers.handle_success(success_event)

        print("\n--- Evento de Falha ---")
        event_handlers.handle_failure(failure_event)

    print("\n✅ Exemplo concluído!")


if __name__ == "__main__":
    main()
