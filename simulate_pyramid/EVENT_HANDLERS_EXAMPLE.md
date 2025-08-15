# Event Handlers Example - Simulate Pyramid

Este exemplo demonstra como usar a extensão Event Handlers no módulo `simulate_pyramid` para controlar eventos de sucesso e falha do ConductorTask.

## Visão Geral

O exemplo inclui:

- **Handlers Personalizados**: Classes e funções para processar eventos
- **Integração com Pyramid**: Configuração automática na inicialização da aplicação
- **Endpoints de Monitoramento**: Rotas para visualizar estatísticas e status
- **Script de Teste**: Simulação de eventos para demonstrar a funcionalidade

## Estrutura do Exemplo

```
simulate_pyramid/
├── app/
│   ├── __init__.py          # Configuração da aplicação Pyramid
│   └── handlers.py          # Handlers personalizados
├── test_handlers.py         # Script de teste
├── development.ini          # Configuração de desenvolvimento
└── EVENT_HANDLERS_EXAMPLE.md # Esta documentação
```

## Handlers Implementados

### 1. TaskEventLogger
- **Função**: Registra eventos de sucesso e falha em arquivo JSON
- **Arquivo**: `task_events.log`
- **Formato**: JSON com timestamp, tipo de evento, dados da task

### 2. TaskMetricsCollector
- **Função**: Coleta métricas de sucesso e falha
- **Dados**: Contadores por task e totais
- **Acesso**: Via endpoint `/stats`

### 3. TaskNotifier
- **Função**: Envia notificações para tasks importantes
- **Critério**: Tasks de pagamento, ordem e email
- **Simulação**: Log das notificações (em produção seria Slack/Email)

### 4. ConditionalTaskHandler
- **Função**: Processamento específico por tipo de task
- **Tipos**: Email, Payment, Order, Generic
- **Ações**: Logs específicos com emojis e ações condicionais

## Como Executar

### 1. Iniciar a Aplicação Pyramid

```bash
cd simulate_pyramid
python -m pyramid.scripts.pserve development.ini
```

### 2. Executar o Script de Teste

```bash
cd simulate_pyramid
python test_handlers.py
```

### 3. Verificar os Endpoints

```bash
# Status da aplicação
curl http://localhost:6543/health

# Configurações
curl http://localhost:6543/settings

# Estatísticas dos handlers
curl http://localhost:6543/stats

# Informações dos handlers
curl http://localhost:6543/handlers
```

## Exemplo de Saída do Script de Teste

```
🚀 Starting ConductorTask event simulation...
==================================================
📈 Simulating SUCCESS events...
🎉 TEST: Task email_send_welcome completed successfully!
   Task ID: task_success_0_1703123456
   Workflow: workflow_0_1703123456
   Result: {'status': 'completed', 'data': 'result_data_0'}

🎉 Pyramid app: Task email_send_welcome completed successfully!
📧 Email task email_send_welcome completed successfully
Task success logged to task_events.log: email_send_welcome
Success metrics updated for email_send_welcome. Total successes: 1
SUCCESS NOTIFICATION: {"type": "success", "task_name": "email_send_welcome", ...}

📉 Simulating FAILURE events...
💥 TEST: Task payment_process_credit_card failed!
   Task ID: task_failure_0_1703123457
   Workflow: workflow_0_1703123457
   Error: Connection timeout

💥 Pyramid app: Task payment_process_credit_card failed!
💳 Payment task payment_process_credit_card failed: Connection timeout
Task failure logged to task_events.log: payment_process_credit_card
Failure metrics updated for payment_process_credit_card. Total failures: 1
FAILURE NOTIFICATION: {"type": "failure", "task_name": "payment_process_credit_card", ...}

✅ Event simulation completed!
==================================================
📊 Final Status:
   Handlers enabled: True
   Success handlers: 2
   Failure handlers: 2
```

## Exemplo de Resposta do Endpoint /stats

```json
{
  "application_stats": {
    "metrics": {
      "total_success": 5,
      "total_failure": 3,
      "task_stats": {
        "email_send_welcome": {"success": 2, "failure": 0},
        "payment_process_credit_card": {"success": 1, "failure": 2},
        "order_fulfillment_shipping": {"success": 2, "failure": 1}
      }
    },
    "notifications_sent": 8,
    "log_file": "task_events.log"
  },
  "handlers_status": {
    "enabled": true,
    "success_handlers_count": 1,
    "failure_handlers_count": 1
  },
  "timestamp": "2023-12-21T10:30:45.123456"
}
```

## Exemplo de Arquivo de Log (task_events.log)

```json
{"timestamp": "2023-12-21T10:30:45.123456", "event_type": "success", "task_name": "email_send_welcome", "task_id": "task_success_0_1703123456", "workflow_instance_id": "workflow_0_1703123456", "worker_id": "worker_3", "result": "{'status': 'completed', 'data': 'result_data_0'}", "args": ["arg_0"], "kwargs": {"param": "value_0"}}
{"timestamp": "2023-12-21T10:30:45.678901", "event_type": "failure", "task_name": "payment_process_credit_card", "task_id": "task_failure_0_1703123457", "workflow_instance_id": "workflow_0_1703123457", "worker_id": "worker_2", "error": "Connection timeout", "args": ["arg_0"], "kwargs": {"param": "value_0"}}
```

## Características Demonstradas

### ✅ Handlers Múltiplos
- Um handler principal que coordena outros handlers
- Handlers especializados para diferentes funcionalidades
- Registro de múltiplos handlers simultaneamente

### ✅ Processamento Condicional
- Diferentes ações baseadas no nome da task
- Notificações apenas para tasks importantes
- Logs específicos por tipo de task

### ✅ Coleta de Métricas
- Contadores de sucesso e falha
- Estatísticas por task individual
- Endpoint para visualização das métricas

### ✅ Logging Estruturado
- Logs em formato JSON
- Arquivo separado para eventos
- Timestamps precisos

### ✅ Notificações
- Simulação de notificações para tasks críticas
- Contador de notificações enviadas
- Formato padronizado para notificações

### ✅ Integração com Pyramid
- Configuração automática na inicialização
- Endpoints REST para monitoramento
- Logs integrados com o sistema da aplicação

## Personalização

### Adicionar Novos Handlers

```python
# Em app/handlers.py
def my_custom_handler(event: TaskSuccessEvent):
    # Sua lógica personalizada aqui
    pass

# Em app/__init__.py
register_success_handler(my_custom_handler)
```

### Modificar Critérios de Notificação

```python
# Em app/handlers.py, classe TaskNotifier
def send_success_notification(self, event: TaskSuccessEvent):
    # Adicione suas próprias condições
    if event.task_name in ["my_critical_task", "another_important_task"]:
        # Enviar notificação
        pass
```

### Adicionar Novos Tipos de Task

```python
# Em app/handlers.py, classe ConditionalTaskHandler
def handle_success_conditionally(self, event: TaskSuccessEvent):
    if event.task_name.startswith("my_custom_"):
        logger.info(f"🎯 Custom task {event.task_name} completed successfully")
        # Sua lógica específica aqui
```

## Monitoramento em Produção

### Logs
- Verifique o arquivo `task_events.log` para histórico completo
- Use ferramentas como ELK Stack para análise
- Configure rotação de logs

### Métricas
- Endpoint `/stats` para métricas em tempo real
- Integre com Prometheus/Grafana
- Configure alertas baseados em thresholds

### Notificações
- Substitua a simulação por integração real (Slack, Email, etc.)
- Configure diferentes níveis de notificação
- Implemente rate limiting para evitar spam

## Troubleshooting

### Handlers não estão sendo chamados
1. Verifique se a extensão está habilitada: `/handlers`
2. Confirme se os handlers estão registrados: `/stats`
3. Verifique os logs da aplicação

### Performance
1. Monitore o tempo de execução dos handlers
2. Use handlers assíncronos para operações pesadas
3. Implemente cache para dados frequentemente acessados

### Erros nos Handlers
1. Handlers com erro são logados mas não quebram a task
2. Verifique os logs da aplicação para erros
3. Implemente retry logic para operações que podem falhar

## Próximos Passos

1. **Integração Real**: Conecte com sistemas reais (Slack, Email, etc.)
2. **Persistência**: Use banco de dados para métricas e logs
3. **Alertas**: Configure alertas baseados em thresholds
4. **Dashboard**: Crie interface web para visualização
5. **Testes**: Adicione testes unitários para os handlers
