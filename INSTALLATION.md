# Guia de Instalação - Conductor Celery

Este guia explica como instalar a biblioteca `conductor_celery` com suas extensões opcionais.

## 🏗️ Para Desenvolvedores da Biblioteca

Se você está desenvolvendo a biblioteca `conductor_celery`, instale as dependências de desenvolvimento:

### Instalação Básica
```bash
# Instalar apenas dependências principais
make install
# ou
poetry install
```

### Instalação com Extensões para Desenvolvimento
```bash
# Com extensão Kafka
make install-kafka
# ou
poetry install -E kafka

# Com extensão Pyramid
make install-pyramid
# ou
poetry install -E pyramid

# Com todas as extensões
make install-all
# ou
poetry install -E all
```

### Testar as Extensões
```bash
# Executar demonstração
make demo

# Executar testes
make test
```

## 📦 Para Usuários da Biblioteca

Quando a biblioteca for publicada no PyPI, os usuários poderão instalar com as extensões desejadas:

### Instalação Básica
```bash
pip install conductor-celery
```

### Instalação com Extensões
```bash
# Com extensão Kafka
pip install conductor-celery[kafka]

# Com extensão Pyramid
pip install conductor-celery[pyramid]

# Com todas as extensões
pip install conductor-celery[all]
```

### Usando Poetry
```bash
# Com extensão Kafka
poetry add conductor-celery[kafka]

# Com extensão Pyramid
poetry add conductor-celery[pyramid]

# Com todas as extensões
poetry add conductor-celery[all]
```

## 🔧 Verificação de Instalação

Após a instalação, você pode verificar se as extensões estão disponíveis:

```python
from conductor_celery.ext import get_manager

manager = get_manager()

# Listar extensões disponíveis
print("Extensões disponíveis:", manager.list_available_extensions())

# Verificar se uma extensão específica está disponível
if manager.is_extension_available("kafka"):
    print("✅ Extensão Kafka está disponível")
else:
    print("❌ Extensão Kafka não está disponível")
```

## 📋 Dependências por Extensão

### Extensão Kafka
- **Dependência**: `kafka-python>=2.2.15`
- **Uso**: Para integração com Apache Kafka
- **Instalação**: `pip install conductor-celery[kafka]`

### Extensão Pyramid
- **Dependência**: `pyramid>=2.0.0`
- **Uso**: Para integração com framework Pyramid
- **Instalação**: `pip install conductor-celery[pyramid]`

### Todas as Extensões
- **Dependências**: `kafka-python>=2.2.15`, `pyramid>=2.0.0`
- **Uso**: Para usar todas as integrações disponíveis
- **Instalação**: `pip install conductor-celery[all]`

## 🚀 Exemplo de Uso Após Instalação

```python
# Importar a biblioteca
from conductor_celery import tasks, wrapper

# Verificar se extensões estão disponíveis
from conductor_celery.ext import get_manager

manager = get_manager()

# Usar extensão Kafka (se disponível)
if manager.is_extension_available("kafka"):
    from conductor_celery.ext import create_extension
    
    kafka_ext = create_extension("kafka", {
        'bootstrap_servers': ['localhost:9092']
    })
    
    with kafka_ext:
        kafka_ext.send_message("my-topic", b"Hello, Kafka!")

# Usar extensão Pyramid (se disponível)
if manager.is_extension_available("pyramid"):
    from conductor_celery.ext import create_extension
    
    pyramid_ext = create_extension("pyramid", {
        'app_name': 'my_app'
    })
    
    # Configurar aplicação Pyramid
    # pyramid_ext.set_app(my_pyramid_app)
```

## 🔍 Solução de Problemas

### Extensão não encontrada
```bash
# Verificar se a extensão foi instalada
pip list | grep kafka-python
pip list | grep pyramid

# Reinstalar com a extensão
pip install conductor-celery[kafka] --force-reinstall
```

### Erro de importação
```python
# Verificar se a extensão está disponível antes de usar
from conductor_celery.ext import get_manager

manager = get_manager()
if not manager.is_extension_available("kafka"):
    print("Instale a extensão Kafka: pip install conductor-celery[kafka]")
```

### Dependências conflitantes
```bash
# Criar ambiente virtual limpo
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate     # Windows

# Instalar biblioteca com extensões
pip install conductor-celery[all]
```
