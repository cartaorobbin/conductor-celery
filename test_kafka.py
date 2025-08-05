#!/usr/bin/env python3
"""
Script para testar se o Kafka está funcionando corretamente.
Este script testa a conectividade, produção e consumo de mensagens.
"""
import os
import json
import time
from conductor_celery.utils import configure_env
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
import sys

# Configurações do Kafka (mesmas do seu projeto)
configure_env()

KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "kafka:9093")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "workspace_conductor_task_failures")
TEST_TOPIC = os.getenv("TEST_TOPIC", "test_topic")

def test_kafka_connection():
    """Testa a conectividade básica com o Kafka"""
    print("🔍 Testando conectividade com o Kafka...")
    
    try:
        # Tenta criar um produtor
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER_URL,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=5000,
            api_version_auto_timeout_ms=5000
        )
        
        # Testa a conexão
        producer.metrics()
        producer.close()
        
        print("✅ Conectividade com o Kafka: OK")
        return True
        
    except NoBrokersAvailable:
        print("❌ Erro: Não foi possível conectar ao Kafka")
        print("   Verifique se o Kafka está rodando em localhost:9093")
        return False
    except Exception as e:
        print(f"❌ Erro de conectividade: {e}")
        return False

def test_producer():
    """Testa a produção de mensagens"""
    print("\n📤 Testando produção de mensagens...")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER_URL,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        
        # Mensagem de teste
        test_message = {
            "test": True,
            "message": "Teste de conectividade do Kafka",
            "timestamp": time.time()
        }
        
        # Envia a mensagem
        future = producer.send(TEST_TOPIC, test_message)
        
        # Aguarda o resultado
        record_metadata = future.get(timeout=10)
        
        print(f"✅ Mensagem enviada com sucesso!")
        print(f"   Tópico: {record_metadata.topic}")
        print(f"   Partição: {record_metadata.partition}")
        print(f"   Offset: {record_metadata.offset}")
        
        producer.flush()
        producer.close()
        return True
        
    except Exception as e:
        print(f"❌ Erro ao produzir mensagem: {e}")
        return False

def test_consumer():
    """Testa o consumo de mensagens"""
    print("\n📥 Testando consumo de mensagens...")
    
    try:
        consumer = KafkaConsumer(
            TEST_TOPIC,
            bootstrap_servers=KAFKA_BROKER_URL,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id='test_consumer_group',
            consumer_timeout_ms=10000  # Timeout de 10 segundos
        )
        
        print("   Aguardando mensagens...")
        message_count = 0
        
        for message in consumer:
            print(f"✅ Mensagem recebida: {message.value}")
            message_count += 1
            if message_count >= 1:  # Para após receber uma mensagem
                break
        
        consumer.close()
        
        if message_count > 0:
            print(f"✅ Consumo de mensagens: OK ({message_count} mensagem recebida)")
            return True
        else:
            print("⚠️  Nenhuma mensagem foi recebida no timeout")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao consumir mensagens: {e}")
        return False

def test_your_topic():
    """Testa especificamente o tópico usado no seu projeto"""
    print(f"\n🎯 Testando o tópico do projeto: {KAFKA_TOPIC}")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER_URL,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        
        # Simula uma mensagem de falha de task (como no seu código)
        test_failure_event = {
            "task_name": "test_task",
            "task_id": "test_task_123",
            "workflow_instance_id": "workflow_456",
            "worker_id": "worker_789",
            "error": "Teste de erro simulado",
            "args": [],
            "kwargs": {}
        }
        
        future = producer.send(KAFKA_TOPIC, test_failure_event)
        record_metadata = future.get(timeout=10)
        
        print(f"✅ Mensagem de falha enviada para {KAFKA_TOPIC}!")
        print(f"   Partição: {record_metadata.partition}")
        print(f"   Offset: {record_metadata.offset}")
        
        producer.flush()
        producer.close()
        return True
        
    except Exception as e:
        print(f"❌ Erro ao enviar mensagem para {KAFKA_TOPIC}: {e}")
        return False

def main():
    """Função principal que executa todos os testes"""
    print("🚀 Iniciando testes do Kafka...")
    print(f"   Broker: {KAFKA_BROKER_URL}")
    print(f"   Tópico do projeto: {KAFKA_TOPIC}")
    print("=" * 50)
    
    # Executa os testes
    tests = [
        ("Conectividade", test_kafka_connection),
        ("Produção", test_producer),
        ("Consumo", test_consumer),
        ("Tópico do Projeto", test_your_topic)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ Erro inesperado no teste {test_name}: {e}")
            results.append((test_name, False))
    
    # Resumo dos resultados
    print("\n" + "=" * 50)
    print("📊 RESUMO DOS TESTES:")
    print("=" * 50)
    
    passed = 0
    for test_name, result in results:
        status = "✅ PASSOU" if result else "❌ FALHOU"
        print(f"{test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\nTotal: {passed}/{len(results)} testes passaram")
    
    if passed == len(results):
        print("🎉 Todos os testes passaram! O Kafka está funcionando corretamente.")
        return 0
    else:
        print("⚠️  Alguns testes falharam. Verifique a configuração do Kafka.")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 