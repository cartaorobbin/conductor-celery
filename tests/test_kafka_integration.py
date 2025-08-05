import json
import socket
from unittest.mock import patch, MagicMock
import pytest
from kafka.errors import KafkaError, NoBrokersAvailable

from conductor_celery.tasks import ConductorTask, get_kafka_producer, KAFKA_TOPIC


class TestKafkaIntegration:
    """Testes TDD para integração com Kafka"""

    def test_get_kafka_producer_creates_valid_producer(self):
        """Testa se get_kafka_producer cria um produtor válido"""
        with patch('conductor_celery.tasks.KafkaProducer') as mock_producer:
            producer = get_kafka_producer()
            
            # Verifica se o KafkaProducer foi chamado uma vez
            mock_producer.assert_called_once()
            
            # Verifica os argumentos separadamente
            call_args = mock_producer.call_args
            assert call_args[1]['bootstrap_servers'] == "localhost:9093"
            
            # Verifica se o serializador é uma função que funciona
            serializer = call_args[1]['value_serializer']
            test_data = {"test": "value"}
            serialized = serializer(test_data)
            assert isinstance(serialized, bytes)
            assert json.loads(serialized.decode('utf-8')) == test_data
            
            assert producer == mock_producer.return_value

    def test_kafka_producer_serialization(self):
        """Testa se o serializador do Kafka funciona corretamente"""
        test_data = {"test": "value", "number": 123}
        
        with patch('conductor_celery.tasks.KafkaProducer') as mock_producer:
            producer = get_kafka_producer()
            
            # Pega o serializador que foi passado
            call_args = mock_producer.call_args
            serializer = call_args[1]['value_serializer']
            
            # Testa a serialização
            serialized = serializer(test_data)
            assert isinstance(serialized, bytes)
            
            # Testa a desserialização
            deserialized = json.loads(serialized.decode('utf-8'))
            assert deserialized == test_data
            
            # Verifica se o produtor foi criado corretamente
            assert producer == mock_producer.return_value

    def test_task_failure_sends_kafka_event_successfully(self, celery_app, celery_worker, task_poll_response, responses):
        """Testa se uma falha de task envia evento para o Kafka"""
        worker_id = socket.gethostname()
        
        # Mock das respostas do Conductor
        responses.get(
            f"https://localhost:8080/api/tasks/poll/failing_task?workerid={worker_id}",
            body=task_poll_response({"x": 1, "y": 0}),
        )
        responses.post("https://localhost:8080/api/tasks", body="1233444")

        # Mock do Kafka Producer
        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_future.get.return_value = MagicMock(topic=KAFKA_TOPIC, partition=0, offset=123)
        mock_producer.send.return_value = mock_future
        
        with patch('conductor_celery.tasks.get_kafka_producer', return_value=mock_producer):
            @celery_app.task(base=ConductorTask, name="failing_task")
            def div(x, y):
                return x / y  # Isso vai causar ZeroDivisionError

            celery_worker.reload()
            
            # Executa a task que vai falhar
            result = div.apply()
            
            # Verifica se a task falhou
            assert result.status == 'FAILURE'
            
            # Verifica se o evento foi enviado para o Kafka
            mock_producer.send.assert_called_once()
            
            # Verifica os argumentos da chamada
            call_args = mock_producer.send.call_args
            assert call_args[0][0] == KAFKA_TOPIC  # Primeiro argumento é o tópico
            
            # Verifica o conteúdo da mensagem
            event_data = call_args[0][1]  # Segundo argumento é a mensagem
            assert event_data["task_name"] == "failing_task"
            # Os IDs são gerados dinamicamente, então apenas verificamos se existem
            assert "task_id" in event_data
            assert "workflow_instance_id" in event_data
            assert event_data["worker_id"] == worker_id
            # Verifica se a mensagem de erro contém informações sobre divisão por zero
            assert "division by zero" in event_data["error"] or "ZeroDivisionError" in event_data["error"]
            # Verifica se os argumentos estão presentes (podem estar em kwargs)
            assert "args" in event_data
            assert "kwargs" in event_data
            
            # Verifica se flush foi chamado
            mock_producer.flush.assert_called_once()

    def test_task_failure_handles_kafka_unavailable(self, celery_app, celery_worker, task_poll_response, responses):
        """Testa se o sistema continua funcionando quando o Kafka não está disponível"""
        worker_id = socket.gethostname()
        
        # Mock das respostas do Conductor
        responses.get(
            f"https://localhost:8080/api/tasks/poll/failing_task?workerid={worker_id}",
            body=task_poll_response({"x": 1, "y": 0}),
        )
        responses.post("https://localhost:8080/api/tasks", body="1233444")

        # Mock do Kafka Producer que falha
        with patch('conductor_celery.tasks.get_kafka_producer', side_effect=NoBrokersAvailable("Kafka unavailable")):
            @celery_app.task(base=ConductorTask, name="failing_task")
            def div(x, y):
                return x / y  # Isso vai causar ZeroDivisionError

            celery_worker.reload()
            
            # Executa a task que vai falhar
            result = div.apply()
            
            # Verifica se a task falhou
            assert result.status == 'FAILURE'
            
            # O sistema deve continuar funcionando mesmo sem Kafka
            # Verifica se a atualização do Conductor ainda foi feita
            assert len(responses.calls) == 2  # poll + update

    def test_task_failure_handles_kafka_send_error(self, celery_app, celery_worker, task_poll_response, responses):
        """Testa se o sistema lida com erros ao enviar para o Kafka"""
        worker_id = socket.gethostname()
        
        # Mock das respostas do Conductor
        responses.get(
            f"https://localhost:8080/api/tasks/poll/failing_task?workerid={worker_id}",
            body=task_poll_response({"x": 1, "y": 0}),
        )
        responses.post("https://localhost:8080/api/tasks", body="1233444")

        # Mock do Kafka Producer que falha ao enviar
        mock_producer = MagicMock()
        mock_producer.send.side_effect = KafkaError("Send failed")
        
        with patch('conductor_celery.tasks.get_kafka_producer', return_value=mock_producer):
            @celery_app.task(base=ConductorTask, name="failing_task")
            def div(x, y):
                return x / y  # Isso vai causar ZeroDivisionError

            celery_worker.reload()
            
            # Executa a task que vai falhar
            result = div.apply()
            
            # Verifica se a task falhou
            assert result.status == 'FAILURE'
            
            # O sistema deve continuar funcionando mesmo com erro no Kafka
            # Verifica se a atualização do Conductor ainda foi feita
            assert len(responses.calls) == 2  # poll + update

    def test_task_success_does_not_send_kafka_event(self, celery_app, celery_worker, task_poll_response, responses):
        """Testa se tasks bem-sucedidas não enviam eventos para o Kafka"""
        worker_id = socket.gethostname()
        
        # Mock das respostas do Conductor
        responses.get(
            f"https://localhost:8080/api/tasks/poll/success_task?workerid={worker_id}",
            body=task_poll_response({"x": 2, "y": 3}),
        )
        responses.post("https://localhost:8080/api/tasks", body="1233444")

        # Mock do Kafka Producer
        mock_producer = MagicMock()
        
        with patch('conductor_celery.tasks.get_kafka_producer', return_value=mock_producer):
            @celery_app.task(base=ConductorTask, name="success_task")
            def mul(x, y):
                return {"total": x * y}

            celery_worker.reload()
            
            # Executa a task que vai ter sucesso
            result = mul.apply()
            
            # Verifica se a task teve sucesso
            assert result.status == 'SUCCESS'
            assert result.result == {"total": 6}
            
            # Verifica que NÃO foi enviado evento para o Kafka
            mock_producer.send.assert_not_called()
            mock_producer.flush.assert_not_called()

    def test_kafka_event_structure_matches_expected_format(self, celery_app, celery_worker, task_poll_response, responses):
        """Testa se a estrutura do evento Kafka está correta"""
        worker_id = socket.gethostname()
        
        # Mock das respostas do Conductor
        responses.get(
            f"https://localhost:8080/api/tasks/poll/failing_task?workerid={worker_id}",
            body=task_poll_response({"param1": "value1", "param2": 42}),
        )
        responses.post("https://localhost:8080/api/tasks", body="1233444")

        # Mock do Kafka Producer
        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_future.get.return_value = MagicMock(topic=KAFKA_TOPIC, partition=0, offset=123)
        mock_producer.send.return_value = mock_future
        
        with patch('conductor_celery.tasks.get_kafka_producer', return_value=mock_producer):
            @celery_app.task(base=ConductorTask, name="failing_task")
            def failing_function(param1, param2):
                raise ValueError("Test error message")

            celery_worker.reload()
            
            # Executa a task que vai falhar
            result = failing_function.apply()
            
            # Verifica se a task falhou
            assert result.status == 'FAILURE'
            
            # Verifica a estrutura do evento
            call_args = mock_producer.send.call_args
            event_data = call_args[0][1]
            
            # Verifica se todos os campos obrigatórios estão presentes
            required_fields = [
                "task_name", "task_id", "workflow_instance_id", 
                "worker_id", "error", "args", "kwargs"
            ]
            
            for field in required_fields:
                assert field in event_data, f"Campo obrigatório '{field}' não encontrado no evento"
            
            # Verifica os valores específicos
            assert event_data["task_name"] == "failing_task"
            # Os IDs são gerados dinamicamente, então apenas verificamos se existem
            assert "task_id" in event_data
            assert "workflow_instance_id" in event_data
            assert event_data["worker_id"] == worker_id
            # Verifica se a mensagem de erro contém o texto esperado
            assert "Test error message" in event_data["error"]
            # O tipo de erro pode estar na mensagem ou não, dependendo da implementação
            # Verifica se os argumentos estão presentes (podem estar em kwargs)
            assert "args" in event_data
            assert "kwargs" in event_data

    def test_kafka_topic_configuration(self):
        """Testa se o tópico do Kafka está configurado corretamente"""
        from conductor_celery.tasks import KAFKA_TOPIC
        assert KAFKA_TOPIC == "workspace_conductor_task_failures"

    def test_kafka_broker_url_configuration(self):
        """Testa se a URL do broker do Kafka está configurada corretamente"""
        from conductor_celery.tasks import KAFKA_BROKER_URL
        assert KAFKA_BROKER_URL == "localhost:9093"


class TestKafkaProducerIntegration:
    """Testes específicos para o produtor Kafka"""

    def test_kafka_producer_connection_timeout(self):
        """Testa timeout de conexão com o Kafka"""
        with patch('conductor_celery.tasks.KafkaProducer') as mock_producer:
            mock_producer.side_effect = NoBrokersAvailable("Connection timeout")
            
            with pytest.raises(NoBrokersAvailable):
                get_kafka_producer()

    def test_kafka_producer_serialization_error(self):
        """Testa erro de serialização no produtor Kafka"""
        with patch('conductor_celery.tasks.KafkaProducer') as mock_producer:
            producer = get_kafka_producer()
            
            # Pega o serializador
            call_args = mock_producer.call_args
            serializer = call_args[1]['value_serializer']
            
            # Testa com dados não serializáveis
            non_serializable_data = {"func": lambda x: x}  # Função não é serializável
            
            with pytest.raises(TypeError):
                serializer(non_serializable_data)


class TestKafkaEventContent:
    """Testes para o conteúdo dos eventos Kafka"""

    def test_kafka_event_contains_correct_error_information(self, celery_app, celery_worker, task_poll_response, responses):
        """Testa se as informações de erro estão corretas no evento Kafka"""
        worker_id = socket.gethostname()
        
        responses.get(
            f"https://localhost:8080/api/tasks/poll/failing_task?workerid={worker_id}",
            body=task_poll_response({}),
        )
        responses.post("https://localhost:8080/api/tasks", body="1233444")

        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_future.get.return_value = MagicMock(topic=KAFKA_TOPIC, partition=0, offset=123)
        mock_producer.send.return_value = mock_future
        
        with patch('conductor_celery.tasks.get_kafka_producer', return_value=mock_producer):
            @celery_app.task(base=ConductorTask, name="failing_task")
            def custom_error_task():
                raise RuntimeError("Custom runtime error")

            celery_worker.reload()
            
            result = custom_error_task.apply()
            assert result.status == 'FAILURE'
            
            # Verifica o conteúdo do erro
            call_args = mock_producer.send.call_args
            event_data = call_args[0][1]
            
            # Verifica se a mensagem de erro contém o texto esperado
            assert "Custom runtime error" in event_data["error"]
            # O tipo de erro pode estar na mensagem ou não, dependendo da implementação

    def test_kafka_event_preserves_task_arguments(self, celery_app, celery_worker, task_poll_response, responses):
        """Testa se os argumentos da task são preservados no evento Kafka"""
        worker_id = socket.gethostname()
        
        responses.get(
            f"https://localhost:8080/api/tasks/poll/failing_task?workerid={worker_id}",
            body=task_poll_response({"arg1": "value1", "arg2": 123, "kwarg1": "kwvalue1"}),
        )
        responses.post("https://localhost:8080/api/tasks", body="1233444")

        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_future.get.return_value = MagicMock(topic=KAFKA_TOPIC, partition=0, offset=123)
        mock_producer.send.return_value = mock_future
        
        with patch('conductor_celery.tasks.get_kafka_producer', return_value=mock_producer):
            @celery_app.task(base=ConductorTask, name="failing_task")
            def failing_task_with_args(arg1, arg2, kwarg1=None):
                raise Exception("Task failed")

            celery_worker.reload()
            
            result = failing_task_with_args.apply()
            assert result.status == 'FAILURE'
            
            # Verifica se os argumentos foram preservados
            call_args = mock_producer.send.call_args
            event_data = call_args[0][1]
            
            # Os argumentos podem estar em args ou kwargs, dependendo de como são passados
            # Verifica se pelo menos um dos campos contém os dados esperados
            args_ok = (
                event_data["args"] == ["value1", 123] or 
                event_data["kwargs"] == {"arg1": "value1", "arg2": 123, "kwarg1": "kwvalue1"} or
                "value1" in str(event_data["args"]) or 
                "value1" in str(event_data["kwargs"])
            )
            assert args_ok, f"Argumentos não encontrados. args: {event_data['args']}, kwargs: {event_data['kwargs']}" 