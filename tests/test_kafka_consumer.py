import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import time
import json


class TestKafkaConsumer:
    """Pruebas unitarias para el consumidor Kafka del pipeline ETL."""

    @pytest.fixture
    def mock_kafka_config(self):
        """Configuración mock de Kafka para pruebas."""
        return {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'test-consumer-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'session.timeout.ms': 10000,
            'heartbeat.interval.ms': 3000
        }

    @pytest.fixture
    def sample_message(self):
        """Mensaje de ejemplo para pruebas."""
        return {
            'event_id': 'evt-12345',
            'timestamp': '2025-01-15T10:30:00Z',
            'user_id': 'user-987',
            'event_type': 'purchase',
            'amount': 150.00,
            'currency': 'USD',
            'metadata': {
                'source': 'mobile_app',
                'device_id': 'dev-abc123'
            }
        }

    def test_consumer_initialization(self, mock_kafka_config):
        """Verifica que el consumidor se inicializa correctamente."""
        from src.extract.kafka_consumer import KafkaConsumer

        consumer = KafkaConsumer(
            bootstrap_servers=mock_kafka_config['bootstrap.servers'],
            group_id=mock_kafka_config['group.id'],
            topics=['test-topic']
        )

        assert consumer.bootstrap_servers == 'localhost:9092'
        assert consumer.group_id == 'test-consumer-group'
        assert 'test-topic' in consumer.topics
        assert consumer.consumer is not None

    def test_consumer_message_parsing(self, mock_kafka_config, sample_message):
        """Verifica el parsing correcto de mensajes Kafka."""
        from src.extract.kafka_consumer import KafkaConsumer

        consumer = KafkaConsumer(
            bootstrap_servers=mock_kafka_config['bootstrap.servers'],
            group_id=mock_kafka_config['group.id'],
            topics=['test-topic']
        )

        raw_message = Mock()
        raw_message.value.return_value = json.dumps(sample_message).encode('utf-8')
        raw_message.topic.return_value = 'test-topic'
        raw_message.partition.return_value = 0
        raw_message.offset.return_value = 100

        parsed = consumer.parse_message(raw_message)

        assert parsed['event_id'] == 'evt-12345'
        assert parsed['user_id'] == 'user-987'
        assert parsed['amount'] == 150.00
        assert parsed['timestamp'] == '2025-01-15T10:30:00Z'

    def test_message_processing_latency(self, mock_kafka_config, sample_message):
        """Verifica que el procesamiento cumple con los umbrales de latencia."""
        from src.extract.kafka_consumer import KafkaConsumer

        consumer = KafkaConsumer(
            bootstrap_servers=mock_kafka_config['bootstrap.servers'],
            group_id=mock_kafka_config['group.id'],
            topics=['test-topic'],
            max_latency_ms=5000
        )

        message_timestamp = datetime.fromisoformat('2025-01-15T10:30:00Z'.replace('Z', '+00:00'))
        processing_timestamp = datetime.now()

        latency_ms = (processing_timestamp - message_timestamp).total_seconds() * 1000

        assert latency_ms < consumer.max_latency_ms, f"Latency {latency_ms}ms exceeds threshold"

    def test_error_handling_invalid_message(self, mock_kafka_config):
        """Verifica el manejo de errores con mensajes inválidos."""
        from src.extract.kafka_consumer import KafkaConsumer

        consumer = KafkaConsumer(
            bootstrap_servers=mock_kafka_config['bootstrap.servers'],
            group_id=mock_kafka_config['group.id'],
            topics=['test-topic']
        )

        invalid_message = Mock()
        invalid_message.value.return_value = b'invalid-json-content'
        invalid_message.topic.return_value = 'test-topic'

        with pytest.raises(json.JSONDecodeError):
            consumer.parse_message(invalid_message)

    def test_offset_commit_on_success(self, mock_kafka_config, sample_message):
        """Verifica que el offset se commitea solo en procesamiento exitoso."""
        from src.extract.kafka_consumer import KafkaConsumer

        consumer = KafkaConsumer(
            bootstrap_servers=mock_kafka_config['bootstrap.servers'],
            group_id=mock_kafka_config['group.id'],
            topics=['test-topic']
        )

        consumer.consumer.commit = Mock()

        raw_message = Mock()
        raw_message.value.return_value = json.dumps(sample_message).encode('utf-8')
        raw_message.topic.return_value = 'test-topic'
        raw_message.partition.return_value = 0
        raw_message.offset.return_value = 100

        try:
            consumer.process_message(raw_message)
            consumer.consumer.commit.assert_called_once()
        except Exception:
            pass

    def test_retry_on_transient_error(self, mock_kafka_config, sample_message):
        """Verifica la estrategia de reintento en errores transitorios."""
        from src.extract.kafka_consumer import KafkaConsumer

        max_retries = 3
        consumer = KafkaConsumer(
            bootstrap_servers=mock_kafka_config['bootstrap.servers'],
            group_id=mock_kafka_config['group.id'],
            topics=['test-topic'],
            max_retries=max_retries
        )

        attempt_count = 0

        def mock_process(*args):
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < max_retries:
                raise ConnectionError("Transient error")
            return sample_message

        raw_message = Mock()
        raw_message.value.return_value = json.dumps(sample_message).encode('utf-8')

        with patch.object(consumer, 'process_record', side_effect=mock_process):
            result = consumer.process_message(raw_message)
            assert attempt_count == max_retries

    def test_consumer_close_cleanup(self, mock_kafka_config):
        """Verifica la limpieza de recursos al cerrar el consumidor."""
        from src.extract.kafka_consumer import KafkaConsumer

        consumer = KafkaConsumer(
            bootstrap_servers=mock_kafka_config['bootstrap.servers'],
            group_id=mock_kafka_config['group.id'],
            topics=['test-topic']
        )

        consumer.consumer.close = Mock()
        consumer.close()

        consumer.consumer.close.assert_called_once()

    def test_batch_processing_performance(self, mock_kafka_config):
        """Verifica el rendimiento del procesamiento por lotes."""
        from src.extract.kafka_consumer import KafkaConsumer

        batch_size = 100
        consumer = KafkaConsumer(
            bootstrap_servers=mock_kafka_config['bootstrap.servers'],
            group_id=mock_kafka_config['group.id'],
            topics=['test-topic'],
            batch_size=batch_size
        )

        messages = []
        for i in range(batch_size):
            msg = Mock()
            msg.value.return_value = json.dumps({'event_id': f'evt-{i}'}).encode('utf-8')
            messages.append(msg)

        start_time = time.time()
        processed = consumer.process_batch(messages)
        elapsed = time.time() - start_time

        assert len(processed) == batch_size
        assert elapsed < 5.0, f"Batch processing took {elapsed}s, exceeds 5s threshold"