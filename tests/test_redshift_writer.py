import pytest
from unittest.mock import Mock, patch, MagicMock, call
from datetime import datetime
import psycopg2
import json


class TestRedshiftWriter:
    """Pruebas unitarias para el escritor de Redshift del pipeline ETL."""

    @pytest.fixture
    def redshift_config(self):
        """Configuración de Redshift para pruebas."""
        return {
            'host': 'test-cluster.xxxx.us-east-1.redshift.amazonaws.com',
            'port': 5439,
            'database': 'analytics',
            'username': 'admin',
            'password': 'test_password',
            'schema': 'public'
        }

    @pytest.fixture
    def sample_records(self):
        """Registros de ejemplo para pruebas."""
        return [
            {
                'event_id': 'evt-12345',
                'timestamp': '2025-01-15T10:30:00Z',
                'user_id': 'user-987',
                'event_type': 'purchase',
                'amount': 150.00,
                'currency': 'USD'
            },
            {
                'event_id': 'evt-12346',
                'timestamp': '2025-01-15T10:31:00Z',
                'user_id': 'user-988',
                'event_type': 'view',
                'amount': None,
                'currency': None
            }
        ]

    @pytest.fixture
    def mock_connection(self):
        """Mock de conexión a Redshift."""
        with patch('psycopg2.connect') as mock_connect:
            conn = MagicMock()
            mock_connect.return_value = conn
            yield conn

    def test_writer_initialization(self, redshift_config):
        """Verifica que el escritor se inicializa correctamente."""
        from src.load.redshift_writer import RedshiftWriter

        writer = RedshiftWriter(
            host=redshift_config['host'],
            port=redshift_config['port'],
            database=redshift_config['database'],
            username=redshift_config['username'],
            password=redshift_config['password'],
            schema=redshift_config['schema']
        )

        assert writer.host == redshift_config['host']
        assert writer.port == redshift_config['port']
        assert writer.database == redshift_config['database']
        assert writer.schema == redshift_config['schema']

    def test_successful_write(self, redshift_config, sample_records, mock_connection):
        """Verifica escritura exitosa de registros."""
        from src.load.redshift_writer import RedshiftWriter

        writer = RedshiftWriter(
            host=redshift_config['host'],
            port=redshift_config['port'],
            database=redshift_config['database'],
            username=redshift_config['username'],
            password=redshift_config['password'],
            schema=redshift_config['schema']
        )

        cursor = MagicMock()
        mock_connection.cursor.return_value = cursor

        result = writer.write_records('events', sample_records)

        assert result.success is True
        assert result.records_written == len(sample_records)
        cursor.execute.assert_called()
        mock_connection.commit.assert_called_once()

    def test_transaction_rollback_on_error(self, redshift_config, sample_records, mock_connection):
        """Verifica rollback de transacción en caso de error."""
        from src.load.redshift_writer import RedshiftWriter

        writer = RedshiftWriter(
            host=redshift_config['host'],
            port=redshift_config['port'],
            database=redshift_config['database'],
            username=redshift_config['username'],
            password=redshift_config['password'],
            schema=redshift_config['schema']
        )

        cursor = MagicMock()
        cursor.execute.side_effect = Exception("Database error")
        mock_connection.cursor.return_value = cursor

        result = writer.write_records('events', sample_records)

        assert result.success is False
        mock_connection.rollback.assert_called_once()

    def test_idempotency_check(self, redshift_config, sample_records, mock_connection):
        """Verifica la verificación de idempotencia antes de escribir."""
        from src.load.redshift_writer import RedshiftWriter

        writer = RedshiftWriter(
            host=redshift_config['host'],
            port=redshift_config['port'],
            database=redshift_config['database'],
            username=redshift_config['username'],
            password=redshift_config['password'],
            schema=redshift_config['schema']
        )

        cursor = MagicMock()
        cursor.fetchone.return_value = (1,)
        mock_connection.cursor.return_value = cursor

        result = writer.write_records('events', sample_records, check_idempotency=True)

        assert result.already_exists is True
        assert result.records_written == 0

    def test_batch_size_handling(self, redshift_config, sample_records, mock_connection):
        """Verifica el manejo correcto del tamaño de lote."""
        from src.load.redshift_writer import RedshiftWriter

        writer = RedshiftWriter(
            host=redshift_config['host'],
            port=redshift_config['port'],
            database=redshift_config['database'],
            username=redshift_config['username'],
            password=redshift_config['password'],
            schema=redshift_config['schema'],
            batch_size=1
        )

        cursor = MagicMock()
        mock_connection.cursor.return_value = cursor

        large_batch = sample_records * 50
        result = writer.write_records('events', large_batch)

        assert cursor.execute.call_count > 1

    def test_retry_on_connection_failure(self, redshift_config, sample_records):
        """Verifica la estrategia de reintento en fallos de conexión."""
        from src.load.redshift_writer import RedshiftWriter

        with patch('psycopg2.connect', side_effect=[Exception("Connection failed"), Exception("Connection failed"), MagicMock()]) as mock_connect:
            writer = RedshiftWriter(
                host=redshift_config['host'],
                port=redshift_config['port'],
                database=redshift_config['database'],
                username=redshift_config['username'],
                password=redshift_config['password'],
                schema=redshift_config['schema'],
                max_retries=3
            )

            cursor = MagicMock()
            mock_connect.return_value.cursor.return_value = cursor

            result = writer.write_records('events', sample_records)

            assert mock_connect.call_count == 3

    def test_consistency_verification(self, redshift_config, sample_records, mock_connection):
        """Verifica la consistencia de datos después de la escritura."""
        from src.load.redshift_writer import RedshiftWriter

        writer = RedshiftWriter(
            host=redshift_config['host'],
            port=redshift_config['port'],
            database=redshift_config['database'],
            username=redshift_config['username'],
            password=redshift_config['password'],
            schema=redshift_config['schema']
        )

        cursor = MagicMock()
        mock_connection.cursor.return_value = cursor
        cursor.fetchone.return_value = (len(sample_records),)

        writer.write_records('events', sample_records)

        verify_cursor = MagicMock()
        verify_cursor.fetchone.return_value = (len(sample_records),)
        mock_connection.cursor.return_value = verify_cursor

        count = writer.verify_record_count('events')

        assert count == len(sample_records)

    def test_partition_key_extraction(self, redshift_config, sample_records, mock_connection):
        """Verifica la extracción de clave de partición para Redshift."""
        from src.load.redshift_writer import RedshiftWriter

        writer = RedshiftWriter(
            host=redshift_config['host'],
            port=redshift_config['port'],
            database=redshift_config['database'],
            username=redshift_config['username'],
            password=redshift_config['password'],
            schema=redshift_config['schema'],
            partition_column='timestamp'
        )

        cursor = MagicMock()
        mock_connection.cursor.return_value = cursor

        writer.write_records('events', sample_records)

        calls = cursor.execute.call_args_list
        assert any('2025-01-15' in str(call) for call in calls)

    def test_dead_letter_queue_on_failure(self, redshift_config, sample_records, mock_connection):
        """Verifica el manejo de cola de mensajes fallidos."""
        from src.load.redshift_writer import RedshiftWriter

        writer = RedshiftWriter(
            host=redshift_config['host'],
            port=redshift_config['port'],
            database=redshift_config['database'],
            username=redshift_config['username'],
            password=redshift_config['password'],
            schema=redshift_config['schema'],
            dead_letter_queue='s3://bucket/dead-letter/'
        )

        cursor = MagicMock()
        cursor.execute.side_effect = Exception("Validation error")
        mock_connection.cursor.return_value = cursor

        with patch('src.load.redshift_writer.s3_writer') as mock_s3:
            mock_s3.write_batch.return_value = True

            result = writer.write_records('events', sample_records, use_dead_letter=True)

            assert result.success is False
            assert result.sent_to_dlq == len(sample_records)

    def test_connection_close_cleanup(self, redshift_config, sample_records, mock_connection):
        """Verifica la limpieza de recursos al cerrar conexión."""
        from src.load.redshift_writer import RedshiftWriter

        writer = RedshiftWriter(
            host=redshift_config['host'],
            port=redshift_config['port'],
            database=redshift_config['database'],
            username=redshift_config['username'],
            password=redshift_config['password'],
            schema=redshift_config['schema']
        )

        writer.close()

        mock_connection.close.assert_called_once()