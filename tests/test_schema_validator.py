import pytest
from unittest.mock import Mock, patch
import json
from datetime import datetime
from pydantic import ValidationError


class TestSchemaValidator:
    """Pruebas unitarias para el validador de esquemas del pipeline ETL."""

    @pytest.fixture
    def valid_event_schema(self):
        """Esquema de evento válido para pruebas."""
        return {
            'type': 'object',
            'required': ['event_id', 'timestamp', 'user_id', 'event_type'],
            'properties': {
                'event_id': {'type': 'string', 'pattern': '^evt-[0-9]+$'},
                'timestamp': {'type': 'string', 'format': 'date-time'},
                'user_id': {'type': 'string', 'pattern': '^user-[0-9]+$'},
                'event_type': {'type': 'string', 'enum': ['purchase', 'view', 'click', 'signup']},
                'amount': {'type': 'number', 'minimum': 0},
                'currency': {'type': 'string', 'enum': ['USD', 'EUR', 'GBP']},
                'metadata': {'type': 'object'}
            },
            'additionalProperties': False
        }

    @pytest.fixture
    def valid_record(self):
        """Registro válido para pruebas."""
        return {
            'event_id': 'evt-12345',
            'timestamp': '2025-01-15T10:30:00Z',
            'user_id': 'user-987',
            'event_type': 'purchase',
            'amount': 150.00,
            'currency': 'USD',
            'metadata': {'source': 'mobile_app'}
        }

    def test_validator_initialization(self, valid_event_schema):
        """Verifica que el validador se inicializa con el esquema correcto."""
        from src.transform.schema_validator import SchemaValidator

        validator = SchemaValidator(schema=valid_event_schema)

        assert validator.schema == valid_event_schema
        assert validator.quarantine_path is not None

    def test_valid_record_passes_validation(self, valid_event_schema, valid_record):
        """Verifica que un registro válido pasa la validación."""
        from src.transform.schema_validator import SchemaValidator

        validator = SchemaValidator(schema=valid_event_schema)
        result = validator.validate(valid_record)

        assert result.is_valid is True
        assert result.errors is None
        assert result.record == valid_record

    def test_missing_required_field_fails(self, valid_event_schema):
        """Verifica que falta de campo requerido falla la validación."""
        from src.transform.schema_validator import SchemaValidator

        validator = SchemaValidator(schema=valid_event_schema)
        invalid_record = {
            'event_id': 'evt-12345',
            'timestamp': '2025-01-15T10:30:00Z'
        }

        result = validator.validate(invalid_record)

        assert result.is_valid is False
        assert result.errors is not None
        assert 'user_id' in str(result.errors).lower() or 'required' in str(result.errors).lower()

    def test_invalid_field_type_fails(self, valid_event_schema, valid_record):
        """Verifica que tipo de campo inválido falla la validación."""
        from src.transform.schema_validator import SchemaValidator

        validator = SchemaValidator(schema=valid_event_schema)
        invalid_record = valid_record.copy()
        invalid_record['amount'] = 'not-a-number'

        result = validator.validate(invalid_record)

        assert result.is_valid is False
        assert result.errors is not None

    def test_invalid_enum_value_fails(self, valid_event_schema, valid_record):
        """Verifica que valor de enum inválido falla la validación."""
        from src.transform.schema_validator import SchemaValidator

        validator = SchemaValidator(schema=valid_event_schema)
        invalid_record = valid_record.copy()
        invalid_record['event_type'] = 'invalid_type'

        result = validator.validate(invalid_record)

        assert result.is_valid is False

    def test_pattern_mismatch_fails(self, valid_event_schema, valid_record):
        """Verifica que patrón no matching falla la validación."""
        from src.transform.schema_validator import SchemaValidator

        validator = SchemaValidator(schema=valid_event_schema)
        invalid_record = valid_record.copy()
        invalid_record['event_id'] = 'invalid-format-123'

        result = validator.validate(invalid_record)

        assert result.is_valid is False

    def test_additional_properties_rejected(self, valid_event_schema, valid_record):
        """Verifica que propiedades adicionales son rechazadas."""
        from src.transform.schema_validator import SchemaValidator

        validator = SchemaValidator(schema=valid_event_schema)
        invalid_record = valid_record.copy()
        invalid_record['extra_field'] = 'not-allowed'

        result = validator.validate(invalid_record)

        assert result.is_valid is False

    def test_quarantine_invalid_records(self, valid_event_schema):
        """Verifica que registros inválidos se envían a cuarentena."""
        from src.transform.schema_validator import SchemaValidator

        validator = SchemaValidator(schema=valid_event_schema)
        invalid_record = {
            'event_id': 'evt-12345',
            'timestamp': '2025-01-15T10:30:00Z'
        }

        with patch('builtins.open', create=True) as mock_open:
            mock_file = Mock()
            mock_open.return_value.__enter__.return_value = mock_file

            result = validator.validate(invalid_record, quarantine=True)

            assert result.is_valid is False
            mock_file.write.assert_called()

    def test_batch_validation(self, valid_event_schema, valid_record):
        """Verifica la validación por lotes."""
        from src.transform.schema_validator import SchemaValidator

        validator = SchemaValidator(schema=valid_event_schema)

        records = [valid_record.copy() for _ in range(10)]
        records[5]['event_type'] = 'invalid_type'

        results = validator.validate_batch(records)

        valid_count = sum(1 for r in results if r.is_valid)
        invalid_count = sum(1 for r in results if not r.is_valid)

        assert valid_count == 9
        assert invalid_count == 1

    def test_validation_performance(self, valid_event_schema, valid_record):
        """Verifica el rendimiento de la validación."""
        from src.transform.schema_validator import SchemaValidator
        import time

        validator = SchemaValidator(schema=valid_event_schema)

        records = [valid_record.copy() for _ in range(1000)]

        start_time = time.time()
        results = validator.validate_batch(records)
        elapsed = time.time() - start_time

        assert all(r.is_valid for r in results)
        assert elapsed < 2.0, f"Validation took {elapsed}s, exceeds 2s threshold"

    def test_schema_evolution_compatibility(self, valid_event_schema):
        """Verifica el manejo de evolución de esquemas."""
        from src.transform.schema_validator import SchemaValidator

        old_schema = valid_event_schema.copy()
        new_schema = valid_event_schema.copy()
        new_schema['properties']['new_field'] = {'type': 'string'}

        validator = SchemaValidator(schema=old_schema, allow_new_fields=True)

        record_with_new_field = {
            'event_id': 'evt-12345',
            'timestamp': '2025-01-15T10:30:00Z',
            'user_id': 'user-987',
            'event_type': 'purchase',
            'new_field': 'value'
        }

        result = validator.validate(record_with_new_field)

        assert result.is_valid is True