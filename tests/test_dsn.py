from unittest import TestCase

from pydantic import BaseModel, ValidationError

from ddaerospike.dsn import AerospikeDsn


class Settings(BaseModel):
    aerospike_url: AerospikeDsn


class TestAerospikeDsn(TestCase):
    def test_single_host_default_port_and_namespace(self):
        # Act
        settings = Settings(aerospike_url='aerospike://aerospike/mail')  # type: ignore[arg-type]

        # Assert
        self.assertEqual(settings.aerospike_url.seeds, 'aerospike:3000')
        self.assertEqual(settings.aerospike_url.namespace, 'mail')

    def test_several_seeds_with_and_without_ports(self):
        # Act
        settings = Settings(aerospike_url='aerospike://a-0:3000,a-1,a-2:3100/mail')  # type: ignore[arg-type]

        # Assert
        self.assertEqual(settings.aerospike_url.seeds, 'a-0:3000,a-1:3000,a-2:3100')

    def test_namespace_is_required(self):
        # Act & Assert
        with self.assertRaisesRegex(ValidationError, 'namespace'):
            Settings(aerospike_url='aerospike://aerospike:3000')  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValidationError, 'namespace'):
            Settings(aerospike_url='aerospike://aerospike:3000/')  # type: ignore[arg-type]

    def test_other_schemes_are_rejected(self):
        # Act & Assert
        with self.assertRaises(ValidationError):
            Settings(aerospike_url='redis://aerospike:3000/mail')  # type: ignore[arg-type]

    def test_json_schema_is_a_string(self):
        # Act & Assert
        self.assertEqual(Settings.model_json_schema()['properties']['aerospike_url']['type'], 'string')
