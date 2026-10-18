from unittest import IsolatedAsyncioTestCase

from aerospike_native import ResultCode
from aerospike_native.exceptions import ServerError
from ddutils.feature_flags import FeatureFlagRecord, FeatureFlags, FeatureFlagStorageError

from ddaerospike.feature_flags import LIST_LIMIT, AerospikeFeatureFlagStorage, FeatureFlagData
from ddaerospike.manager import AerospikeManager
from tests.fakes import ClientSetup

ON = FeatureFlagRecord(enabled=True, proportion=1.0, payload={'ttl': 10})


class TestAerospikeFeatureFlagStorage(IsolatedAsyncioTestCase):
    def setUp(self):
        self.setup = ClientSetup()
        setup = self.setup

        class FeatureFlagManager(AerospikeManager[FeatureFlagData]):
            client_factory = setup.factory
            namespace = 'ns'
            set_name = 'feature_flag'

        self.manager = FeatureFlagManager()
        self.storage = AerospikeFeatureFlagStorage(self.manager)
        self.client = setup.client

    async def test_put_get_delete(self):
        # Act
        await self.storage.put('new_checkout', ON)
        record = await self.storage.get('new_checkout')
        deleted = await self.storage.delete('new_checkout')
        deleted_again = await self.storage.delete('new_checkout')

        # Assert
        self.assertEqual(record, ON)
        self.assertTrue(deleted)
        self.assertFalse(deleted_again)
        self.assertIsNone(await self.storage.get('new_checkout'))

    async def test_put_overwrites(self):
        # Arrange
        await self.storage.put('flag', ON)

        # Act
        await self.storage.put('flag', FeatureFlagRecord())

        # Assert
        self.assertEqual(await self.storage.get('flag'), FeatureFlagRecord())

    async def test_get_map_uses_the_primary_keys(self):
        # Arrange
        await self.storage.put('on', ON)
        await self.storage.put('off', FeatureFlagRecord())

        # Act
        records = await self.storage.get_map()

        # Assert
        self.assertEqual(records, {'on': ON, 'off': FeatureFlagRecord()})

    async def test_malformed_record_reads_as_the_defaults(self):
        # Arrange: a proportion outside 0..1 does not validate
        self.client.store[('ns', 'feature_flag', 'broken')] = {'enabled': True, 'proportion': 7.0, 'payload': {}}

        # Act
        with self.assertLogs('ddaerospike.feature_flags', level='ERROR'):
            record = await self.storage.get('broken')
        with self.assertLogs('ddaerospike.feature_flags', level='ERROR'), self.assertRaises(FeatureFlagStorageError):
            await self.storage.get_map()

        # Assert
        self.assertEqual(record, FeatureFlagRecord())

    async def test_server_errors_become_storage_errors(self):
        # Arrange
        async def failing_get(*_args, **_kwargs):
            raise ServerError('down', ResultCode.TIMEOUT)

        self.client.get = failing_get  # type: ignore[method-assign]

        # Act & Assert
        with self.assertRaises(FeatureFlagStorageError) as ctx:
            await self.storage.get('on')
        self.assertIsInstance(ctx.exception.__cause__, ServerError)

    async def test_full_list_is_reported(self):
        # Arrange
        for i in range(LIST_LIMIT):
            self.client.store[('ns', 'feature_flag', f'flag-{i}')] = {'enabled': True, 'proportion': 1.0, 'payload': {}}

        # Act
        with self.assertLogs('ddaerospike.feature_flags', level='ERROR') as logs:
            records = await self.storage.get_map()

        # Assert
        self.assertEqual(len(records), LIST_LIMIT)
        self.assertIn('more flags', str(logs.records[0].msg))

    async def test_rename_is_put_then_delete(self):
        # Arrange
        await self.storage.put('old', ON)

        # Act
        await self.storage.rename('old', 'new', FeatureFlagRecord(enabled=True, proportion=0.5))

        # Assert
        self.assertEqual(await self.storage.get_map(), {'new': FeatureFlagRecord(enabled=True, proportion=0.5)})
        self.assertNotIn(('ns', 'feature_flag', 'old'), self.client.store)

    async def test_reads_through_feature_flags(self):
        # Arrange
        await self.storage.put('on', ON)
        flags = FeatureFlags(self.storage)

        # Act & Assert
        self.assertTrue(await flags.is_enabled('on'))
        self.assertEqual(await flags.get_payload('on'), {'ttl': 10})
