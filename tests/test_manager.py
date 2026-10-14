from datetime import UTC, datetime
from unittest import IsolatedAsyncioTestCase

from aerospike_native import RecordExistsAction
from pydantic import BaseModel

from ddaerospike.data import AerospikeData, AerospikeTimestamp
from ddaerospike.filters import AerospikeFilterCollection, AerospikeFilterOperator
from ddaerospike.manager import AerospikeManager
from tests.fakes import ClientSetup

MOMENT = datetime(2026, 10, 14, tzinfo=UTC)


class Address(BaseModel):
    city: str
    zip_code: str | None = None


class ProfileData(AerospikeData[dict]):
    name: str
    age: int | None = None
    created_at: AerospikeTimestamp = MOMENT
    address: Address | None = None


class TestSubclassing(IsolatedAsyncioTestCase):
    def test_record_type_and_wiring_are_required(self):
        # Arrange
        setup = ClientSetup()

        # Act & Assert
        with self.assertRaisesRegex(TypeError, 'must specify the record type'):

            class Untyped(AerospikeManager):  # type: ignore[type-arg]
                client_factory = setup.factory
                namespace = 'ns'
                set_name = 'profile'

        with self.assertRaisesRegex(AttributeError, 'client_factory.*must be set'):

            class NoFactory(AerospikeManager[ProfileData]):
                namespace = 'ns'
                set_name = 'profile'

        with self.assertRaisesRegex(TypeError, 'client_factory.*must be ConnectionManagerFactory'):

            class RegistryInstead(AerospikeManager[ProfileData]):
                client_factory = setup.client  # type: ignore[assignment]
                namespace = 'ns'
                set_name = 'profile'

        with self.assertRaisesRegex(AttributeError, 'set_name.*must be set'):

            class NoSet(AerospikeManager[ProfileData]):
                client_factory = setup.factory
                namespace = 'ns'


class TestAerospikeManager(IsolatedAsyncioTestCase):
    def setUp(self):
        self.setup = ClientSetup()
        setup = self.setup

        class ProfileManager(AerospikeManager[ProfileData]):
            client_factory = setup.factory
            namespace = 'ns'
            set_name = 'profile'
            ttl_seconds = 60

        self.manager = ProfileManager()
        self.client = setup.client

    async def test_put_and_get_round_trip(self):
        # Act
        created = await self.manager.put(1, ProfileData(name='a', age=3, address=Address(city='x')))
        data = await self.manager.get(1)

        # Assert
        self.assertTrue(created)
        self.assertEqual(
            self.client.store[('ns', 'profile', '1')],
            {'name': 'a', 'age': 3, 'created_at': 1791936000000000, 'address': {'city': 'x'}},
        )
        assert data is not None
        self.assertEqual(data, ProfileData(name='a', age=3, address=Address(city='x')))
        self.assertEqual(data.created_at, MOMENT)

    async def test_write_policy_carries_ttl_and_send_key(self):
        # Act
        await self.manager.put(1, ProfileData(name='a'))

        # Assert
        policy = self.client.policies[-1]
        self.assertTrue(policy.send_key)
        self.assertEqual(policy.max_retries, 0)
        self.assertIsNotNone(policy.expiration)

    async def test_none_bins_are_removed_and_nested_none_left_out(self):
        # Arrange
        await self.manager.put(1, ProfileData(name='a', age=3, address=Address(city='x', zip_code=None)))

        # Act
        await self.manager.put(1, ProfileData(name='a', age=None, address=None))

        # Assert
        self.assertEqual(self.client.store[('ns', 'profile', '1')], {'name': 'a', 'created_at': 1791936000000000})

    async def test_if_absent_is_create_only(self):
        # Arrange
        await self.manager.put(1, ProfileData(name='a'))

        # Act
        second = await self.manager.put(1, ProfileData(name='b'), if_absent=True)

        # Assert
        self.assertFalse(second)
        self.assertEqual(self.client.policies[-1].record_exists_action, RecordExistsAction.CREATE_ONLY)
        self.assertEqual(self.client.store[('ns', 'profile', '1')]['name'], 'a')

    async def test_get_missing_and_unknown_bins(self):
        # Arrange
        self.client.store[('ns', 'profile', '9')] = {'other': 1}

        # Act & Assert
        self.assertIsNone(await self.manager.get(404))
        self.assertIsNone(await self.manager.get(9))

    async def test_get_map_and_delete(self):
        # Arrange
        await self.manager.put(1, ProfileData(name='a'))
        await self.manager.put(2, ProfileData(name='b'))

        # Act
        found = await self.manager.get_map([1, 2, 3])
        deleted = await self.manager.delete(1)
        deleted_again = await self.manager.delete(1)

        # Assert
        self.assertEqual(sorted(found), ['1', '2'])
        self.assertEqual(found['2'].name, 'b')
        self.assertEqual(await self.manager.get_map([]), {})
        self.assertTrue(deleted)
        self.assertFalse(deleted_again)

    async def test_get_list_and_find_one_with_filters(self):
        # Arrange
        await self.manager.put(1, ProfileData(name='a', age=20))
        await self.manager.put(2, ProfileData(name='b', age=30))
        filters = AerospikeFilterCollection(ProfileData).add_filter('age', AerospikeFilterOperator.GTE, 18)

        # Act
        items = await self.manager.get_list(filters, limit=1)
        one = await self.manager.find_one(filters)

        # Assert: the fake does not evaluate expressions, it checks they were handed to the query
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].pk, '1')
        self.assertEqual(items[0].data.name, 'a')
        self.assertIsNotNone(one)
        self.assertIsNotNone(self.client.query_calls[-1]['policy'].filter_expression)
        self.assertTrue(self.client.last_recordset.closed)

    async def test_get_list_rejects_filters_built_for_another_model(self):
        # Arrange
        class OtherData(AerospikeData[dict]):
            age: int = 0

        filters = AerospikeFilterCollection(OtherData).add_filter('age', AerospikeFilterOperator.GT, 1)

        # Act & Assert
        with self.assertRaisesRegex(ValueError, 'filters are for `OtherData`'):
            await self.manager.get_list(filters)  # type: ignore[invalid-argument-type]
        self.assertEqual(self.client.query_calls, [])

    async def test_filter_bins_hook_limits_what_is_written(self):
        # Arrange
        class PartialProfileData(ProfileData):
            def filter_bins(self, bins):
                return {name: kind for name, kind in bins.items() if name != 'age'}

        class PartialManager(AerospikeManager[PartialProfileData]):
            client_factory = self.setup.factory
            namespace = 'ns'
            set_name = 'profile'

        # Act
        await PartialManager().put(1, PartialProfileData(name='a', age=3))

        # Assert
        self.assertNotIn('age', self.client.store[('ns', 'profile', '1')])
