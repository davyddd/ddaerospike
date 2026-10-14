from datetime import UTC, date, datetime, timedelta, timezone
from unittest import TestCase

from pydantic import BaseModel, RootModel, ValidationError

from ddaerospike.data import AerospikeBinType, AerospikeData, AerospikeTimestamp, timestamp_to_bin

MOMENT = datetime(2026, 10, 14, 12, 30, 15, 123456, tzinfo=UTC)
MOMENT_BIN = 1791981015123456


class Address(BaseModel):
    city: str
    zip_code: str | None = None


class Tags(RootModel[list[str]]):
    pass


class ProfileData(AerospikeData[dict]):
    name: str
    age: int | None = None
    created_at: AerospikeTimestamp
    seen_at: AerospikeTimestamp | None = None
    address: Address | None = None
    tags: Tags | None = None


class TestTimestamps(TestCase):
    def test_bin_value_is_utc_microseconds(self):
        # Act & Assert
        self.assertEqual(timestamp_to_bin(MOMENT), MOMENT_BIN)
        self.assertEqual(timestamp_to_bin(MOMENT.astimezone(timezone(timedelta(hours=3)))), MOMENT_BIN)

    def test_naive_datetime_is_rejected(self):
        # Act & Assert
        with self.assertRaises(ValueError):
            timestamp_to_bin(MOMENT.replace(tzinfo=None))
        with self.assertRaises(ValidationError):
            ProfileData(name='a', created_at=MOMENT.replace(tzinfo=None))

    def test_round_trip_through_bins(self):
        # Arrange
        data = ProfileData(name='a', created_at=MOMENT.astimezone(timezone(timedelta(hours=3))))

        # Act
        bins = data.model_dump(mode='json')
        back = ProfileData.factory(bins)

        # Assert
        self.assertEqual(bins['created_at'], MOMENT_BIN)
        self.assertEqual(back.created_at, MOMENT)
        self.assertEqual(back.created_at.tzinfo, UTC)


class TestAerospikeData(TestCase):
    def test_bins_are_classified(self):
        # Act & Assert
        self.assertEqual(
            ProfileData.get_bins(),
            {
                'name': AerospikeBinType.SIMPLE,
                'age': AerospikeBinType.SIMPLE,
                'created_at': AerospikeBinType.SIMPLE,
                'seen_at': AerospikeBinType.SIMPLE,
                'address': AerospikeBinType.COMPOSITE,
                'tags': AerospikeBinType.COMPOSITE,
            },
        )

    def test_long_bin_names_are_rejected_at_class_definition(self):
        # Act & Assert
        with self.assertRaisesRegex(TypeError, '15 characters'):

            class Broken(AerospikeData[dict]):
                a_very_long_bin_name: int

    def test_bare_datetime_bins_are_rejected_at_class_definition(self):
        # Act & Assert
        for annotation in (datetime, date, 'datetime | None'):
            with self.subTest(annotation=annotation), self.assertRaisesRegex(TypeError, 'AerospikeTimestamp'):
                type_hint = eval(annotation) if isinstance(annotation, str) else annotation  # noqa: S307

                class Broken(AerospikeData[dict]):
                    __annotations__ = {'when': type_hint}

    def test_factory_builds_from_bins(self):
        # Act
        data = ProfileData.factory({'name': 'a', 'created_at': MOMENT_BIN, 'address': {'city': 'x'}})

        # Assert
        self.assertEqual(data.name, 'a')
        self.assertEqual(data.address, Address(city='x'))

    def test_entity_mapping_is_left_to_subclasses(self):
        # Act & Assert
        with self.assertRaises(NotImplementedError):
            ProfileData(name='a', created_at=MOMENT).to_entity()
        with self.assertRaises(NotImplementedError):
            ProfileData.from_entity({})
