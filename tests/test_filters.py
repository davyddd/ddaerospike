from datetime import UTC, date, datetime
from decimal import Decimal
from enum import StrEnum
from unittest import TestCase
from uuid import UUID

from aerospike_native import Filter, FilterExpression
from pydantic import BaseModel

from ddaerospike.data import AerospikeData, AerospikeTimestamp
from ddaerospike.filters import (
    AerospikeFilter,
    AerospikeFilterCollection,
    AerospikeFilterOperator,
    bin_scalar_type,
    normalize_scalar,
)


class Color(StrEnum):
    RED = 'red'


class SomeStr(str):
    pass


class Address(BaseModel):
    city: str


class ProfileData(AerospikeData[dict]):
    name: str
    age: int | None = None
    score: float = 0.0
    color: Color = Color.RED
    created_at: AerospikeTimestamp | None = None
    address: Address | None = None
    tags: list[str] | None = None


class TestNormalizeScalar(TestCase):
    def test_scalars_enums_uuids_and_dates(self):
        # Act & Assert
        self.assertIs(normalize_scalar(True), True)
        self.assertEqual(normalize_scalar(3), 3)
        self.assertEqual(normalize_scalar(Color.RED), 'red')
        self.assertIs(type(normalize_scalar(SomeStr('x'))), str)
        self.assertEqual(normalize_scalar(UUID(int=1)), '00000000-0000-0000-0000-000000000001')
        self.assertEqual(normalize_scalar(datetime(1970, 1, 1, 0, 0, 1, tzinfo=UTC)), 1_000_000)
        self.assertEqual(normalize_scalar(date(1970, 1, 2)), 86_400_000_000)
        self.assertEqual(normalize_scalar(Decimal('1.5')), 1.5)

    def test_unsupported_values_are_rejected(self):
        # Act & Assert
        with self.assertRaisesRegex(ValueError, 'Unsupported'):
            normalize_scalar(object())


class TestAerospikeFilter(TestCase):
    def test_operator_must_accept_the_value_type(self):
        # Act & Assert
        AerospikeFilter(field='age', operator=AerospikeFilterOperator.GT, value=18)
        with self.assertRaisesRegex(ValueError, 'takes float, int'):
            AerospikeFilter(field='name', operator=AerospikeFilterOperator.GT, value='a')
        with self.assertRaisesRegex(ValueError, 'takes no value'):
            AerospikeFilter(field='name', operator=AerospikeFilterOperator.IS_NULL, value=1)
        with self.assertRaisesRegex(ValueError, 'takes a value'):
            AerospikeFilter(field='name', operator=AerospikeFilterOperator.EQUAL)
        with self.assertRaisesRegex(ValueError, 'single value'):
            AerospikeFilter(field='age', operator=AerospikeFilterOperator.EQUAL, value=[1, 2])

    def test_between_takes_an_ordered_pair_of_one_type(self):
        # Act & Assert
        condition = AerospikeFilter(field='age', operator=AerospikeFilterOperator.BETWEEN, value=[18, 65])
        self.assertEqual(condition.value, (18, 65))
        for bad in ([1], [1, 2, 3], [2, 1], [1, 2.5]):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                AerospikeFilter(field='age', operator=AerospikeFilterOperator.BETWEEN, value=bad)

    def test_indexable_conditions(self):
        # Act & Assert
        self.assertTrue(AerospikeFilter(field='a', operator=AerospikeFilterOperator.EQUAL, value='x').indexable)
        self.assertTrue(AerospikeFilter(field='a', operator=AerospikeFilterOperator.BETWEEN, value=[1, 2]).indexable)
        self.assertFalse(AerospikeFilter(field='a', operator=AerospikeFilterOperator.EQUAL, value=1.5).indexable)
        self.assertFalse(AerospikeFilter(field='a', operator=AerospikeFilterOperator.GT, value=1).indexable)
        with self.assertRaisesRegex(ValueError, 'is an expression'):
            AerospikeFilter(field='a', operator=AerospikeFilterOperator.GT, value=1).check_indexable()

    def test_compiles_to_index_filters_and_expressions(self):
        # Act & Assert
        self.assertIsInstance(AerospikeFilter(field='a', operator=AerospikeFilterOperator.EQUAL, value=1).index_filter, Filter)
        for operator, value in [
            (AerospikeFilterOperator.EQUAL, 'x'),
            (AerospikeFilterOperator.NOT_EQUAL, True),
            (AerospikeFilterOperator.GT, 1),
            (AerospikeFilterOperator.GTE, 1.5),
            (AerospikeFilterOperator.LT, 1),
            (AerospikeFilterOperator.LTE, 1),
            (AerospikeFilterOperator.BETWEEN, [1, 2]),
            (AerospikeFilterOperator.IS_NULL, None),
            (AerospikeFilterOperator.IS_NOT_NULL, None),
        ]:
            with self.subTest(operator=operator):
                expression = AerospikeFilter(field='a', operator=operator, value=value).expression
                self.assertIsInstance(expression, FilterExpression)


class TestBinScalarType(TestCase):
    def test_scalars_through_optional_annotated_enum_and_literal(self):
        # Act & Assert
        self.assertIs(bin_scalar_type(int), int)
        self.assertIs(bin_scalar_type(bool), bool)
        self.assertIs(bin_scalar_type(str | None), str)
        self.assertIs(bin_scalar_type(AerospikeTimestamp | None), int)
        self.assertIs(bin_scalar_type(Color), str)
        self.assertIs(bin_scalar_type(Decimal), float)
        self.assertIsNone(bin_scalar_type(list[str]))
        self.assertIsNone(bin_scalar_type(int | str))


class TestAerospikeFilterCollection(TestCase):
    def test_one_index_filter_and_any_number_of_expressions(self):
        # Act
        filters = (
            AerospikeFilterCollection(ProfileData)
            .add_filter('name', AerospikeFilterOperator.EQUAL, 'a', use_index=True)
            .add_filter('age', AerospikeFilterOperator.GTE, 18)
            .add_filter('created_at', AerospikeFilterOperator.IS_NOT_NULL)
        )

        # Assert
        self.assertIsInstance(filters.compiled_index_filter, Filter)
        self.assertIsInstance(filters.compiled_expression, FilterExpression)
        self.assertEqual([c.field for c in filters.filters], ['age', 'created_at'])
        with self.assertRaisesRegex(ValueError, 'one secondary-index filter'):
            filters.add_filter('age', AerospikeFilterOperator.EQUAL, 1, use_index=True)

    def test_no_expression_without_filters(self):
        # Act & Assert
        self.assertIsNone(AerospikeFilterCollection(ProfileData).compiled_expression)
        self.assertIsNone(AerospikeFilterCollection(ProfileData).compiled_index_filter)

    def test_values_are_matched_against_the_bin(self):
        # Act & Assert
        filters = AerospikeFilterCollection(ProfileData)
        filters.add_filter('color', AerospikeFilterOperator.EQUAL, Color.RED)
        filters.add_filter('created_at', AerospikeFilterOperator.LT, datetime(2026, 1, 1, tzinfo=UTC))
        widened = filters.add_filter('score', AerospikeFilterOperator.GT, 1).filters[-1]
        self.assertIs(widened.python_type, float)
        with self.assertRaisesRegex(ValueError, 'has no bin `salary`'):
            filters.add_filter('salary', AerospikeFilterOperator.GT, 1)
        with self.assertRaisesRegex(ValueError, 'holds `int`, got `str`'):
            filters.add_filter('age', AerospikeFilterOperator.EQUAL, 'x')
        with self.assertRaisesRegex(ValueError, 'takes only `is_null`'):
            filters.add_filter('address', AerospikeFilterOperator.EQUAL, 'x')
        with self.assertRaisesRegex(ValueError, 'takes only `is_null`'):
            filters.add_filter('tags', AerospikeFilterOperator.EQUAL, 'x')
        filters.add_filter('address', AerospikeFilterOperator.IS_NULL)
