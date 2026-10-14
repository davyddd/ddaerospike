from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum
from functools import cache
from types import NoneType, UnionType
from typing import Annotated, Any, Generic, Literal, Self, Union, get_args, get_origin
from uuid import UUID

from aerospike_native import Filter, FilterExpression
from dddesign.structure.domains.constants import BaseEnum
from dddesign.structure.domains.value_objects import ValueObject
from ddutils.convertors import convert_to_base_value
from ddutils.value_helpers import is_complex_sequence_value
from pydantic import BaseModel, Field, field_validator, model_validator

from ddaerospike.data import AerospikeBinType, AerospikeData, AerospikeDataT, timestamp_to_bin

# The scalar types a filter value is read as; a UUID, a datetime, a date and a Decimal are converted, the rest rejected
Scalar = bool | int | float | str

# Expressions are typed: the bin and the value are read as one of these
_BIN_EXPRESSIONS: dict[type, Callable[[str], FilterExpression]] = {
    bool: FilterExpression.bool_bin,
    int: FilterExpression.int_bin,
    float: FilterExpression.float_bin,
    str: FilterExpression.string_bin,
}
_VALUE_EXPRESSIONS: dict[type, Callable[[Any], FilterExpression]] = {
    bool: FilterExpression.bool_val,
    int: FilterExpression.int_val,
    float: FilterExpression.float_val,
    str: FilterExpression.string_val,
}


class AerospikeFilterOperator(str, BaseEnum):
    EQUAL = 'equal'
    NOT_EQUAL = 'not_equal'
    GT = 'gt'
    GTE = 'gte'
    LT = 'lt'
    LTE = 'lte'
    BETWEEN = 'between'  # value: (low, high), inclusive
    IS_NULL = 'is_null'  # no value; "the bin is missing", the manager writes `None` as no bin
    IS_NOT_NULL = 'is_not_null'  # no value


_SCALARS = {bool, int, float, str}
_NUMBERS = {int, float}
# Which value types each operator takes: numbers compare by order, strings and bools by equality only
_OPERATOR_TYPES: dict[AerospikeFilterOperator, set[type]] = {
    AerospikeFilterOperator.EQUAL: _SCALARS,
    AerospikeFilterOperator.NOT_EQUAL: _SCALARS,
    AerospikeFilterOperator.GT: _NUMBERS,
    AerospikeFilterOperator.GTE: _NUMBERS,
    AerospikeFilterOperator.LT: _NUMBERS,
    AerospikeFilterOperator.LTE: _NUMBERS,
    AerospikeFilterOperator.BETWEEN: _NUMBERS,
}
# What a secondary index can answer: equality on int/str, a range on int. Anything else (a float, a bool) makes
# the client panic instead of raising, so it is refused here.
_INDEX_TYPES: dict[AerospikeFilterOperator, set[type]] = {
    AerospikeFilterOperator.EQUAL: {int, str},
    AerospikeFilterOperator.BETWEEN: {int},
}
_NULL_OPERATORS = {AerospikeFilterOperator.IS_NULL, AerospikeFilterOperator.IS_NOT_NULL}
_BETWEEN_SIZE = 2


def normalize_scalar(value: Any) -> Scalar:
    """One filter value as the server compares it: a bool, an int, a float or a str."""
    try:
        # The exact base type first: a subclass (`class SomeStr(str)`, `class ProfileId(UUID)`) or an `Enum` member
        # would not look up an expression
        base = convert_to_base_value(value)
    except TypeError as error:
        raise ValueError(f'Unsupported value type `{type(value).__name__}`') from error

    if isinstance(base, UUID):
        return str(base)
    elif isinstance(base, datetime):
        # `AerospikeTimestamp` bins: integer microseconds since the epoch, UTC
        return timestamp_to_bin(base)
    elif isinstance(base, date):
        return timestamp_to_bin(datetime(base.year, base.month, base.day, tzinfo=UTC))
    elif isinstance(base, Decimal):
        # Aerospike has no decimal type; a `Decimal` compares against a float bin
        return float(base)
    elif type(base) in _SCALARS:  # the exact type after `convert_to_base_value`
        return base
    else:
        raise ValueError(f'Unsupported value type `{type(value).__name__}`')


# The scalar a bin holds, by the base class of its annotation; ordered, the first match wins (`bool` before `int`,
# `datetime` is a `date`). `UUID` is stored as a string, dates as `AerospikeTimestamp` integers, `Decimal` as a float.
_BIN_SCALARS: dict[type, type] = {bool: bool, int: int, float: float, str: str, UUID: str, date: int, Decimal: float}


def bin_scalar_type(annotation: Any) -> type | None:
    """
    The scalar a bin declared with this annotation holds, as a filter value compares against it (`_BIN_SCALARS`;
    an `Enum` by its members' values, `Optional`, `Annotated` and `Literal` unwrapped). `None` for a bin the scalar
    operators cannot read: a list, a dict, a union of several types.
    """
    annotation = _unwrap_annotation(annotation)
    if annotation is None:
        return None
    if get_origin(annotation) is Literal:
        return _single({bin_scalar_type(type(member)) for member in get_args(annotation)})
    if issubclass(annotation, Enum):
        return _single({bin_scalar_type(type(member.value)) for member in annotation})
    return next((scalar for base, scalar in _BIN_SCALARS.items() if issubclass(annotation, base)), None)


def _unwrap_annotation(annotation: Any) -> Any:
    """A plain class or a `Literal` out of `Annotated[...]` / `X | None`; `None` for a generic or a union of types."""
    origin = get_origin(annotation)
    if origin is Annotated:
        return _unwrap_annotation(get_args(annotation)[0])
    if origin is UnionType or origin is Union:
        members = [member for member in get_args(annotation) if member is not NoneType]
        return _unwrap_annotation(members[0]) if len(members) == 1 else None
    if origin is Literal or (origin is None and isinstance(annotation, type)):
        return annotation
    return None  # `list[str]`, `dict[str, int]`


def _single(types: set[type | None]) -> type | None:
    return next(iter(types)) if len(types) == 1 else None


@cache
def model_bin_scalar_types(model: type[AerospikeData]) -> dict[str, type | None]:
    """Per bin of the model: the scalar it holds, `None` for a composite bin or one the scalar operators cannot read."""
    bins = model.get_bins()
    return {
        field: bin_scalar_type(info.annotation) if bins[field] == AerospikeBinType.SIMPLE else None
        for field, info in model.model_fields.items()
    }


class AerospikeFilter(ValueObject):
    """One condition on a bin: the value decides the type, the operator must accept that type."""

    field: str
    operator: AerospikeFilterOperator
    value: Any = None  # none for `is_null` / `is_not_null`

    @field_validator('value')
    @classmethod
    def validate_value(cls, value: Any) -> Any:
        if value is None:
            return None
        if is_complex_sequence_value(value):  # a list or a tuple, for `between`; a set has no order
            return tuple(normalize_scalar(item) for item in value)
        return normalize_scalar(value)

    @model_validator(mode='after')
    def validate_consistency(self) -> Self:
        if self.operator in _NULL_OPERATORS:
            if self.value is not None:
                raise ValueError(f'`{self.operator}` takes no value')
            return self
        if self.value is None:
            raise ValueError(f'`{self.operator}` takes a value')

        if self.operator == AerospikeFilterOperator.BETWEEN:
            self._check_between_value()
        elif is_complex_sequence_value(self.value):
            raise ValueError(f'`{self.operator}` takes a single value')

        allowed = _OPERATOR_TYPES[self.operator]
        if self.python_type not in allowed:
            names = ', '.join(sorted(t.__name__ for t in allowed))
            raise ValueError(f'`{self.operator}` takes {names}, got `{self.python_type.__name__}`')
        return self

    def _check_between_value(self) -> None:
        if not is_complex_sequence_value(self.value) or len(self.value) != _BETWEEN_SIZE:
            raise ValueError('`between` takes exactly two values: (low, high)')
        if len({type(item) for item in self.value}) > 1:
            raise ValueError('`between` takes values of one type')
        if self.value[0] > self.value[1]:
            raise ValueError('`between` takes (low, high), low first')

    @property
    def python_type(self) -> type:
        value = self.value[0] if is_complex_sequence_value(self.value) else self.value
        return type(value)

    @property
    def indexable(self) -> bool:
        return self.python_type in _INDEX_TYPES.get(self.operator, set())

    def check_indexable(self) -> None:
        if not self.indexable:
            pairs = ', '.join(f'`{op}` on {"/".join(sorted(t.__name__ for t in types))}' for op, types in _INDEX_TYPES.items())
            raise ValueError(f'An index answers {pairs}; `{self.operator}` on `{self.python_type.__name__}` is an expression')

    @property
    def index_filter(self) -> Filter:
        """The secondary-index filter: needs an index on `field`."""
        self.check_indexable()
        if self.operator == AerospikeFilterOperator.EQUAL:
            return Filter.equal(self.field, self.value)
        else:
            return Filter.range(self.field, *self.value)

    @property
    def expression(self) -> FilterExpression:  # noqa: PLR0911
        """The same condition as a filter expression: evaluated on every record the query reads, no index needed."""
        if self.operator in _NULL_OPERATORS:
            exists = FilterExpression.bin_exists(self.field)
            return FilterExpression.not_(exists) if self.operator == AerospikeFilterOperator.IS_NULL else exists

        bin_expression = _BIN_EXPRESSIONS[self.python_type](self.field)
        value_expression = _VALUE_EXPRESSIONS[self.python_type]
        match self.operator:
            case AerospikeFilterOperator.EQUAL:
                return FilterExpression.eq(bin_expression, value_expression(self.value))
            case AerospikeFilterOperator.NOT_EQUAL:
                # False for a missing bin too: "not equal or missing" is this plus `is_null`
                return FilterExpression.ne(bin_expression, value_expression(self.value))
            case AerospikeFilterOperator.GT:
                return FilterExpression.gt(bin_expression, value_expression(self.value))
            case AerospikeFilterOperator.GTE:
                return FilterExpression.ge(bin_expression, value_expression(self.value))
            case AerospikeFilterOperator.LT:
                return FilterExpression.lt(bin_expression, value_expression(self.value))
            case AerospikeFilterOperator.LTE:
                return FilterExpression.le(bin_expression, value_expression(self.value))
            case _:
                low, high = self.value
                return FilterExpression.and_(
                    [
                        FilterExpression.ge(bin_expression, value_expression(low)),
                        FilterExpression.le(bin_expression, value_expression(high)),
                    ]
                )


class AerospikeFilterCollection(BaseModel, Generic[AerospikeDataT]):
    """
    Conditions of one query on a set, ANDed: at most one secondary-index filter (a query runs on one index) and any
    number of filter expressions, applied to every record the index (or the whole set) yields. Bound to the record
    model of the set: `add_filter` checks the bin and matches the value against the bin's scalar, and the manager
    refuses a collection built for another model.
    """

    model: type[AerospikeDataT]
    index_filter: AerospikeFilter | None = None
    filters: list[AerospikeFilter] = Field(default_factory=list)

    def __init__(self, model: type[AerospikeDataT], **data: Any) -> None:
        super().__init__(model=model, **data)

    @property
    def compiled_index_filter(self) -> Filter | None:
        return self.index_filter.index_filter if self.index_filter is not None else None

    @property
    def compiled_expression(self) -> FilterExpression | None:
        expressions = [condition.expression for condition in self.filters]
        if not expressions:
            return None
        if len(expressions) == 1:
            return expressions[0]
        return FilterExpression.and_(expressions)

    def add_filter(self, field: str, operator: AerospikeFilterOperator, value: Any = None, use_index: bool = False) -> Self:
        condition = self._match_bin(AerospikeFilter(field=field, operator=operator, value=value))
        if use_index:
            if self.index_filter is not None:
                raise ValueError('A query takes one secondary-index filter; add the rest as expressions')
            condition.check_indexable()
            self.index_filter = condition
        else:
            self.filters.append(condition)

        return self

    def _match_bin(self, condition: AerospikeFilter) -> AerospikeFilter:
        """The condition names a bin of the model and its value is of the bin's scalar; an int widens to a float bin."""
        scalar_types = model_bin_scalar_types(self.model)
        if condition.field not in scalar_types:
            raise ValueError(f'`{self.model.__name__}` has no bin `{condition.field}`')
        if condition.operator in _NULL_OPERATORS:
            return condition

        expected = scalar_types[condition.field]
        if expected is None:
            raise ValueError(f'Bin `{condition.field}` of `{self.model.__name__}` takes only `is_null` / `is_not_null`')
        if condition.python_type is expected:
            return condition
        if expected is float and condition.python_type is int:
            value = condition.value
            widened = tuple(float(item) for item in value) if is_complex_sequence_value(value) else float(value)
            return AerospikeFilter(field=condition.field, operator=condition.operator, value=widened)
        raise ValueError(
            f'Bin `{condition.field}` of `{self.model.__name__}` holds `{expected.__name__}`, '
            f'got `{condition.python_type.__name__}`'
        )


__all__ = (
    'AerospikeFilter',
    'AerospikeFilterCollection',
    'AerospikeFilterOperator',
    'Scalar',
    'bin_scalar_type',
    'model_bin_scalar_types',
    'normalize_scalar',
)
