from datetime import UTC, date, datetime, timedelta
from typing import Annotated, Any, Generic, NamedTuple, Self, TypeVar, get_origin

from dddesign.structure.domains.constants import BaseEnum
from dddesign.structure.domains.dto import DataTransferObject
from ddutils.annotation_helpers import get_annotation_without_optional, is_subclass
from pydantic import AfterValidator, BaseModel, BeforeValidator, PlainSerializer

# Server-side limit on the length of a bin name
BIN_NAME_MAX_LENGTH = 15

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MICROSECOND = timedelta(microseconds=1)


def timestamp_to_bin(value: datetime) -> int:
    """A datetime as it is stored in a bin: integer microseconds since the epoch, so it compares as a number."""
    if value.tzinfo is None:
        raise ValueError('Aerospike timestamps must be timezone-aware')
    return (value - _EPOCH) // _MICROSECOND


def _timestamp_from_bin(value: Any) -> Any:
    return _EPOCH + timedelta(microseconds=value) if isinstance(value, int) and not isinstance(value, bool) else value


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError('Aerospike timestamps must be timezone-aware')
    return value.astimezone(UTC)


_TIMESTAMP_SERIALIZER = PlainSerializer(timestamp_to_bin, return_type=int)

# The only way to declare a datetime bin: stored as integer microseconds (UTC), read back as an aware datetime,
# and the same representation `AerospikeFilter` compares against. A bare `datetime` / `date` field is rejected.
AerospikeTimestamp = Annotated[datetime, BeforeValidator(_timestamp_from_bin), AfterValidator(_as_utc), _TIMESTAMP_SERIALIZER]


def _is_composite_annotation(annotation: Any) -> bool:
    """A `BaseModel` bin (a `RootModel` is one), optional or not. A union of several types or a `Literal` is a simple bin."""
    try:
        inner = get_annotation_without_optional(annotation)
    except TypeError:
        return False
    return is_subclass(inner, BaseModel)


def _is_bare_date_annotation(annotation: Any, metadata: list[Any]) -> bool:
    """A `datetime` / `date` bin (optional or not) declared without `AerospikeTimestamp`."""
    try:
        inner = get_annotation_without_optional(annotation)
    except TypeError:
        return False
    if not is_subclass(inner, date):
        return False
    # pydantic unpacks a top-level `Annotated` into `metadata`; one inside `| None` stays in the annotation
    if get_origin(inner) is Annotated:
        metadata = [*metadata, *inner.__metadata__]
    return not any(item is _TIMESTAMP_SERIALIZER for item in metadata)


EntityT = TypeVar('EntityT')


class AerospikeBinType(str, BaseEnum):
    SIMPLE = 'simple'
    COMPOSITE = 'composite'


class AerospikeData(DataTransferObject, Generic[EntityT]):
    """
    A record of an Aerospike set: every field is a bin.

    Scalars and plain containers are simple bins. A `BaseModel` / `RootModel` field (a nested object, a typed
    list or dict) is a composite bin, stored as a map or list; `AerospikeManager` strips its `None` values
    before writing, so an unset optional nested field costs nothing. Datetimes are declared as
    `AerospikeTimestamp` (inside composite models too, so they are stored the same way).

    Subclasses map to and from the domain through `to_entity` / `from_entity`.
    """

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        too_long = tuple(field for field in cls.model_fields if len(field) > BIN_NAME_MAX_LENGTH)
        if too_long:
            raise TypeError(f'Aerospike bin names must be at most {BIN_NAME_MAX_LENGTH} characters, got {too_long!r}')
        bare_dates = tuple(
            field for field, info in cls.model_fields.items() if _is_bare_date_annotation(info.annotation, info.metadata)
        )
        if bare_dates:
            raise TypeError(f'Datetime bins must be declared as `AerospikeTimestamp`, got {bare_dates!r}')

    @classmethod
    def get_bins(cls) -> dict[str, AerospikeBinType]:
        return {
            field: AerospikeBinType.COMPOSITE if _is_composite_annotation(info.annotation) else AerospikeBinType.SIMPLE
            for field, info in cls.model_fields.items()
        }

    def filter_bins(self, bins: dict[str, AerospikeBinType]) -> dict[str, AerospikeBinType]:
        """Hook: the bins this instance writes. Override to leave some out depending on the data."""
        return bins

    @classmethod
    def factory(cls, data: dict[str, Any]) -> Self:
        return cls.model_validate(data)

    def to_entity(self, **kwargs: Any) -> EntityT:
        raise NotImplementedError

    @classmethod
    def from_entity(cls, entity: EntityT) -> Self:
        raise NotImplementedError


# Bound to the real class, not a forward reference: pydantic builds a schema for `type[AerospikeDataT]` fields
AerospikeDataT = TypeVar('AerospikeDataT', bound=AerospikeData)


class AerospikeDataWithPK(NamedTuple, Generic[AerospikeDataT]):
    # None when the record was written without `send_key`: the server then holds only the key digest
    pk: str | None
    data: AerospikeDataT


__all__ = (
    'AerospikeBinType',
    'AerospikeData',
    'AerospikeDataWithPK',
    'AerospikeTimestamp',
    'BIN_NAME_MAX_LENGTH',
    'timestamp_to_bin',
)
