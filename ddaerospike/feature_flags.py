import logging
from typing import Any, Self

from aerospike_native import ServerError
from aerospike_native.exceptions import AerospikeError
from ddutils.feature_flags import DEFAULT_PROPORTION, FeatureFlagRecord, FeatureFlagStorage, FeatureFlagStorageError
from pydantic import Field, ValidationError

from ddaerospike.data import AerospikeData
from ddaerospike.manager import AerospikeManager

logger = logging.getLogger(__name__)

# The set is read whole by an admin panel; well above the number of flags a product ever has
LIST_LIMIT = 1000


class FeatureFlagData(AerospikeData[FeatureFlagRecord]):
    """A record of the feature flag set: the flag name is the primary key, the rest are its bins."""

    enabled: bool = False
    proportion: float = Field(default=DEFAULT_PROPORTION, ge=0, le=1)
    payload: dict[str, Any] = Field(default_factory=dict)

    def to_entity(self, **kwargs: Any) -> FeatureFlagRecord:  # noqa: ARG002
        return FeatureFlagRecord(enabled=self.enabled, proportion=self.proportion, payload=self.payload)

    @classmethod
    def from_entity(cls, entity: FeatureFlagRecord) -> Self:
        return cls(enabled=entity.enabled, proportion=entity.proportion, payload=dict(entity.payload))


class AerospikeFeatureFlagStorage(FeatureFlagStorage):
    """
    Feature flags in one Aerospike set, a flag per record, through an `AerospikeManager[FeatureFlagData]`
    the application wires (client factory, namespace, set).

    A record whose bins do not validate (edited by hand) is read as the defaults, like a malformed Redis entry.
    `rename` is put-then-delete: Aerospike has no multi-record transaction for two keys.

    Example:
        class FeatureFlagManager(AerospikeManager[FeatureFlagData]):
            client_factory = aerospike_client_factory
            namespace = settings.AEROSPIKE_URL.namespace
            set_name = 'feature_flag'

        feature_flags = FeatureFlags(AerospikeFeatureFlagStorage(FeatureFlagManager()))
    """

    def __init__(self, manager: AerospikeManager[FeatureFlagData]) -> None:
        self.manager = manager

    async def get(self, name: str) -> FeatureFlagRecord | None:
        try:
            data = await self.manager.get(name)
        except (AerospikeError, ServerError) as e:
            raise FeatureFlagStorageError() from e
        except ValidationError as e:
            logger.error(
                {'message': 'FEATURE_FLAGS: Malformed record, reading the defaults', 'flag_name': name, 'error': str(e)}
            )
            return FeatureFlagRecord()
        return data.to_entity() if data is not None else None

    async def get_map(self) -> dict[str, FeatureFlagRecord]:
        try:
            items = await self.manager.get_list(limit=LIST_LIMIT)
        except (AerospikeError, ServerError) as e:
            raise FeatureFlagStorageError() from e
        except ValidationError as e:
            # one bad record hides the list; `get` tells which flag it is
            logger.error({'message': 'FEATURE_FLAGS: Malformed record in the set, the list is unavailable', 'error': str(e)})
            raise FeatureFlagStorageError() from e
        if len(items) >= LIST_LIMIT:
            logger.error({'message': 'FEATURE_FLAGS: The set holds more flags than the list reads', 'limit': LIST_LIMIT})
        return {item.pk: item.data.to_entity() for item in items if item.pk is not None}

    async def put(self, name: str, record: FeatureFlagRecord) -> None:
        try:
            await self.manager.put(name, FeatureFlagData.from_entity(record))
        except (AerospikeError, ServerError) as e:
            raise FeatureFlagStorageError() from e

    async def delete(self, name: str) -> bool:
        try:
            return await self.manager.delete(name)
        except (AerospikeError, ServerError) as e:
            raise FeatureFlagStorageError() from e


__all__ = ('AerospikeFeatureFlagStorage', 'FeatureFlagData')
