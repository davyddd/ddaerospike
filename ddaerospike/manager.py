from abc import ABC
from collections.abc import Hashable, Sequence
from contextlib import AbstractAsyncContextManager
from functools import cached_property
from typing import Any, ClassVar, Generic, Protocol, TypeVar, cast

from aerospike_native import (
    BatchPolicy,
    Client,
    Expiration,
    Key,
    PartitionFilter,
    QueryPolicy,
    ReadPolicy,
    RecordExistsAction,
    Statement,
    WritePolicy,
)
from aerospike_native.exceptions import RecordExistsError, RecordNotFound
from ddutils.class_helpers import get_generic_base_argument, require_class_attribute
from ddutils.connections import ConnectionManagerFactory

from ddaerospike.data import AerospikeBinType, AerospikeData, AerospikeDataT, AerospikeDataWithPK
from ddaerospike.filters import AerospikeFilterCollection
from ddaerospike.policies import (
    DEFAULT_BATCH_POLICY,
    DEFAULT_QUERY_POLICY,
    DEFAULT_READ_POLICY,
    DEFAULT_WRITE_POLICY,
    AerospikeBatchPolicyFields,
    AerospikeQueryPolicyFields,
    AerospikeReadPolicyFields,
    AerospikeWritePolicyFields,
    build_policy,
)


class Stringable(Protocol):
    def __str__(self) -> str: ...


class AerospikeManager(ABC, Generic[AerospikeDataT]):
    """
    ORM-like access to one Aerospike set (a Django-style `Manager`, not a `ConnectionManager`), records typed as
    an `AerospikeData` model. Like `GenericCache` of ddredis it is not a `dddesign` Repository: a context
    Repository owns one and maps records to entities. And like `GenericCache` it knows nothing of the
    application's config: the subclass wires the client factory, the namespace and the set.

    The client is obtained for every call from `client_factory`, a `ConnectionManagerFactory` called with
    `alias`, like a ddsql adapter runs a query inside `factory(alias=...)`. The factory owns the lifecycle:
    which cluster the alias means, the scope the client lives in (one per process and event loop, the client
    completes its futures on the loop it was created in), its lazy creation and its close on shutdown.

    Example:
        class ProfileAerospikeManager(AerospikeManager[ProfileAerospike]):
            client_factory = aerospike_client_factory
            namespace = settings.AEROSPIKE_URL.namespace
            set_name = 'profile'
            ttl_seconds = 30 * 24 * 60 * 60

        profile_aerospike_manager = ProfileAerospikeManager()
    """

    _model: ClassVar[type[AerospikeData]]

    # Wiring, set by the subclass
    client_factory: ClassVar[ConnectionManagerFactory[Any, Client, Any]]
    alias: ClassVar[Hashable | None] = None
    namespace: ClassVar[str]
    set_name: ClassVar[str]
    # TTL set by every write, so an active record keeps living and a forgotten one expires. None: a write leaves
    # the record's TTL alone, a new record gets the namespace `default-ttl` (0 on the mail cluster: never expires)
    ttl_seconds: ClassVar[int | None] = None
    read_policy_fields: ClassVar[AerospikeReadPolicyFields] = DEFAULT_READ_POLICY
    batch_policy_fields: ClassVar[AerospikeBatchPolicyFields] = DEFAULT_BATCH_POLICY
    query_policy_fields: ClassVar[AerospikeQueryPolicyFields] = DEFAULT_QUERY_POLICY
    write_policy_fields: ClassVar[AerospikeWritePolicyFields] = DEFAULT_WRITE_POLICY

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        model = get_generic_base_argument(cls, AerospikeManager)
        if model is None or isinstance(model, TypeVar):
            raise TypeError(f'{cls.__name__} must specify the record type: class {cls.__name__}(AerospikeManager[YourData])')
        cls._model = model

        require_class_attribute(cls, 'client_factory', ConnectionManagerFactory)
        require_class_attribute(cls, 'namespace', str)
        require_class_attribute(cls, 'set_name', str)

    def _open_client(self) -> AbstractAsyncContextManager[Client]:
        return self.client_factory(alias=self.alias)

    @cached_property
    def read_policy(self) -> ReadPolicy:
        return build_policy(ReadPolicy, self.read_policy_fields)

    @cached_property
    def batch_policy(self) -> BatchPolicy:
        return build_policy(BatchPolicy, self.batch_policy_fields)

    def _query_policy(self, filters: AerospikeFilterCollection[AerospikeDataT] | None, limit: int) -> QueryPolicy:
        policy = build_policy(QueryPolicy, self.query_policy_fields)
        if filters is not None:
            policy.filter_expression = filters.compiled_expression
        # Stops the nodes once `limit` records are out instead of streaming the whole set to the client; the
        # server splits it across nodes and treats it as an upper bound, `_collect` applies the exact limit.
        policy.max_records = limit
        return policy

    def _write_policy(self, ttl: int | None = None, **overrides: Any) -> WritePolicy:
        policy = build_policy(WritePolicy, {**self.write_policy_fields, **overrides})
        policy.expiration = self._expiration(ttl)
        return policy

    def _expiration(self, ttl: int | None) -> Expiration:
        if ttl is None:
            ttl = self.ttl_seconds
        return Expiration.seconds(ttl) if ttl is not None else Expiration.DONT_UPDATE

    def _key(self, primary_key: Stringable) -> Key:
        return Key(self.namespace, self.set_name, str(primary_key))

    def _build(self, bins: dict[str, Any] | None) -> AerospikeDataT | None:
        """The model from the bins of a record; None when the record has no bin the model knows.

        An empty map or list is a value, not an absent bin: whether a missing bin has a default is up to the model.
        """
        if not bins:
            return None

        model_bins = self._model.get_bins()
        data = {name: value for name, value in bins.items() if name in model_bins}
        if not data:
            return None

        return cast('AerospikeDataT', self._model.factory(data))

    def _serialize_bins(self, entity: AerospikeDataT) -> dict[str, Any]:
        bins = entity.filter_bins(self._model.get_bins())
        simple_bins = {name for name, bin_type in bins.items() if bin_type is AerospikeBinType.SIMPLE}
        composite_bins = {name for name, bin_type in bins.items() if bin_type is AerospikeBinType.COMPOSITE}

        # A bin set to None, simple or composite, is removed from the record by the write. Inside a composite bin
        # the None values are left out, so an unset optional nested field costs nothing.
        data = entity.model_dump(mode='json', exclude_none=True, include=composite_bins)
        data.update({name: None for name in composite_bins if getattr(entity, name) is None})
        data.update(entity.model_dump(mode='json', exclude_none=False, include=simple_bins))
        return data

    async def get(self, primary_key: Stringable) -> AerospikeDataT | None:
        bins = list(self._model.get_bins())
        async with self._open_client() as client:
            try:
                record = await client.get(self._key(primary_key), bins, policy=self.read_policy)
            except RecordNotFound:
                return None
        return self._build(record.bins)

    async def get_map(self, primary_keys: Sequence[Stringable]) -> dict[str, AerospikeDataT]:
        if not primary_keys:
            return {}

        bins = list(self._model.get_bins())
        keys = [self._key(primary_key) for primary_key in primary_keys]
        async with self._open_client() as client:
            batch = await client.batch_read(keys, bins, batch_policy=self.batch_policy)

        results: dict[str, AerospikeDataT] = {}
        for batch_record in batch:
            # Only what was read: a key the batch could not answer (a timeout, a node away) is left out, like a
            # missing one, so one bad key does not fail the whole map
            record = batch_record.record  # a copy of the record; read it once
            data = self._build(record.bins) if record is not None else None
            if data is not None:
                results[str(batch_record.key.value)] = data
        return results

    async def get_list(
        self, filters: AerospikeFilterCollection[AerospikeDataT] | None = None, limit: int = 40
    ) -> list[AerospikeDataWithPK[AerospikeDataT]]:
        """Records matching `filters` (the whole set without them), at most `limit`, in no particular order."""
        statement = Statement(self.namespace, self.set_name, list(self._model.get_bins()))
        if filters is not None:
            if filters.model is not self._model:
                raise ValueError(
                    f'The filters are for `{filters.model.__name__}`, {type(self).__name__} reads `{self._model.__name__}`'
                )
            if (index_filter := filters.compiled_index_filter) is not None:
                statement.filters = [index_filter]

        async with self._open_client() as client:
            recordset = await client.query(statement, PartitionFilter.all(), policy=self._query_policy(filters, limit))
            try:
                return await self._collect(recordset, limit)
            finally:
                recordset.close()

    async def _collect(self, recordset: Any, limit: int) -> list[AerospikeDataWithPK[AerospikeDataT]]:
        results: list[AerospikeDataWithPK[AerospikeDataT]] = []
        async for record in recordset:
            data = self._build(record.bins)
            if data is None:
                continue
            results.append(AerospikeDataWithPK(pk=self._primary_key(record), data=data))
            if len(results) >= limit:
                break
        return results

    @staticmethod
    def _primary_key(record: Any) -> str | None:
        value = record.key.value if record.key is not None else None
        return str(value) if value is not None else None

    async def find_one(self, filters: AerospikeFilterCollection[AerospikeDataT]) -> AerospikeDataWithPK[AerospikeDataT] | None:
        return next(iter(await self.get_list(filters, limit=1)), None)

    async def put(
        self, primary_key: Stringable, entity: AerospikeDataT, ttl: int | None = None, *, if_absent: bool = False
    ) -> bool:
        """Create or overwrite the record's bins (bins the model does not know stay untouched).

        With `if_absent` the write is create-only, checked by the server in the same operation:
        False when the record already exists, so concurrent writers of one key get exactly one True.
        """
        overrides = {'record_exists_action': RecordExistsAction.CREATE_ONLY} if if_absent else {}
        async with self._open_client() as client:
            try:
                await client.put(
                    self._key(primary_key), self._serialize_bins(entity), policy=self._write_policy(ttl, **overrides)
                )
            except RecordExistsError:
                return False
        return True

    async def delete(self, primary_key: Stringable) -> bool:
        """Remove the record. False when there was none."""
        async with self._open_client() as client:
            return await client.delete(self._key(primary_key), policy=self._write_policy())


__all__ = ('AerospikeManager', 'Stringable')
