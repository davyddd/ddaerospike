"""A stand-in for `aerospike_native.Client`, so the manager tests run without a server."""

import os
from dataclasses import dataclass, field
from typing import Any

from aerospike_native import Key, RecordExistsAction, ResultCode
from aerospike_native.exceptions import RecordExistsError, RecordNotFound
from ddutils.connections import ConnectionManagerFactory
from ddutils.scoped_registry import ScopedRegistry


@dataclass
class FakeRecord:
    key: Key | None
    bins: dict[str, Any]


@dataclass
class FakeBatchRecord:
    key: Key
    record: FakeRecord | None


class FakeRecordSet:
    def __init__(self, records: list[FakeRecord]):
        self._records = records
        self.closed = False

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        for record in self._records:
            yield record

    def close(self) -> None:
        self.closed = True


class FakeClient:
    """Records live in `store` by (namespace, set, pk); every call remembers the policy it was given."""

    def __init__(self):
        self.store: dict[tuple[str, str, str], dict[str, Any]] = {}
        self.policies: list[Any] = []
        self.query_calls: list[dict[str, Any]] = []
        self.closed = False

    @staticmethod
    def _index(key: Key) -> tuple[str, str, str]:
        return key.namespace, key.set_name, str(key.value)

    async def get(self, key: Key, bins: list[str], policy: Any = None) -> FakeRecord:
        self.policies.append(policy)
        stored = self.store.get(self._index(key))
        if stored is None:
            raise RecordNotFound('not found', ResultCode.KEY_NOT_FOUND_ERROR)
        return FakeRecord(key, {name: value for name, value in stored.items() if name in bins})

    async def batch_read(self, keys: list[Key], bins: list[str], batch_policy: Any = None) -> list[FakeBatchRecord]:
        self.policies.append(batch_policy)
        result = []
        for key in keys:
            stored = self.store.get(self._index(key))
            record = FakeRecord(key, {n: v for n, v in stored.items() if n in bins}) if stored is not None else None
            result.append(FakeBatchRecord(key, record))
        return result

    async def query(self, statement: Any, _partition_filter: Any, policy: Any = None) -> FakeRecordSet:
        self.query_calls.append({'statement': statement, 'policy': policy})
        self.policies.append(policy)
        # `Statement` exposes the set but not the namespace; the fake holds one namespace anyway
        records = [
            FakeRecord(Key(ns, set_name, pk), dict(bins))
            for (ns, set_name, pk), bins in self.store.items()
            if set_name == statement.set_name
        ]
        self.last_recordset = FakeRecordSet(records)
        return self.last_recordset

    async def put(self, key: Key, bins: dict[str, Any], policy: Any = None) -> None:
        self.policies.append(policy)
        index = self._index(key)
        if policy is not None and policy.record_exists_action == RecordExistsAction.CREATE_ONLY and index in self.store:
            raise RecordExistsError('exists', ResultCode.KEY_EXISTS_ERROR)
        current = self.store.setdefault(index, {})
        for name, value in bins.items():
            if value is None:
                current.pop(name, None)
            else:
                current[name] = value

    async def delete(self, key: Key, policy: Any = None) -> bool:
        self.policies.append(policy)
        return self.store.pop(self._index(key), None) is not None

    def close(self) -> None:
        self.closed = True


@dataclass
class ClientSetup:
    client: FakeClient = field(default_factory=FakeClient)

    @property
    def factory(self) -> ConnectionManagerFactory:
        return ConnectionManagerFactory(
            {'primary': ScopedRegistry[FakeClient](create_func=lambda: self.client, scope_func=os.getpid)}, default='primary'
        )
