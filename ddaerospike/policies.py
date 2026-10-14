from collections.abc import Mapping
from typing import Any, TypedDict, TypeVar

from aerospike_native import CommitLevel, GenerationPolicy, ReadModeAP, ReadModeSC, RecordExistsAction, Replica

# Milliseconds everywhere. `total_timeout` bounds the whole command, retries included; `socket_timeout`
# one round trip. Reads are idempotent and retried; writes are not retried (a timed-out write may have landed).


class AerospikeReadPolicyFields(TypedDict, total=False):
    total_timeout: int
    socket_timeout: int
    max_retries: int
    sleep_between_retries: int
    replica: Replica
    read_mode_ap: ReadModeAP
    read_mode_sc: ReadModeSC
    read_touch_ttl: int
    use_compression: bool


class AerospikeBatchPolicyFields(AerospikeReadPolicyFields, total=False):
    allow_inline: bool
    respond_all_keys: bool


class AerospikeQueryPolicyFields(TypedDict, total=False):
    total_timeout: int
    socket_timeout: int
    max_retries: int
    sleep_between_retries: int
    replica: Replica
    records_per_second: int
    record_queue_size: int
    include_bin_data: bool
    use_compression: bool


class AerospikeWritePolicyFields(TypedDict, total=False):
    total_timeout: int
    socket_timeout: int
    max_retries: int
    sleep_between_retries: int
    record_exists_action: RecordExistsAction
    generation_policy: GenerationPolicy
    generation: int
    commit_level: CommitLevel
    send_key: bool
    durable_delete: bool
    use_compression: bool


DEFAULT_READ_POLICY: AerospikeReadPolicyFields = {
    'total_timeout': 2_000,
    'socket_timeout': 2_000,
    'max_retries': 2,
    'sleep_between_retries': 10,
}
DEFAULT_BATCH_POLICY: AerospikeBatchPolicyFields = {
    'total_timeout': 5_000,
    'socket_timeout': 5_000,
    'max_retries': 2,
    'sleep_between_retries': 10,
}
DEFAULT_QUERY_POLICY: AerospikeQueryPolicyFields = {
    'total_timeout': 60_000,
    'socket_timeout': 30_000,
}
DEFAULT_WRITE_POLICY: AerospikeWritePolicyFields = {
    'total_timeout': 2_000,
    'socket_timeout': 2_000,
    'max_retries': 0,
    # Store the user key with the record, so queries and scans can return it (see `AerospikeDataWithPK.pk`)
    'send_key': True,
}


PolicyT = TypeVar('PolicyT')


def build_policy(policy_class: type[PolicyT], fields: Mapping[str, Any]) -> PolicyT:
    """A policy object with `fields` set on it; every other field keeps the client default."""
    policy = policy_class()
    for name, value in fields.items():
        setattr(policy, name, value)
    return policy


__all__ = (
    'DEFAULT_BATCH_POLICY',
    'DEFAULT_QUERY_POLICY',
    'DEFAULT_READ_POLICY',
    'DEFAULT_WRITE_POLICY',
    'AerospikeBatchPolicyFields',
    'AerospikeQueryPolicyFields',
    'AerospikeReadPolicyFields',
    'AerospikeWritePolicyFields',
    'build_policy',
)
