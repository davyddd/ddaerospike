# ddaerospike

[![pypi](https://img.shields.io/pypi/v/ddaerospike.svg)](https://pypi.python.org/pypi/ddaerospike)
[![downloads](https://static.pepy.tech/badge/ddaerospike/month)](https://pepy.tech/project/ddaerospike)
[![versions](https://img.shields.io/pypi/pyversions/ddaerospike.svg)](https://github.com/davyddd/ddaerospike)
[![codecov](https://codecov.io/gh/davyddd/ddaerospike/branch/main/graph/badge.svg)](https://app.codecov.io/github/davyddd/ddaerospike)
[![license](https://img.shields.io/github/license/davyddd/ddaerospike.svg)](https://github.com/davyddd/ddaerospike/blob/main/LICENSE)

Aerospike helpers: typed records, filters and a set manager on top of aerospike-native

## Installation

Install the library using pip:
```bash
pip install ddaerospike
```

## Development

The project runs entirely in Docker. Requires Docker and [Fabric](https://www.fabfile.org/) on the host:

```bash
fab build      # build the dev image
fab tests      # run pytest
fab linters    # run ruff (with --fix), ty and complexipy
fab shell      # IPython inside the container
fab bash       # bash inside the container
```

This project was generated from [dd-lib-stub](https://github.com/davyddd/dd-lib-stub);
run `uvx copier update` to pull in template updates.

## Compatibility

Python 3.11 to 3.15. What changed in each release, breaking changes included,
is in the [GitHub releases](https://github.com/davyddd/ddaerospike/releases).

<!-- The sections below are the library's own; copier update leaves them alone -->

Depends on `aerospike-native>=1.0,<2`, `dddesign>=2.1.1,<3`, `ddutils>=0.4.0,<0.5.0` and `pydantic>=2.10,<3` (see
`AerospikeDsn`). The tests run against a fake of the aerospike-native client, no server is needed.

## aerospike-native

The library sits on `aerospike-native`, the official Rust-core asyncio client. Two of its properties shape how the
manager is wired: a client completes its futures on the event loop it was created in, so the factory behind
`client_factory` keeps one client per process and loop; and on import it installs uvloop as the global event loop
policy (`AEROSPIKE_NO_UVLOOP=1` opts out), which importing `ddaerospike` therefore does too.

Errors come out as the client raises them: `RecordNotFound`, `RecordExistsError` and the other mapped errors
subclass `aerospike_native.exceptions.AerospikeError`, a server code without a mapping arrives as
`aerospike_native.ServerError`, which does not. "Any Aerospike error" is `except (AerospikeError, ServerError)`.

`aerospike-native` 1.0 declares server 8.2+; the library is exercised against Community 8.0.0.10 (get, batch read,
put, delete, query with filter expressions and index filters). The client's API is documented only through its
type stubs.

## AerospikeDsn

A settings field for a cluster: `aerospike://host[:port][,host[:port]]/<namespace>`, validated as a URL like
pydantic's `PostgresDsn`. The hosts are the seeds the client learns the rest of the cluster from (a missing port
is 3000), the path names the namespace and is required: a DSN without one fails validation with `ValueError`.
`seeds` is a string, the form `new_client` takes, unlike the list of `KafkaDsn.bootstrap_servers`.

`pydantic>=2.10` comes from here: `AerospikeDsn` subclasses `pydantic.networks._BaseMultiHostUrl` the way
`PostgresDsn` does. It is not a public pydantic API, so a pydantic upgrade is where to look if the DSN breaks.

```python
from pydantic_settings import BaseSettings

from ddaerospike.dsn import AerospikeDsn


class Settings(BaseSettings):
    AEROSPIKE_URL: AerospikeDsn  # AEROSPIKE_URL=aerospike://aerospike-0,aerospike-1/application


settings = Settings()
settings.AEROSPIKE_URL.seeds      # 'aerospike-0:3000,aerospike-1:3000', what `new_client` takes
settings.AEROSPIKE_URL.namespace  # 'application'
```

## AerospikeData

A record of a set, every field a bin, mapped to and from the domain through `to_entity` / `from_entity`. The
model is a frozen `DataTransferObject` from dddesign: change a record with `model_copy(update=...)`.

```python
from typing import Self

from pydantic import BaseModel

from ddaerospike.data import AerospikeData, AerospikeTimestamp


class Address(BaseModel):
    city: str
    zip_code: str | None = None


class ProfileData(AerospikeData[Profile]):
    name: str
    age: int | None = None
    created_at: AerospikeTimestamp
    address: Address | None = None  # a composite bin

    def to_entity(self, **kwargs) -> Profile:
        return Profile(name=self.name, age=self.age, created_at=self.created_at)

    @classmethod
    def from_entity(cls, entity: Profile) -> Self:
        return cls(name=entity.name, age=entity.age, created_at=entity.created_at)
```

### Bins

Bin names are at most 15 characters (`BIN_NAME_MAX_LENGTH`); a longer field name is rejected at class definition.
`get_bins()` tells which field is which kind of bin (`AerospikeBinType.SIMPLE` / `COMPOSITE`):

- Scalars, `Literal`s, unions and plain containers (`list[str]`, `dict[str, int]`) are simple bins, written as they are.
- A `BaseModel` field is a composite bin, stored as a map, written without its `None` values and validated on read.
  A list or a dict that should be validated the same way, nested models or a structure with a schema, is declared
  through a pydantic `RootModel` (`class Tags(RootModel[list[str]])`), stored as a list or map.
- A field set to `None`, simple or composite, removes the bin on `put`; a record whose bins are all removed is
  deleted by the server. To leave a bin untouched instead, drop it from the dict `filter_bins(bins)` returns, the
  hook that decides which bins an instance writes.
- An empty map or list is read back as is, so a composite field that may be missing from a record needs a default.
- `factory(data)` builds the model from the bins read; `to_entity` and `from_entity` are not abstract, they raise
  `NotImplementedError` at runtime when a subclass does not define them.

A datetime bin is declared as `AerospikeTimestamp`: stored as integer microseconds since the epoch (UTC,
`timestamp_to_bin`), read back as an aware datetime, and the representation a filter compares a `datetime` /
`date` value against. A bare `datetime` or `date` field is rejected at class definition, but only on the model
itself: inside a nested `BaseModel` the check does not reach, and a bare `datetime` there is stored as an ISO
string that no filter matches, so use `AerospikeTimestamp` in composite models as well. A naive datetime is
rejected when the record is constructed.

`get_list` and `find_one` return `AerospikeDataWithPK(pk, data)`; `pk` is `None` for a record written without
`send_key` (the server then holds only the digest), see the write policy below.

## AerospikeManager

ORM-like access to one set, typed by its `AerospikeData` model: `get`, `get_map`, `get_list`, `find_one`, `put`
and `delete`. It is a Django-style manager, not a dddesign repository: a context repository owns one and maps
records to entities. The client is obtained for every call from `client_factory`, a `ConnectionManagerFactory`
from [ddutils](https://github.com/davyddd/ddutils) called with `alias`, the way a
[ddsql](https://github.com/davyddd/ddsql) adapter runs a query inside `factory(alias=...)`: the factory owns which
cluster the alias means, the scope the client lives in (one per process and event loop, the client completes its
futures on the loop it was created in), its lazy creation and its close on shutdown.

```python
from enum import Enum
from functools import partial

from aerospike_native import Client, ClientPolicy, new_client
from ddutils.connections import ConnectionManagerFactory
from ddutils.scoped_registry import ScopedRegistry, process_loop_scope

from ddaerospike.dsn import AerospikeDsn
from ddaerospike.manager import AerospikeManager


class AerospikeConnectionAlias(str, Enum):
    """The cluster to talk to; the alias resolves to its DSN, everything else derives from that."""

    PRIMARY = 'primary'

    @property
    def url(self) -> AerospikeDsn:
        return {AerospikeConnectionAlias.PRIMARY: settings.AEROSPIKE_URL}[self]  # the `Settings` above

    @property
    def namespace(self) -> str:
        return self.url.namespace


async def create_client(alias: AerospikeConnectionAlias) -> Client:
    policy = ClientPolicy()
    policy.timeout = 5_000
    return await new_client(policy, alias.url.seeds)


client_registries = {
    alias: ScopedRegistry[Client](
        create_func=partial(create_client, alias), scope_func=process_loop_scope, destructor_method_name='close'
    )
    for alias in AerospikeConnectionAlias
}
aerospike_client_factory = ConnectionManagerFactory(client_registries, default=AerospikeConnectionAlias.PRIMARY)


class ProfileManager(AerospikeManager[ProfileData]):
    client_factory = aerospike_client_factory
    namespace = AerospikeConnectionAlias.PRIMARY.namespace
    set_name = 'profile'
    ttl_seconds = 30 * 24 * 60 * 60  # set by every write; None leaves the record's TTL alone
    # alias = AerospikeConnectionAlias.PRIMARY  # None picks the factory's default


profile_manager = ProfileManager()

await profile_manager.put(profile.id, ProfileData.from_entity(profile))
data = await profile_manager.get(profile.id)                       # ProfileData | None
await profile_manager.get_map([1, 2, 3])                           # {'1': ProfileData, ...}
await profile_manager.put(profile.id, data, if_absent=True)        # False when the record exists
await profile_manager.delete(profile.id)

await aerospike_client_factory.close()                             # on shutdown
```

A subclass must bind the record type (`AerospikeManager[ProfileData]`, `TypeError` otherwise) and declare
`client_factory` (a `ConnectionManagerFactory`), `namespace` and `set_name`; a missing one is an `AttributeError`,
a mistyped one a `TypeError`, both at class definition. The primary key is `str(primary_key)` whatever is passed.

| Method | Returns | |
|---|---|---|
| `get(pk)` | `Data \| None` | `None` when the record is missing or has none of the model's bins |
| `get_map(pks)` | `dict[str, Data]` | keyed by `str(pk)`; a key that is missing, or that the batch could not answer (a timeout, a node away), is left out |
| `get_list(filters=None, limit=40)` | `list[AerospikeDataWithPK]` | the whole set without filters, see below for `limit` |
| `find_one(filters)` | `AerospikeDataWithPK \| None` | an arbitrary match when several records match |
| `put(pk, data, ttl=None, *, if_absent=False)` | `bool` | merges the model's bins into the record, bins the model does not know stay; `if_absent` is create-only, checked by the server, `False` when the record exists |
| `delete(pk)` | `bool` | `False` when there was no record |

`limit` of `get_list` goes to the server as `max_records`, which stops the nodes once that many records are out
instead of streaming the whole set, and the client stops reading at `limit`. The server splits `max_records`
across the nodes and treats the share as an upper bound, so a query may return fewer than `limit` records although
more match; there is no order and no cursor. Pagination over a set is not what a query is for.

### Policies

`read_policy_fields`, `batch_policy_fields`, `query_policy_fields` and `write_policy_fields` are class attributes
holding the fields set on the client's policy objects (typed dicts from `ddaerospike.policies`, every other field
keeps the client default; `build_policy(PolicyClass, fields)` is the public helper). Overriding one replaces the
whole dict, so extend the default rather than start from scratch: `write_policy_fields = {**DEFAULT_WRITE_POLICY,
'total_timeout': 5_000}`, or `send_key` is silently gone. Milliseconds everywhere; `total_timeout` bounds the
whole command, retries included, `socket_timeout` one round trip.

| | total / socket timeout | retries | |
|---|---|---|---|
| `DEFAULT_READ_POLICY` | 2 s / 2 s | 2, 10 ms apart | |
| `DEFAULT_BATCH_POLICY` | 5 s / 5 s | 2, 10 ms apart | |
| `DEFAULT_QUERY_POLICY` | 60 s / 30 s | none | |
| `DEFAULT_WRITE_POLICY` | 2 s / 2 s | none, a timed-out write may have landed | `send_key=True`: the key is stored with the record, so queries return `pk` |

### TTL

`ttl_seconds` on the manager is set by every `put`, so the TTL slides: an active record keeps living, one nobody
touches expires. The `ttl` argument of `put` overrides it for that write. Without either the write leaves the
record's TTL as it is (`Expiration.DONT_UPDATE`) and a new record gets the namespace `default-ttl`. A namespace
with `nsup-period 0` refuses a write that sets a TTL: the server answers FORBIDDEN, which arrives as `ServerError`.

## Filters

`AerospikeFilterCollection(Model)` holds the conditions of one query on a set, ANDed, bound to the set's record
model: at most one secondary-index filter (a query runs on one index) and any number of filter expressions,
evaluated on every record the index or the whole set yields. The binding is what makes a filter safe: the server
compares typed values, and a value of another type than the bin matches nothing, silently. `add_filter` checks that
the bin exists and that the value is of the scalar the bin holds, and a manager refuses a collection built for
another model.

The scalar of a bin follows its annotation: `bool`; `int` for `int`, `AerospikeTimestamp` (the value is a
`datetime` or `date`, converted the way the bin is stored, a `date` as midnight UTC) and an `Enum` of ints; `float`
for `float` and `Decimal`; `str` for `str`, `UUID` and an `Enum` of strings. The value may be an `Enum` member or
a subclass of these types, it counts as its base type (`normalize_scalar`). An `int` value widens to a `float`
bin, any other mismatch raises `ValueError`. A list, a dict, a union of several types and a composite bin take
only the null checks.

| Operator | `int`, `float` bins | `str`, `bool` bins | with `use_index=True` |
|---|---|---|---|
| `EQUAL`, `NOT_EQUAL` | ✓ | ✓ | `EQUAL` on `int`, `str` |
| `GT`, `GTE`, `LT`, `LTE` | ✓ | | |
| `BETWEEN` `(low, high)`, inclusive | ✓ | | `int` |
| `IS_NULL`, `IS_NOT_NULL` (no value) | any bin, composite too | | |

```python
from ddaerospike.filters import AerospikeFilterCollection, AerospikeFilterOperator

filters = (
    AerospikeFilterCollection(ProfileData)
    .add_filter('name', AerospikeFilterOperator.EQUAL, 'john', use_index=True)   # needs an index on `name`
    .add_filter('age', AerospikeFilterOperator.BETWEEN, [18, 65])
    .add_filter('address', AerospikeFilterOperator.IS_NOT_NULL)
)

items = await profile_manager.get_list(filters, limit=100)   # [AerospikeDataWithPK(pk='1', data=ProfileData(...)), ...]
first = await profile_manager.find_one(filters)
```

`IS_NULL` is "the bin is missing", since the manager writes `None` as no bin; `NOT_EQUAL` on a missing bin is
false, so "not equal or missing" is two conditions. A query takes one `use_index=True` filter, a second one raises;
a pair the index cannot answer (anything but `EQUAL` on `int` / `str` and `BETWEEN` on `int`) raises too, instead
of reaching the client, which would panic. The secondary index itself is created on the cluster (`asinfo
sindex-create`), not by code; without one the same condition goes in as an expression and the query reads the
whole set. There is deliberately no `IN`, no list-contains and no string operator.

The building blocks are public for a filter built by hand: `AerospikeFilter(field, operator, value)` with its
`expression` and `index_filter`, `bin_scalar_type(annotation)`, `model_bin_scalar_types(model)`,
`normalize_scalar(value)` and the `Scalar` alias. `AerospikeFilterCollection` is a pydantic model whose
`index_filter` and `filters` fields are open; only `add_filter` runs the bin check, so fill them through it.

## AerospikeFeatureFlagStorage

The Aerospike backend of [ddutils](https://github.com/davyddd/ddutils) feature flags: one set, a flag per record
(`FeatureFlagData`: `enabled`, `proportion` validated 0..1 and `payload` bins, the flag name is the primary key),
through an `AerospikeManager[FeatureFlagData]` the application wires. Client and server errors raise
`FeatureFlagStorageError`, so `FeatureFlags` keeps serving the last known values.

```python
from ddaerospike.feature_flags import AerospikeFeatureFlagStorage, FeatureFlagData
from ddaerospike.manager import AerospikeManager
from ddutils.feature_flags import FeatureFlags


class FeatureFlagManager(AerospikeManager[FeatureFlagData]):
    client_factory = aerospike_client_factory
    namespace = settings.AEROSPIKE_URL.namespace
    set_name = 'feature_flag'


feature_flags = FeatureFlags(AerospikeFeatureFlagStorage(FeatureFlagManager()))

if await feature_flags.is_enabled('new_checkout'):            # default proportion: on or off for everyone
    ...
if await feature_flags.is_enabled('new_ranking', user_id):  # on for `proportion` of the users
    ...
```

What differs from the Redis storage:

- A record that does not validate (edited by hand) is read by `get` as the defaults, with an ERROR log entry. In
  `get_map` one such record makes the whole list unavailable: it raises `FeatureFlagStorageError`, and `get` tells
  which flag it is.
- `get_map` reads the set with `limit=LIST_LIMIT` (1000) and logs an ERROR when the set holds at least that many
  flags. It is keyed by the record's `pk`, so the manager must write with `send_key=True`, the default write policy;
  a record written without it is skipped.
- `rename` is put-then-delete: Aerospike has no transaction over two keys.
