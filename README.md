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
run `copier update` to pull in template updates.

## AerospikeDsn

A settings field for a cluster: `aerospike://host[:port][,host[:port]]/<namespace>`, validated as a URL like
pydantic's `PostgresDsn`. The hosts are the seeds the client learns the rest of the cluster from (a missing port
is 3000), the path names the namespace and is required.

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

A record of a set, every field a bin. Scalars and plain containers are simple bins; a `BaseModel` / `RootModel`
field is a composite bin stored as a map or list. Datetimes are declared as `AerospikeTimestamp`: stored as integer
microseconds since the epoch (UTC), read back as an aware datetime, and compared the same way by the filters; a
bare `datetime` or `date` field is rejected at class definition, as is a bin name over 15 characters. Subclasses
map to and from the domain through `to_entity` / `from_entity`.

```python
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
    def from_entity(cls, entity: Profile) -> 'ProfileData':
        return cls(name=entity.name, age=entity.age, created_at=entity.created_at)
```

## AerospikeManager

ORM-like access to one set, typed by its `AerospikeData` model: `get`, `get_map`, `get_list`, `find_one`, `put`
and `delete`. It is a Django-style manager, not a dddesign repository: a context repository owns one and maps
records to entities. The client is obtained for every call from `client_factory`, a `ConnectionManagerFactory`
from [ddutils](https://github.com/davyddd/ddutils) called with `alias`, the way a
[ddsql](https://github.com/davyddd/ddsql) adapter runs a query inside `factory(alias=...)`: the factory owns which
cluster the alias means, the scope the client lives in (one per process and event loop, the client completes its
futures on the loop it was created in), its lazy creation and its close on shutdown.

```python
import asyncio
import os
from enum import Enum
from functools import partial

from aerospike_native import Client, ClientPolicy, new_client
from ddutils.connections import ConnectionManagerFactory
from ddutils.scoped_registry import ScopedRegistry

from ddaerospike.manager import AerospikeManager


class AerospikeConnectionAlias(str, Enum):
    PRIMARY = 'primary'


async def create_client() -> Client:
    policy = ClientPolicy()
    policy.timeout = 5_000
    return await new_client(policy, settings.AEROSPIKE_URL.seeds)


def client_scope() -> tuple[int, asyncio.AbstractEventLoop]:
    return os.getpid(), asyncio.get_running_loop()


client_registries = {
    AerospikeConnectionAlias.PRIMARY: ScopedRegistry[Client](
        create_func=create_client, scope_func=client_scope, destructor_method_name='close'
    ),
}
aerospike_client_factory = ConnectionManagerFactory(client_registries, default=AerospikeConnectionAlias.PRIMARY)


class ProfileManager(AerospikeManager[ProfileData]):
    client_factory = aerospike_client_factory
    namespace = settings.AEROSPIKE_URL.namespace
    set_name = 'profile'
    ttl_seconds = 30 * 24 * 60 * 60  # set by every write; None leaves the record's TTL alone


profile_manager = ProfileManager()

await profile_manager.put(profile.id, ProfileData.from_entity(profile))
data = await profile_manager.get(profile.id)                       # ProfileData | None
await profile_manager.get_map([1, 2, 3])                           # {'1': ProfileData, ...}
await profile_manager.put(profile.id, data, if_absent=True)        # False when the record exists
await profile_manager.delete(profile.id)

await aerospike_client_factory.close()                             # on shutdown
```

A bin set to `None` is removed from the record by the write; inside a composite bin the `None` values are left
out. The `read_policy_fields`, `batch_policy_fields`, `query_policy_fields` and `write_policy_fields` class
attributes (typed dicts from `ddaerospike.policies`) override the defaults: short timeouts, retries on reads only,
`send_key` on writes so queries can return the primary key.

## Filters

`AerospikeFilterCollection(Model)` holds the conditions of one query on a set, bound to its record model: at most
one secondary-index filter (a query runs on one index: equality on `int` / `str`, a range on `int`) and any number
of filter expressions, ANDed and evaluated on every record the index or the whole set yields. `add_filter` checks
the bin against the model and the value against the bin's scalar (an `int` widens to a `float` bin); values are
normalized the way the server compares them (enums to their value, UUIDs to strings, datetimes to
`AerospikeTimestamp` microseconds, `Decimal` to float), and every operator checks the type it accepts. A composite
bin only takes `is_null` / `is_not_null`, which take no value.

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

## AerospikeFeatureFlagStorage

The Aerospike backend of [ddutils](https://github.com/davyddd/ddutils) feature flags: one set, a flag per record
(`FeatureFlagData`: `enabled`, `proportion`, `payload` bins, the flag name is the primary key), through an
`AerospikeManager[FeatureFlagData]` the application wires. Client and server errors raise
`FeatureFlagStorageError`, so `FeatureFlags` keeps serving the last known values; a record that does not validate
is read as the defaults.

```python
from ddaerospike.feature_flags import AerospikeFeatureFlagStorage, FeatureFlagData
from ddaerospike.manager import AerospikeManager
from ddutils.feature_flags import FeatureFlags


class FeatureFlagManager(AerospikeManager[FeatureFlagData]):
    client_factory = aerospike_client_factory
    namespace = settings.AEROSPIKE_URL.namespace
    set_name = 'feature_flag'


feature_flags = FeatureFlags(AerospikeFeatureFlagStorage(FeatureFlagManager()))

if await feature_flags.is_enabled('new_checkout'):            # proportion 1.0: on or off for everyone
    ...
if await feature_flags.is_enabled('new_ranking', user_id):  # on for `proportion` of the users
    ...
```
