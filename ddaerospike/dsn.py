from typing import Any, cast

from pydantic import GetCoreSchemaHandler, GetJsonSchemaHandler, UrlConstraints
from pydantic.json_schema import JsonSchemaValue
from pydantic.networks import _BaseMultiHostUrl
from pydantic_core.core_schema import AfterValidatorFunctionSchema, CoreSchema, no_info_after_validator_function

DEFAULT_PORT = 3000


class AerospikeDsn(_BaseMultiHostUrl):
    """`aerospike://host[:port][,host[:port]]/<namespace>`.

    The hosts are the seeds the client learns the rest of the cluster from; a missing port is 3000.
    The path names the namespace and is required. Built like pydantic's own `PostgresDsn`.
    """

    _constraints = UrlConstraints(allowed_schemes=['aerospike'], host_required=True, default_port=DEFAULT_PORT)

    @classmethod
    def __get_pydantic_core_schema__(cls, source: type[Any], handler: GetCoreSchemaHandler) -> CoreSchema:
        return no_info_after_validator_function(_require_namespace, super().__get_pydantic_core_schema__(source, handler))

    @classmethod
    def __get_pydantic_json_schema__(cls, core_schema: CoreSchema, handler: GetJsonSchemaHandler) -> JsonSchemaValue:
        # Peel off the after-validator above, the base class expects its own function-wrap schema
        inner = cast('AfterValidatorFunctionSchema', core_schema)['schema']
        return super().__get_pydantic_json_schema__(inner, handler)

    @property
    def namespace(self) -> str:
        return (self.path or '').strip('/')

    @property
    def seeds(self) -> str:
        """`host:port,host:port`, the form `aerospike_native.new_client` takes."""
        # `default_port` fills the port of a single host only; in a host list the others come back as None
        return ','.join(f'{host["host"]}:{host["port"] or DEFAULT_PORT}' for host in self.hosts())


def _require_namespace(dsn: AerospikeDsn) -> AerospikeDsn:
    if not dsn.namespace:
        raise ValueError('the path must name the namespace, e.g. aerospike://host:3000/application_db')
    return dsn


__all__ = ('AerospikeDsn',)
