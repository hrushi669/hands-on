# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Atlan Pte. Ltd.
import logging
from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import Asset, Table, Connection
from pyatlan.model.enums import AtlanConnectorType
from pyatlan.model.fields.atlan_fields import AtlanField
from pyatlan.model.fluent_search import FluentSearch
from pyatlan.model.search import IndexSearchRequest
from ref.cbe_common import time_it

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    client = AtlanClient()

    # TODO: Replace with YOUR connection's name
    connection = client.asset.find_connections_by_name("hrushikesh-dokala", AtlanConnectorType.MSSQL)[0]

    prefix = "C"
    guids = []

    def get_directly():
        table = client.asset.get_by_qualified_name(
            f"{connection.qualified_name}/wwi/Warehouse/PackageTypes",
            asset_type=Table
        )
        logger.info(f"Found {table}")
    time_it(get_directly)

    def filter_and_log():
        filtered = to_asset_list(filter_tables(connection, prefix), client)
        logger.info(f"Found {len(filtered)} tables starting with '{prefix}'.")
    time_it(filter_and_log)

    def project_and_log():
        projections = [Table.DATABASE_NAME, Table.SCHEMA_NAME, Table.NAME, Table.DESCRIPTION]
        projected = to_asset_list(project(connection, prefix, projections), client)
        logger.info(f"Found {len(projected)} tables starting with '{prefix}':")
        for asset in projected:
            if isinstance(asset, Table):
                description = "(no description)" if asset.description is None else asset.description
                logger.info(
                    f" ... {asset.database_name}.{asset.schema_name}.{asset.name} = {description}"
                )
                guids.append(asset.guid)
    time_it(project_and_log)

    def fetch_and_log():
        for guid in guids:
            table = client.asset.get_by_guid(guid, asset_type=Table)
            description = "(no description)" if table.description is None else table.description
            logger.info(
                f" ... {table.database_name}.{table.schema_name}.{table.name} = {description}"
            )
    time_it(fetch_and_log)


def to_asset_list(request, client: AtlanClient) -> list[Asset]:
    assets = []
    for result in client.asset.search(request):
        assets.append(result)
    return assets


def filter_tables(connection: Connection, prefix: str) -> IndexSearchRequest:
    return FluentSearch(
        wheres=[
            Asset.TYPE_NAME.eq("Table"),
            Asset.STATUS.eq("ACTIVE"),
            Asset.QUALIFIED_NAME.startswith(connection.qualified_name),
            Table.NAME.startswith(prefix)
        ]
    ).to_request()
 

def project(connection: Connection, prefix: str, projections: list[AtlanField]) -> IndexSearchRequest:
    return FluentSearch(
        wheres=[
            Asset.TYPE_NAME.eq("Table"),
            Asset.STATUS.eq("ACTIVE"),
            Asset.QUALIFIED_NAME.startswith(connection.qualified_name),
            Table.NAME.startswith(prefix)
        ],
        _includes_on_results=[field.atlan_field_name for field in projections]
    ).to_request()


if __name__ == '__main__':
    main()
