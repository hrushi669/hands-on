# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Atlan Pte. Ltd.
import logging
from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import Asset, Table
from pyatlan.model.enums import AtlanConnectorType, LineageDirection
from pyatlan.model.fluent_search import FluentSearch
from pyatlan.model.lineage import FluentLineage
from ref.cbe_common import time_it

logging.basicConfig(level=logging.INFO, filename="lineage.logs", filemode='a', format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def main():
    client = AtlanClient()

    # TODO: replace with YOUR connection's name
    connection = client.asset.find_connections_by_name("hrushikesh-dokala", AtlanConnectorType.MSSQL)[0]

    def traverse_and_log():
        request = FluentSearch(
            wheres=[
                Asset.TYPE_NAME.eq("Table"),
                Table.QUALIFIED_NAME.startswith(connection.qualified_name),
                Table.DATABASE_NAME.eq("wwi", True),
                Table.SCHEMA_NAME.eq("sales", True),
                Table.NAME.eq("orders", True)
            ]
        ).to_request()
        response = client.asset.search(request)
        starting_guid = ""
        for asset in response:
            starting_guid = asset.guid

        request = FluentLineage(
            starting_guid=starting_guid, # this defines the starting node of the lineage ( you will not see the lineage of the current immediate downstream asset, it only depends on the stating node)
            direction=LineageDirection.DOWNSTREAM, # can be UPSTREAM
            where_assets=[FluentLineage.ACTIVE],
            includes_in_results=[Asset.TYPE_NAME.in_lineage.neq("Process")]
        ).request
        request.immediate_neighbors = True
        response = client.asset.get_lineage_list(request)
        count = 0
        for asset in response:
            logger.info(
                f"{asset.qualified_name} -> "
                f"{[x.qualified_name for x in asset.immediate_downstream] if asset.immediate_downstream else []}"
            )
            count += 1
        logger.info(f"Found {count} downstream assets (not including processes).")
    time_it(traverse_and_log)


if __name__ == '__main__':
    main()
