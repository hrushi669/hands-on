# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Atlan Pte. Ltd.
import logging

from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import Table
from pyatlan.model.core import Announcement
from pyatlan.model.enums import AtlanConnectorType, AnnouncementType
from cbe_common import time_it

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    client = AtlanClient()

    # TODO: Replace with YOUR connection's name
    connection = client.asset.find_connections_by_name("hrushikesh-dokala", AtlanConnectorType.MSSQL)[0]

    def create_table() -> str:
        create = Table.creator(
            name="MyTable",
            schema_qualified_name=f"{connection.qualified_name}/wwi/Sales"
        )
        create.set_announcement(Announcement(
            announcement_type=AnnouncementType.INFORMATION,
            announcement_title="FYI",
            announcement_message="This is an entirely new table."
        ))
        response = client.asset.save(create)
        result = response.assets_created(Table)[0]
        logger.info(f"Result: {result}")
        return result.guid
    guid = time_it(create_table)

    def update_table():
        update = Table.updater(
            f"{connection.qualified_name}/wwi/Sales/MyTable",
            "MyTable"
        )
        update.remove_announcement()
        response = client.asset.save(update)
        result = response.assets_updated(Table)[0]
        logger.info(f"Result: {result}")
    time_it(update_table)

    def purge_table():
        response = client.asset.purge_by_guid(guid)
        result = response.assets_deleted(Table)[0]
        logger.info(f"Result: {result}")
    time_it(purge_table)


if __name__ == '__main__':
    main()
