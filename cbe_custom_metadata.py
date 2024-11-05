# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Atlan Pte. Ltd.
import logging

from pyatlan.client.asset import Batch
from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import Asset, Table
from pyatlan.model.core import Announcement
from pyatlan.model.enums import AtlanConnectorType, AnnouncementType
from pyatlan.model.fluent_search import FluentSearch

from cbe_common import time_it

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    client = AtlanClient()

    # TODO: replace with YOUR connection's name
    connection = client.asset.find_connections_by_name("hrushikesh-dokala", AtlanConnectorType.MSSQL)[0]

    def search_and_update():
        request = FluentSearch(
            wheres=[
                Asset.TYPE_NAME.eq("Table"),
                Table.QUALIFIED_NAME.startswith(connection.qualified_name),
                Table.DATABASE_NAME.eq("wwi", True),
                Table.SCHEMA_NAME.eq("sales", True),
                Table.NAME.eq("invoices", True)
            ]
        ).to_request()
        tables = client.asset.search(request)

        for table in tables:
            update = table.trim_to_required()
            cma = update.get_custom_metadata("Cost")
            cma["List"] = 100
            cma["Discount"] = 10
            cma["Total"] = 90
            response = client.asset.save_merging_cm(update)
            result = response.assets_updated(Table)[0]
            logger.info(f"Result: {result}")
    time_it(search_and_update)

    def direct_update():
        update = Table.updater(f"{connection.qualified_name}/wwi/Sales/Invoices", "Invoices")
        cma = update.get_custom_metadata("Cost")
        cma["List"] = 1000
        cma["Discount"] = 20
        cma["Total"] = 800
        response = client.asset.save_merging_cm(update)
        result = response.assets_updated(Table)[0]
        logger.info(f"Result: {result}")
    time_it(direct_update)

    def batch_update():
        batch = Batch(client.asset, 20) # this limits the updates to 20 and then complete the API request, empties the queue
        update1 = Table.updater(
            f"{connection.qualified_name}/wwi/Sales/InvoiceLines",
            "InvoiceLines"
        )
        update1.set_announcement(Announcement(
            announcement_type=AnnouncementType.INFORMATION,
            announcement_title="Part of a batch",
            announcement_message="This was added as part of a batch of updates (via Python)."
        ))
        update2 = Table.updater(
            f"{connection.qualified_name}/wwi/Application/DeliveryMethods",
            "DeliveryMethods"
        )
        update2.set_announcement(Announcement(
            announcement_type=AnnouncementType.INFORMATION,
            announcement_title="Part of a batch",
            announcement_message="This was added as part of a batch of updates (via Python)."
        ))
        update3 = Table.updater(
            f"{connection.qualified_name}/wwi/Warehouse/ColdRoomTemperatures",
            "ColdRoomTemperatures"
        )
        update3.set_announcement(Announcement(
            announcement_type=AnnouncementType.INFORMATION,
            announcement_title="Part of a batch",
            announcement_message="This was added as part of a batch of updates (via Python)."
        ))
        batch.add(update1)
        batch.add(update2)
        batch.add(update3)
        batch.flush()
        logger.info(f"Total updated: {len(batch.updated)}")
    time_it(batch_update)


if __name__ == '__main__':
    main()
