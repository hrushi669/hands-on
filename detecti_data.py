import pandas as pd
from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import Asset, Table
from pyatlan.model.enums import AtlanConnectorType, LineageDirection, AnnouncementType
from pyatlan.model.fluent_search import FluentSearch
from pyatlan.model.lineage import FluentLineage
from pyatlan.model.core import Announcement
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
client = AtlanClient()
connection = client.asset.find_connections_by_name("hrushikesh-dokala", AtlanConnectorType.MSSQL)[0]


def load_csv(file_path):
    df = pd.read_csv(file_path)
    schema_table_split = df['Table'].str.split('.', expand=True)
    df['Database'], df['Schema'], df['TableName'] = schema_table_split[0], schema_table_split[1], schema_table_split[2]
    df.drop(columns=['Table'], inplace=True)
    df[['Overall score', 'Accuracy', 'Completeness', 'Uniqueness', 'Validity', 'Rows scanned']] = df[['Overall score', 'Accuracy', 'Completeness', 'Uniqueness', 'Validity', 'Rows scanned']].apply(pd.to_numeric, errors='coerce').fillna(0)
    return df


def search_and_update(df):
    """Search for tables and update custom metadata based on data quality scores."""
    for _, row in df.iterrows():
        db_name, schema_name, table_name = row['Database'], row['Schema'], row['TableName']
        
        request = FluentSearch(
            wheres=[
                Asset.TYPE_NAME.eq("Table"),
                Table.QUALIFIED_NAME.startswith(connection.qualified_name),
                Table.DATABASE_NAME.eq(db_name, True),
                Table.SCHEMA_NAME.eq(schema_name, True),
                Table.NAME.eq(table_name, True)
            ]
        ).to_request()
        
        response = client.asset.search(request)
        tables = list(response.current_page())
        
        if not tables:
            logger.warning(f"No table found for {db_name}.{schema_name}.{table_name}")
            continue
        
        table = tables[0]
        logger.info(f"Updating metadata for table: {table.qualified_name}")
        update = table.trim_to_required()
        cma = update.get_custom_metadata("DetectiData Table Tracker")
        
        cma["Trust Score"] = row["Overall score"]
        cma["Accuracy"] = row["Accuracy"]
        cma["Completeness"] = row["Completeness"]
        cma["Uniqueness"] = row["Uniqueness"]
        cma["Validity"] = row["Validity"]
        cma["Rows scanned"] = row["Rows scanned"]
        
        assign_announcement(table, row["Overall score"]) # this will assign the annoucement type based on the trust score
        
        response = client.asset.save_merging_cm(update)
        logger.info(f"Metadata and announcement updated for {table.qualified_name}")


def assign_announcement(asset, score):
    """
    Apply an announcement based on the trust score.
    """
    if score < 50:
        announcement_type, message = AnnouncementType.ISSUE, "Critical issue: Trust score below 50."
    elif score < 80:
        announcement_type, message = AnnouncementType.WARNING, "Warning: Trust score below 80."
    else:
        announcement_type, message = AnnouncementType.INFORMATION, "Trust score is acceptable."
    
    asset.set_announcement(Announcement(
        announcement_type=announcement_type,
        announcement_title="Data Quality Alert",
        announcement_message=message
    ))
    logger.info(f"Set {announcement_type} for {asset.qualified_name} due to trust score {score}")


def traverse_and_log(df):
    """
    Traverse lineage for each table and log downstream lineage.
    """
    for _, row in df.iterrows():
        db_name, schema_name, table_name = row['Database'], row['Schema'], row['TableName']
        
        request = FluentSearch(
            wheres=[
                Asset.TYPE_NAME.eq("Table"),
                Table.QUALIFIED_NAME.startswith(connection.qualified_name),
                Table.DATABASE_NAME.eq(db_name, True),
                Table.SCHEMA_NAME.eq(schema_name, True),
                Table.NAME.eq(table_name, True)
            ]
        ).to_request()
        
        response = client.asset.search(request)
        tables = list(response.current_page())

        starting_guid = tables[0].guid # the parent table in the lineage prop
        downstream_lineage(starting_guid, row["Overall score"])


def downstream_lineage(starting_guid, score):
    request = FluentLineage(
        starting_guid=starting_guid,
        direction=LineageDirection.DOWNSTREAM,
        where_assets=[FluentLineage.ACTIVE],
        includes_in_results=[Asset.TYPE_NAME.in_lineage.neq("Process")]
    ).request
    request.immediate_neighbors = True
    lineage_response = client.asset.get_lineage_list(request)
    
    for downstream_asset in lineage_response:
        assign_announcement(downstream_asset, score)


if __name__ == '__main__':
    file_path = "~/Downloads/day0.csv"
    df = load_csv(file_path)
    # search_and_update(df)
    # traverse_and_log(df)
    print(df)
