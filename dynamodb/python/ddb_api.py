import logging
from pprint import pprint

import boto3
from boto3.dynamodb.conditions import Attr, Key

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ddb_client = boto3.resource("dynamodb")

TABLE_NAME = "recognition_metadata"


def table_exists(table):
    try:
        ddb_table = ddb_client.Table(table)
        return ddb_table

    except Exception as e:
        raise e


def get_data(ddb_table, data_source, joint_type, metadata_key, metadata_value):
    try:
        if metadata_key and metadata_value:
            response = ddb_table.query(
                KeyConditionExpression=Key("data_source").eq(data_source)
                & Key("joint_type#survey_id").begins_with(joint_type),
                FilterExpression=Attr(f"metadata.{metadata_key}").eq(
                    f"{metadata_value}"
                ),
            )

        else:
            response = ddb_table.query(
                KeyConditionExpression=Key("data_source").eq(data_source)
                & Key("joint_type#survey_id").begins_with(joint_type)
            )

        return response["Items"]

    except Exception as e:
        raise e


if __name__ == "__main__":
    ddb_table = table_exists(TABLE_NAME)

    if ddb_table:
        # prompts to filter metadata information
        data_source = input("Enter the data collection device type:")

        joint_type = input("Enter the joint type:")

        metadata_key = input("Enter metadata key to filter from:") or None

        metadata_value = input("Enter metadata value for filter:") or None

        response = get_data(
            ddb_table, data_source, joint_type, metadata_key, metadata_value
        )

        pprint(response)
