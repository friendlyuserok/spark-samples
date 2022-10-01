"""
How to run

PYTHONPATH=$PWD spark-submit --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.iceberg.type=hadoop \
    --conf spark.sql.catalog.iceberg.warehouse=/tmp/warehouse \
    --conf spark.sql.defaultCatalog=iceberg \
    /app/scripts/iceberg_table.py
"""

from random import randint
from typing import List
from uuid import uuid4

from utils.spark_client import SparkClient

DATASET_SIZE = 10000
TABLE_FULLNAME = 'iceberg.sample.partitioned'


def get_partitions_ddl(partitions: List[str]) -> List[str]:
    """
    Returns partition definition statement.
    """
    if len(partitions) < 1:
        return []

    formatted_partitions = []

    for partition in partitions:
        split = partition.split(':')
        partition_name = split[0]
        partition_transform = None

        if len(split) > 1:
            partition_transform = split[1]
            partition_ddl = f"{partition_transform}({partition_name})"
            formatted_partitions.append(partition_ddl)
            continue

        formatted_partitions.append(partition_name)

    return formatted_partitions


def add_partition_fields(spark_session, partitions: List[str]):
    partitions_ddl = get_partitions_ddl(
        partitions
    )

    for partition in partitions_ddl:
        spark_session.sql(
            f'ALTER TABLE {TABLE_FULLNAME} ADD PARTITION FIELD {partition}'
        )


def main():
    # creates spark session
    client = SparkClient()
    spark = client.conn

    # data layout
    # id: long
    # name: string - uuid
    # country: string - enum ['brazil', 'usa', 'germany', 'france']

    columns = ['id', 'name', 'country']
    countries = ['brazil', 'usa', 'germany', 'france']

    data = [
        (
            i, str(uuid4()), countries[randint(0, len(countries) - 1)]
        )
        for i in range(DATASET_SIZE)]

    # creates dataframe
    dataframe = spark.createDataFrame(data, columns)
    dataframe.show(truncate=False)

    # prepare and write to local storage partitioned by country column
    dataframe \
        .limit(0) \
        .writeTo(TABLE_FULLNAME) \
        .tableProperty('format-version', '2') \
        .createOrReplace()

    add_partition_fields(spark, ['country'])

    dataframe \
        .repartition(4, 'country') \
        .writeTo(TABLE_FULLNAME) \
        .append()

    spark.stop()


if __name__ == '__main__':
    main()
