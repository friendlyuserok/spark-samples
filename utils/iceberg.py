from typing import List

from pyspark.sql import SparkSession


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


def add_partition_fields(spark_session: SparkSession, table_fullname: str, partitions: List[str]):
    partitions_ddl = get_partitions_ddl(
        partitions
    )

    for partition in partitions_ddl:
        spark_session.sql(
            f'ALTER TABLE {table_fullname} ADD PARTITION FIELD {partition}'
        )
