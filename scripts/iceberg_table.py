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
from uuid import uuid4

from utils.iceberg import add_partition_fields
from utils.spark_client import SparkClient

DATASET_SIZE = 10000
TABLE_FULLNAME = 'iceberg.sample.partitioned'


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

    add_partition_fields(spark, TABLE_FULLNAME, ['country'])

    dataframe \
        .repartition(4, 'country') \
        .writeTo(TABLE_FULLNAME) \
        .append()

    spark.stop()


if __name__ == '__main__':
    main()
