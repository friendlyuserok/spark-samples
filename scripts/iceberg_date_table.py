"""
How to run

PYTHONPATH=$PWD spark-submit --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.iceberg.type=hadoop \
    --conf spark.sql.catalog.iceberg.warehouse=/tmp/warehouse \
    --conf spark.sql.defaultCatalog=iceberg \
    /app/scripts/iceberg_date_table.py
"""

from random import randint
from uuid import uuid4
from datetime import datetime

from utils.iceberg import add_partition_fields
from utils.spark_client import SparkClient

DATASET_SIZE = 10000
TABLE_FULLNAME = 'iceberg.sample.date_partitioned'


def main():
    """
    See official documentation: https://iceberg.apache.org/docs/latest/spark-writes/#writing-to-partitioned-tables
    """
    # creates spark session
    client = SparkClient()
    spark = client.conn

    columns = ['id', 'name', 'born_date']

    data = [
        (
            i, str(uuid4()), datetime(randint(2000, 2022), 1, 1)
        )
        for i in range(DATASET_SIZE)]

    # creates dataframe
    dataframe = spark.createDataFrame(data, columns)
    dataframe.show(truncate=False)

    # prepare and write to local storage
    dataframe \
        .limit(0) \
        .writeTo(TABLE_FULLNAME) \
        .tableProperty('format-version', '2') \
        .createOrReplace()

    add_partition_fields(spark, TABLE_FULLNAME, ['born_date:years'])

    dataframe \
        .writeTo(TABLE_FULLNAME) \
        .append()

    spark.stop()


if __name__ == '__main__':
    main()

    """
    Output
    root@******:/app# ls /tmp/warehouse/sample/date_partitioned/data/ -l
    total 92
    drwxr-xr-x 2 root root 4096 Oct  1 22:29 'born_date_year=2000'
    drwxr-xr-x 2 root root 4096 Oct  1 22:29 'born_date_year=2001'
    drwxr-xr-x 2 root root 4096 Oct  1 22:29 'born_date_year=2002'
    drwxr-xr-x 2 root root 4096 Oct  1 22:29 'born_date_year=2003'
    drwxr-xr-x 2 root root 4096 Oct  1 22:29 'born_date_year=2004'
    drwxr-xr-x 2 root root 4096 Oct  1 22:29 'born_date_year=2005'
    drwxr-xr-x 2 root root 4096 Oct  1 22:29 'born_date_year=2006'
    drwxr-xr-x 2 root root 4096 Oct  1 22:29 'born_date_year=2007'
    drwxr-xr-x 2 root root 4096 Oct  1 22:29 'born_date_year=2008'
    drwxr-xr-x 2 root root 4096 Oct  1 22:29 'born_date_year=2009'
    drwxr-xr-x 2 root root 4096 Oct  1 22:29 'born_date_year=2010'
    drwxr-xr-x 2 root root 4096 Oct  1 22:29 'born_date_year=2011'
    drwxr-xr-x 2 root root 4096 Oct  1 22:29 'born_date_year=2012'
    drwxr-xr-x 2 root root 4096 Oct  1 22:29 'born_date_year=2013'
    drwxr-xr-x 2 root root 4096 Oct  1 22:29 'born_date_year=2014'
    drwxr-xr-x 2 root root 4096 Oct  1 22:29 'born_date_year=2015'
    drwxr-xr-x 2 root root 4096 Oct  1 22:29 'born_date_year=2016'
    drwxr-xr-x 2 root root 4096 Oct  1 22:29 'born_date_year=2017'
    drwxr-xr-x 2 root root 4096 Oct  1 22:29 'born_date_year=2018'
    drwxr-xr-x 2 root root 4096 Oct  1 22:29 'born_date_year=2019'
    drwxr-xr-x 2 root root 4096 Oct  1 22:29 'born_date_year=2020'
    drwxr-xr-x 2 root root 4096 Oct  1 22:29 'born_date_year=2021'
    drwxr-xr-x 2 root root 4096 Oct  1 22:29 'born_date_year=2022'
    """
