"""
How to execute

PYTHONPATH=$PWD spark-submit /app/scripts/hive_table.py
"""

from random import randint
from uuid import uuid4

from utils.spark_client import SparkClient

DATASET_SIZE = 10000

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
    .repartition(4, 'country') \
    .write \
    .partitionBy('country') \
    .format('parquet') \
    .option('path', '/tmp/sample_partitioned') \
    .mode('overwrite') \
    .save()

spark.stop()
