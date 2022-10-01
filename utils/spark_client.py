"""Spark client."""
from typing import Dict, Optional

from pyspark import SparkConf
from pyspark.sql import DataFrame, SparkSession


class SparkClient:
    def __init__(self, configs: Dict[str, str] = {}) -> None:
        self._session: Optional[SparkSession] = None
        self._configs = configs

    @property
    def conn(self) -> SparkSession:
        """Gets or creates a SparkSession"""

        if not self._session:
            configs = self.build_spark_conf(self._configs)
            self._session = SparkSession \
                .builder \
                .config(conf=configs) \
                .enableHiveSupport() \
                .getOrCreate()

        return self._session

    def sql(self, query: str) -> DataFrame:
        """Runs a query using Spark SQL"""
        return self.conn.sql(query)

    def build_spark_conf(self, configs):
        spark_configs = []
        return SparkConf().setAll(list(configs.items()) + spark_configs)
