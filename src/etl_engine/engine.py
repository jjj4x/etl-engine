from typing import Optional
from logging import getLogger

from pyspark import SparkConf, sql

from etl_engine.config import ETLConf
from etl_engine.io_adapters import (
    HDFSReader,
    HDFSWriter,
    JDBCReader,
    JDBCWriter,
    TableReader,
    TableWriter,
    ExternalTableWriter,
)
from etl_engine.strategies import (
    SnapshotStrategy,
    HWMStrategy,
    HWMIncrementStrategy,
)

LOG = getLogger(__name__)


class ETLEngine:
    def __init__(self, conf: ETLConf):
        self.conf = conf

    def _execute(self, spark: sql.SparkSession):
        source_system_type = self.conf.etl.source.system
        if source_system_type == 'HDFS':
            reader_class = HDFSReader
        elif source_system_type == 'Table':
            reader_class = TableReader
        else:
            reader_class = JDBCReader

        target_system_type = self.conf.etl.target.system
        if target_system_type == 'HDFS':
            writer_class = HDFSWriter
        elif target_system_type == 'Table':
            writer_class = TableWriter
        elif target_system_type == 'ExternalTable':
            writer_class = ExternalTableWriter
        else:  # Any RDBMS
            writer_class = JDBCWriter

        reader = reader_class()
        writer = writer_class()

        strategy = self.conf.etl.strategy
        if strategy.name == 'snapshot':
            strategy = SnapshotStrategy(reader, writer)
        elif strategy.name == 'hwm':
            strategy = HWMStrategy(reader, writer, self.conf.metastore)
        elif strategy.name == 'hwm_increment':
            strategy = HWMIncrementStrategy(reader, writer, self.conf.metastore)
        else:
            raise ValueError(f'Unrecognized ETL strategy: {strategy.name}.')

        strategy.load(spark, self.conf)

    def execute(self, spark: Optional[sql.SparkSession] = None):
        if spark is not None:
            return self._execute(spark)

        spark_conf = SparkConf().setAll(self.conf.spark.conf)
        with sql.SparkSession.builder.config(conf=spark_conf).enableHiveSupport().getOrCreate() as spark:
            return self._execute(spark)
