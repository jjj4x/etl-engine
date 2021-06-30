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
from strategies import (
    SnapshotETLStrategy,
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
        else:
            writer_class = JDBCWriter

        reader = reader_class()
        writer = writer_class(spark)

        strategy = self.conf.etl.strategy
        if strategy == 'snapshot':
            strategy = SnapshotETLStrategy(reader, writer)
            ...  # TODO
        elif strategy == 'snapshot_use_checkpoints':
            ...  # TODO
        elif strategy == 'snapshot_use_watermarks':
            ...  # TODO
        elif strategy == 'snapshot_checkpointed_with_watermark':
            ...  # TODO
        elif strategy == 'discrete_interval':
            ...  # TODO
        elif strategy == 'discrete_interval_checkpointed':
            ...  # TODO
        elif strategy == 'increment_':
            ...  # TODO
        elif strategy == 'watermark_checkpointed':
            ...  # TODO
        else:
            raise ValueError(f'Unrecognized ETL strategy: {strategy}.')

        strategy.load(spark, self.conf)

    def execute(self, spark: Optional[sql.SparkSession] = None):
        if spark is not None:
            return self._execute(spark)

        spark_conf = SparkConf().setAll(self.conf.spark.conf)
        with sql.SparkSession.builder.config(conf=spark_conf).getOrCreate() as spark:
            return self._execute(spark)
