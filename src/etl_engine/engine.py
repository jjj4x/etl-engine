from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional, Tuple, List, Union, MutableMapping
from logging import getLogger

from pyspark import SparkConf, sql

LOG = getLogger(__name__)


@dataclass
class SourceTarget:
    schema: str = field(default='')
    name: str = field(default='')
    location: str = field(default='')
    system: str = field(default='')

    @property
    def fqdn(self):
        return f'{self.schema}.{self.name}'.strip('.')


@dataclass
class ETL:
    source: SourceTarget = field(default_factory=SourceTarget)
    target: SourceTarget = field(default_factory=SourceTarget)

    def __post_init__(self):
        if not isinstance(self.source, SourceTarget):
            self.source = SourceTarget(**self.source)
        if not isinstance(self.target, SourceTarget):
            self.target = SourceTarget(**self.target)


@dataclass
class SparkWrite:
    mode: str = field(default=None)
    format: str = field(default=None)
    partitionBy: str = field(default=None)
    options: MutableMapping[str, str] = field(default_factory=dict)


@dataclass
class SparkRead:
    options: MutableMapping[str, str] = field(default_factory=dict)
    format: str = field(default=None)
    # TODO: JDBC properties


@dataclass
class Spark:
    conf: List[Tuple[str, str]] = field(default_factory=list)
    write: SparkWrite = field(default_factory=dict)
    read: SparkRead = field(default_factory=dict)

    def __post_init__(self):
        if self.conf:
            self.conf = [tuple(k_v) for k_v in self.conf]
        if not isinstance(self.write, SparkWrite):
            self.write = SparkWrite(**self.write)
        if not isinstance(self.read, SparkRead):
            self.read = SparkRead(**self.read)


@dataclass
class ETLConf:
    spark: Spark = field(default_factory=Spark)
    etl: ETL = field(default_factory=ETL)

    def __post_init__(self):
        if not isinstance(self.spark, Spark):
            self.spark = Spark(**self.spark)

    @classmethod
    def from_yaml(cls):
        ...  # TODO

    @classmethod
    def from_mapping(cls):
        ...  # TODO


class ReaderABC(ABC):
    reader_type = NotImplemented

    @abstractmethod
    def read(self, spark: sql.SparkSession, conf: ETLConf, query: Optional[str] = None) -> sql.DataFrame:
        """Reads data source and returns DataFrame."""


class WriterABC(ABC):
    def __init__(self, spark: sql.SparkSession):
        self.spark = spark

    @abstractmethod
    def write(self, df: sql.DataFrame, conf: ETLConf):
        """Writes DataFrame to data target."""


class JDBCReader(ReaderABC):
    reader_type = 'relational'

    def read(self, spark: sql.SparkSession, conf: ETLConf, query: Optional[str] = None):
        if not query:
            raise ValueError('JDBCReader requires a query.')

        return (
            spark
            .read
            .format('jdbc')
            .option('dbtable', query)
            .options(**conf.spark.read.options)
            .load()
        )


class HDFSWriter(WriterABC):
    def write(self, df: sql.DataFrame, conf: ETLConf):
        df_writer = df.write

        if conf.spark.write.partitionBy:
            df_writer = df_writer.partitionBy(conf.spark.write.partitionBy)

        return (
            df_writer
            .format(conf.spark.write.format)  # "orc"
            .mode(conf.spark.write.mode)  # "append"
            .options(**conf.spark.write.options)  # (("orc.dictionary.key.threshold", "1.0"),)
            .save(conf.etl.target.location)  # "hdfs://localhost:8020/users_with_options.orc"
        )


class TableWriter(WriterABC):
    def write(self, df: sql.DataFrame, conf: ETLConf):
        df_writer = df.write

        if conf.spark.write.partitionBy:
            df_writer = df_writer.partitionBy(conf.spark.write.partitionBy)

        return (
            df_writer
            .format(conf.spark.write.format)  # "orc"
            .mode(conf.spark.write.mode)  # "append"
            .options(**conf.spark.write.options)  # (("orc.dictionary.key.threshold", "1.0"),)
            .saveAsTable(conf.etl.target.fqdn)  # "my_table"
        )


class ExternalTableWriter(WriterABC):
    def write(self, df: sql.DataFrame, conf: ETLConf):
        HDFSWriter(self.spark).write(df, conf)

        create_table = '\n'.join([
            'create external table if not exists {fqdn} (',
            '{columns}',
            ')',
            'stored as parquet',
            'location "{location}"',
        ])
        columns = []
        for column_name, column_type in df.dtypes:
            columns.append(f'{column_name} {column_type}')

        query = create_table.format(
            fqdn=conf.etl.target.fqdn,
            columns='\n,'.join(columns),
            location=conf.etl.target.location,
        )

        if conf.spark.write.partitionBy:
            query += f'partition by {conf.spark.write.partitionBy}'

        self.spark.sql(query)
        if conf.spark.write.partitionBy:
            self.spark.sql(f'msck repair table {conf.etl.target.fqdn}')


class HDFSReader(ReaderABC):
    reader_type = 'filesystem'

    def read(self, spark: sql.SparkSession, conf: ETLConf, query: Optional[str] = None) -> sql.DataFrame:
        return (
            spark
            .read
            .load(path=conf.etl.source.name, format=conf.spark.read.format, **conf.spark.read.options)
        )


class TableReader(ReaderABC):
    reader_type = 'relational'

    ...  # TODO


class JDBCWriter(WriterABC):

    def write(self, df: sql.DataFrame, conf: ETLConf):
        df_writer = df.write

        return (
            df_writer
            .format('jdbc')
            .mode(conf.spark.write.mode)
            .option('dbtable', conf.etl.target.fqdn)
            .options(**conf.spark.write.options)
            .save()
        )


class SnapshotETLStrategy:
    def __init__(self, reader: ReaderABC, writer: WriterABC):
        self.reader = reader
        self.writer = writer

    def load(self, spark: sql.SparkSession, conf: ETLConf):
        statements = [
            'select ',
            conf.etl.columns,
            'from',
            conf.etl.source.fqdn,
        ]
        if conf.etl.where:
            statements.append(f'where {conf.etl.where}')

        query = ' '.join(statements)

        LOG.info('Running query: {0}.', query)

        if self.reader.reader_type == 'relational':
            df = self.reader.read(spark, conf, query)
        else:
            raise ValueError('Non relational reader_types are not supported yet.')

        self.writer.write(df, conf)

        ...  # TODO: return count and statistics


class ETL:
    def __init__(self, conf: ETLConf):
        self.conf = conf

    def _execute(self, spark: sql.SparkSession):
        # FIXME: maybe configure with spark?
        source_type = self.conf.etl.source.type
        if source_type == 'HDFS':
            reader = HDFSReader
        elif source_type == 'Hive':
            reader = HiveReader
        else:
            reader = JDBCReader

        target_type = self.conf.etl.target.type
        if target_type == 'HDFS':
            writer = HDFSWriter
        elif target_type == 'Hive':
            writer = HiveWriter
        else:
            writer = JDBCWriter

        strategy = self.conf.etl.strategy
        if strategy == 'snapshot':
            strategy = SnapshotETLStrategy(reader(), writer())
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
