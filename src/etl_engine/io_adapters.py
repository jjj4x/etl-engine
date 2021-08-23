from abc import ABC, abstractmethod
from typing import Optional

from pyspark import sql

from etl_engine.config import ETLConf


class ReaderABC(ABC):
    reader_type = NotImplemented

    @abstractmethod
    def read(self, spark: sql.SparkSession, conf: ETLConf, query: Optional[str] = None) -> sql.DataFrame:
        """Reads data source and returns DataFrame."""


class WriterABC(ABC):
    @abstractmethod
    def write(self, df: sql.DataFrame, conf: ETLConf, spark: Optional[sql.SparkSession] = None):
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
            .option('dbtable', f'({query}) as query')
            .options(**conf.spark.read.options)
            .load()
        )


class HDFSWriter(WriterABC):
    def write(self, df: sql.DataFrame, conf: ETLConf, spark: Optional[sql.SparkSession] = None):
        df_writer = df.write

        if conf.spark.write.partitionBy:
            df_writer = df_writer.partitionBy(conf.spark.write.partitionBy)

        return (
            df_writer
            .format(conf.spark.write.format)  # "orc"
            .mode(conf.spark.write.mode)  # "append"
            .options(**conf.spark.write.options)  # (("orc.dictionary.expression.threshold", "1.0"),)
            .save(conf.etl.target.location)  # "hdfs://localhost:8020/users_with_options.orc"
        )


class TableWriter(WriterABC):
    def write(self, df: sql.DataFrame, conf: ETLConf, spark: Optional[sql.SparkSession] = None):
        df_writer = df.write

        if conf.spark.write.partitionBy:
            df_writer = df_writer.partitionBy(conf.spark.write.partitionBy)

        return (
            df_writer
            .format(conf.spark.write.format)  # "orc"
            .mode(conf.spark.write.mode)  # "append"
            .options(**conf.spark.write.options)  # (("orc.dictionary.expression.threshold", "1.0"),)
            .saveAsTable(conf.etl.target.fqdn)  # "my_table"
        )


class ExternalTableWriter(WriterABC):
    def write(self, df: sql.DataFrame, conf: ETLConf, spark: Optional[sql.SparkSession] = None):
        HDFSWriter().write(df, conf)

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

        spark.sql(query)
        if conf.spark.write.partitionBy:
            spark.sql(f'msck repair table {conf.etl.target.fqdn}')


class HDFSReader(ReaderABC):
    reader_type = 'filesystem'

    def read(self, spark: sql.SparkSession, conf: ETLConf, query: Optional[str] = None) -> sql.DataFrame:
        return (
            spark
            .read
            .load(path=conf.etl.source.location, format=conf.spark.read.format, **conf.spark.read.options)
        )


class TableReader(ReaderABC):
    reader_type = 'relational'

    def read(self, spark: sql.SparkSession, conf: ETLConf, query: Optional[str] = None) -> sql.DataFrame:
        return spark.sql(query)


class JDBCWriter(WriterABC):

    def write(self, df: sql.DataFrame, conf: ETLConf, spark: Optional[sql.SparkSession] = None):
        df_writer = df.write

        return (
            df_writer
            .format('jdbc')
            .mode(conf.spark.write.mode)
            .option('dbtable', conf.etl.target.fqdn)
            .options(**conf.spark.write.options)
            .save()
        )
