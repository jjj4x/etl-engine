from logging import getLogger

from pyspark import sql

from etl_engine.config import ETLConf, HWM
from etl_engine.dialects import Query
from etl_engine.io_adapters import (
    ReaderABC,
    WriterABC,
)
from etl_engine.metastore import MetastoreABC

LOG = getLogger(__name__)


class SnapshotStrategy:
    def __init__(self, reader: ReaderABC, writer: WriterABC):
        self.reader = reader
        self.writer = writer

    def load(self, spark: sql.SparkSession, conf: ETLConf):
        query = Query(
            columns=conf.etl.source.columns,
            from_=conf.etl.source.fqdn,
            where=[conf.etl.source.where],
        )

        LOG.info('Running query: {0}.', query.sql)

        if self.reader.reader_type == 'relational':
            df = self.reader.read(spark, conf, query.sql)
        else:
            raise ValueError('Non relational reader_types are not supported yet.')

        self.writer.write(df, conf, spark)

        ...  # TODO: return count and statistics


# noinspection SqlDialectInspection
class HWMStrategy:
    def __init__(
        self,
        reader: ReaderABC,
        writer: WriterABC,
        metastore: MetastoreABC,
    ):
        self.reader = reader
        self.writer = writer
        self.metastore = metastore

    def load(self, spark: sql.SparkSession, conf: ETLConf):
        query = Query(
            columns=conf.etl.source.columns,
            from_=conf.etl.source.fqdn,
            where=[conf.etl.source.where],
        )

        old_hwm = self.metastore.hwm or conf.etl.source.hwm

        df = self.reader.read(
            spark,
            conf,
            old_hwm.max_hwm_sql(conf.etl.source.fqdn),
        )

        new_hwm = HWM.from_literal(old_hwm.expression, df.take(1)[0].new_hwm)

        if new_hwm.literal_value <= old_hwm.literal_value:
            return

        query.where.append(old_hwm.predicate_left)
        query.where.append(new_hwm.predicate_right)

        LOG.info('Running query: {0}.', query.sql)

        if self.reader.reader_type == 'relational':
            df = self.reader.read(spark, conf, query.sql)
        else:
            raise ValueError('Non relational reader_types are not supported yet.')

        self.writer.write(df, conf, spark)

        self.metastore.hwm = new_hwm

        ...  # TODO: return count and statistics


# noinspection SqlDialectInspection
class HWMIncrementStrategy:
    def __init__(
        self,
        reader: ReaderABC,
        writer: WriterABC,
        metastore: MetastoreABC,
    ):
        self.reader = reader
        self.writer = writer
        self.metastore = metastore

    def load(self, spark: sql.SparkSession, conf: ETLConf):
        query = Query(
            columns=conf.etl.source.columns,
            from_=conf.etl.source.fqdn,
            where=[conf.etl.source.where],
        )

        old_hwm = self.metastore.hwm or conf.etl.source.hwm

        new_hwm_literal_value = old_hwm.literal_value + conf.etl.strategy.increment
        new_hwm_upper_boundary = HWM.from_literal(old_hwm.expression, new_hwm_literal_value)

        # TODO: Refactor duplicates
        df = self.reader.read(
            spark,
            conf,
            old_hwm.max_hwm_sql(conf.etl.source.fqdn),
        )

        new_hwm = HWM.from_literal(old_hwm.expression, df.take(1)[0].new_hwm)

        if new_hwm.literal_value <= old_hwm.literal_value:
            return

        query.where.append(new_hwm_upper_boundary.predicate_right)

        LOG.info('Running query: {0}.', query.sql)

        if self.reader.reader_type == 'relational':
            df = self.reader.read(spark, conf, query.sql)
        else:
            raise ValueError('Non relational reader_types are not supported yet.')

        self.writer.write(df, conf, spark)

        self.metastore.hwm = new_hwm_upper_boundary

        ...  # TODO: return count and statistics
