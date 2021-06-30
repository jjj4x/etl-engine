from logging import getLogger

from pyspark import sql

from etl_engine.config import ETLConf
from etl_engine.dialects import Query
from etl_engine.io_adapters import (
    ReaderABC,
    WriterABC,
)

LOG = getLogger(__name__)


class SnapshotETLStrategy:
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

        self.writer.write(df, conf)

        ...  # TODO: return count and statistics
