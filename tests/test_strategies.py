from pyspark import SparkConf, sql
from psycopg2 import connect, extras

from etl_engine import config, strategies, io_adapters


# noinspection SqlDialectInspection
class TestSnapshotStrategy:
    @staticmethod
    def setup():
        #     - *dbname*: the database name
        #     - *database*: the database name (only as keyword argument)
        #     - *user*: user name used to authenticate
        #     - *password*: password used to authenticate
        #     - *host*: database host address (defaults to UNIX socket if not provided)
        #     - *port*: connection port number (defaults to 5432 if not provided)
        with connect(
            host='postgres',
            database='postgres',
            user='postgres',
            password='postgres',
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("drop table if exists test_snapshot_strategy")

                cur.execute("""
                    create table test_snapshot_strategy (
                        id int primary key,
                        name varchar(255)
                    )
                """)

                extras.execute_values(
                    cur,
                    "insert into test_snapshot_strategy(id, name) values %s",
                    [(i, f'Number: {i}') for i in range(1, 10000 + 1)],
                    page_size=1000,
                )

    def test_all(self):
        conf = config.ETLConf(
            spark={
                'read': {
                    'format': 'jdbc',
                    'options': {
                        'url': 'jdbc:postgresql://postgres:5432/postgres',
                        'isolationLevel': 'READ_COMMITTED',
                        'user': 'postgres',
                        'password': 'postgres',
                        'driver': 'org.postgresql.Driver',
                    },
                },
                'write': {
                    'mode': 'overwrite',
                    'format': 'parquet',
                },
                'conf': [
                    ('spark.app.name', __name__),
                    ('spark.master', 'local[2]'),
                    ('spark.jars', 'https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.19/postgresql-42.2.19.jar'),
                ],
            },
            etl={
                'source': {
                    'schema': 'public',
                    'name': 'test_snapshot_strategy',
                },
                'target': {
                    'location': 'hdfs://hpdc:8020/test_snapshot_strategy.parquet',
                },
            },
        )

        # TODO: make configuration via conf optional.
        # TODO: make so etl conf can be instantiated with validate=True|False
        # TODO: make more a unified interface for readers and writers (spark session..)
        spark_conf = SparkConf().setAll(conf.spark.conf)
        with sql.SparkSession.builder.config(conf=spark_conf).getOrCreate() as spark:
            reader = io_adapters.JDBCReader()
            writer = io_adapters.HDFSWriter(spark)
            strategy = strategies.SnapshotETLStrategy(reader, writer)
            strategy.load(spark, conf)

            df = spark.read.load(path=conf.etl.target.location).cache()
            assert df.count() == 10000
            assert df.agg({'id': 'max'}).take(1)[0][0] == 10000
            assert df.agg({'id': 'min'}).take(1)[0][0] == 1
