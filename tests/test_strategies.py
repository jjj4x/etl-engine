from pyspark import SparkConf, sql
from psycopg2 import connect, extras

from etl_engine import config, strategies, io_adapters
from etl_engine.metastore import InMemoryMetastore


# noinspection SqlDialectInspection
class TestSnapshotStrategy:
    @staticmethod
    def setup():
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

        spark_conf = SparkConf().setAll(conf.spark.conf)
        with sql.SparkSession.builder.config(conf=spark_conf).getOrCreate() as spark:
            reader = io_adapters.JDBCReader()
            writer = io_adapters.HDFSWriter()
            strategy = strategies.SnapshotStrategy(reader, writer)
            strategy.load(spark, conf)

            df = spark.read.load(path=conf.etl.target.location).cache()
            assert df.count() == 10000
            assert df.agg({'id': 'max'}).take(1)[0][0] == 10000
            assert df.agg({'id': 'min'}).take(1)[0][0] == 1


# noinspection SqlDialectInspection
class TestSnapshotUseHWMStrategy:
    @staticmethod
    def setup():
        with connect(
            host='postgres',
            database='postgres',
            user='postgres',
            password='postgres',
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("drop table if exists test_snapshot_hwm_strategy")

                cur.execute("""
                    create table test_snapshot_hwm_strategy (
                        id int primary key,
                        name varchar(255)
                    )
                """)

                extras.execute_values(
                    cur,
                    "insert into test_snapshot_hwm_strategy(id, name) values %s",
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
                    'name': 'test_snapshot_hwm_strategy',
                    'hwm': {
                        'expression': 'id',
                        'hwm_type': 'int',
                    },
                },
                'target': {
                    'location': 'hdfs://hpdc:8020/test_snapshot_strategy.parquet',
                },
            },
        )

        metastore = InMemoryMetastore()
        spark_conf = SparkConf().setAll(conf.spark.conf)
        with sql.SparkSession.builder.config(conf=spark_conf).getOrCreate() as spark:
            reader = io_adapters.JDBCReader()
            writer = io_adapters.HDFSWriter()
            strategy = strategies.HWMStrategy(reader, writer, metastore)
            strategy.load(spark, conf)

            df = spark.read.load(path=conf.etl.target.location).cache()
            assert df.count() == 10000
            assert df.agg({'id': 'max'}).take(1)[0][0] == 10000
            assert df.agg({'id': 'min'}).take(1)[0][0] == 1

            assert len(metastore.hwms) == 1

            with connect(
                host='postgres',
                database='postgres',
                user='postgres',
                password='postgres',
            ) as conn:
                with conn.cursor() as cur:
                    extras.execute_values(
                        cur,
                        "insert into test_snapshot_hwm_strategy(id, name) values %s",
                        [(i, f'Number: {i}') for i in range(10001, 20000 + 1)],
                        page_size=1000,
                    )

            conf.spark.write.mode = 'append'
            strategy.load(spark, conf)

            assert df.count() == 20000
            assert df.agg({'id': 'max'}).take(1)[0][0] == 20000
            assert df.agg({'id': 'min'}).take(1)[0][0] == 1

            assert len(metastore.hwms) == 2
