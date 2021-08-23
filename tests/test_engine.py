from pyspark import SparkConf, sql
from psycopg2 import connect, extras

from etl_engine import config, engine

POSTGRES_JAR = 'https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.19/postgresql-42.2.19.jar'


# noinspection SqlDialectInspection
class TestETLEngine:
    @staticmethod
    def setup():
        with connect(
            host='postgres',
            database='postgres',
            user='postgres',
            password='postgres',
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("drop table if exists test_etl_engine_hwm")

                cur.execute("""
                    create table test_etl_engine_hwm(
                        id int primary key,
                        name varchar(255)
                    )
                """)

                extras.execute_values(
                    cur,
                    "insert into test_etl_engine_hwm(id, name) values %s",
                    [(i, f'Number: {i}') for i in range(1, 20000 + 1)],
                    page_size=1000,
                )

    def test_all(self):
        conf = {
            'spark': {
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
                    ('spark.jars', POSTGRES_JAR),
                ],
            },
            'etl': {
                'strategy': 'hwm',
                'source': {
                    'system': 'Postgres',
                    'schema': 'public',
                    'name': 'test_etl_engine_hwm',
                    'hwm': {
                        'expression': 'id',
                        'hwm_type': 'int',
                    },
                },
                'target': {
                    'system': 'ExternalTable',
                    'location': 'hdfs://hpdc:8020/test_snapshot_strategy.parquet',
                    'schema': 'default',
                    'name': 'test_etl_engine_hwm',
                },
            },
        }
        conf = config.ETLConf(**conf)

        etl = engine.ETLEngine(conf)
        etl.execute()

        spark_conf = SparkConf().setAll(conf.spark.conf)
        with sql.SparkSession.builder.config(conf=spark_conf).getOrCreate() as spark:
            df = spark.read.load(path=conf.etl.target.location).cache()
            assert df.count() == 20000
            assert df.agg({'id': 'max'}).take(1)[0][0] == 20000
            assert df.agg({'id': 'min'}).take(1)[0][0] == 1

            assert len(conf.metastore.hwms) == 1

        with connect(
            host='postgres',
            database='postgres',
            user='postgres',
            password='postgres',
        ) as conn:
            with conn.cursor() as cur:
                extras.execute_values(
                    cur,
                    "insert into test_etl_engine_hwm(id, name) values %s",
                    [(i, f'Number: {i}') for i in range(20001, 40000 + 1)],
                    page_size=1000,
                )

        conf.spark.write.mode = 'append'

        etl = engine.ETLEngine(conf)
        etl.execute()

        spark_conf = SparkConf().setAll(conf.spark.conf)
        with sql.SparkSession.builder.config(conf=spark_conf).getOrCreate() as spark:
            df = spark.read.load(path=conf.etl.target.location).cache()
            assert df.count() == 40000
            assert df.agg({'id': 'max'}).take(1)[0][0] == 40000
            assert df.agg({'id': 'min'}).take(1)[0][0] == 1

            assert len(conf.metastore.hwms) == 2
