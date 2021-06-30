from pytest import raises

from etl_engine import config


class TestConfig:
    def test_source_target(self):
        with raises(ValueError):
            config.SourceTarget().validate()

    def test_etl(self):
        etl = config.ETL(
            source={'schema': 'public', 'name': 'users'},
            target={'location': '/tmp/users'},
            strategy='snapshot',
        )
        assert isinstance(etl.source, config.SourceTarget)
        assert isinstance(etl.target, config.SourceTarget)

    def test_spark(self):
        spark = config.Spark(
            conf=[('spark.app.name', 'App'), ('spark.master', 'yarn')],
            read={
                'format': 'jdbc',
                'options': {
                    'url': 'jdbc:postgresql://postgres:5432/postgres',
                    'fetchsize': '100',
                    'isolationLevel': 'READ_COMMITTED',
                    'sessionInitStatement': 'select 300',
                    'user': 'postgres',
                    'password': 'postgres',
                    'driver': 'org.postgresql.Driver',
                },
            },
            write={
                'format': 'parquet',
                'mode': 'overwrite',
                'options': {
                    'spark.sql.parquet.compression.codec': 'snappy',
                },
            },
        )
        assert isinstance(spark.read, config.SparkRead)
        assert isinstance(spark.write, config.SparkWrite)
        assert spark.read.format == 'jdbc'
        assert spark.read.options['user'] == 'postgres'
        assert spark.write.format == 'parquet'
        assert spark.write.options

    def test_etl_conf_plain(self):
        conf = config.ETLConf(
            spark={
                'conf': [('spark.app.name', 'App'), ('spark.master', 'yarn')],
                'read': {
                    'format': 'jdbc',
                    'options': {
                        'url': 'jdbc:postgresql://postgres:5432/postgres',
                        'fetchsize': '100',
                        'isolationLevel': 'READ_COMMITTED',
                        'sessionInitStatement': 'select 300',
                        'user': 'postgres',
                        'password': 'postgres',
                        'driver': 'org.postgresql.Driver',
                    },
                },
                'write': {
                    'format': 'parquet',
                    'mode': 'overwrite',
                    'options': {
                        'spark.sql.parquet.compression.codec': 'snappy',
                    },
                },
            },
            etl=config.ETL(
                source={'schema': 'public', 'name': 'users'},
                target={'location': '/tmp/users'},
                strategy='snapshot',
            ),
        )
        assert isinstance(conf.etl, config.ETL)
        assert isinstance(conf.spark, config.Spark)
        assert isinstance(conf.spark.write, config.SparkWrite)
        assert isinstance(conf.spark.read, config.SparkRead)
