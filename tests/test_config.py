from datetime import date, datetime

from pytest import raises

from etl_engine import config


class TestHWM:
    def test_hwm(self):
        with raises(ValueError):
            config.HWM('user_id', value='Max', literal_value=1989)

        hwm = config.HWM('coalesce(account_gid, account_number)', None, None, 'int')
        assert not hwm.is_unset
        assert hwm.literal_value == 0
        assert hwm.value == '0'

        hwm.literal_value = 100
        assert hwm.literal_value == 100
        assert hwm.value == '100'
        assert hwm.predicate_right == 'coalesce(account_gid, account_number) <= 100'

        hwm = config.HWM.from_literal('user_id', None, hwm_type='str')
        assert hwm.value is ''
        assert hwm.literal_value is ''
        assert hwm.hwm_type == 'str'

        hwm = config.HWM.from_literal('user_id', 100)
        assert hwm.value == '100'
        assert hwm.literal_value == 100
        assert hwm.hwm_type == 'int'

        hwm = config.HWM.from_literal('business_date', date(2020, 1, 1))
        assert hwm.value == '"2020-01-01T00:00:00"'
        assert hwm.hwm_type == 'datetime'

        hwm = config.HWM.from_mapping(**hwm.as_mapping())
        assert hwm.literal_value == datetime(2020, 1, 1)
        assert hwm.value == '"2020-01-01T00:00:00"'
        assert hwm.hwm_type == 'datetime'


class TestConfig:
    def test_source_target(self):
        with raises(ValueError):
            config.SourceTarget().validate()

    def test_etl_with_hwm(self):
        etl = config.ETL(
            source={
                'schema': 'public',
                'name': 'account_balance',
                'system': 'Postgres',
                'columns': [],
                'where': 'is_technical = false and account_gid is not null',
                'hwm': {
                    'expression': 'coalesce(account_gid, account_num)',
                    'hwm_type': 'int',
                },
            },
            target={
                'location': 'hive/warehouse/raw/account_balance/',
                'system': 'HDFS',
            },
            strategy='increment',
        )
        assert etl.source.hwm.value == '0'
        assert etl.source.hwm.literal_value == 0
        assert etl.source.hwm.predicate_left == '0 < coalesce(account_gid, account_num)'

        etl.source.hwm.literal_value = 1000
        assert etl.source.hwm.predicate_right == 'coalesce(account_gid, account_num) <= 1000'

        etl.source.hwm.hwm_type = 'datetime'
        etl.source.hwm.literal_value = date(2020, 1, 1)
        assert etl.source.hwm.predicate_right == 'coalesce(account_gid, account_num) <= "2020-01-01T00:00:00"'

        etl.source.hwm.literal_value = None
        etl.source.hwm.initialize()
        assert etl.source.hwm.predicate_left == '"1900-01-01T00:00:00" < coalesce(account_gid, account_num)'

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
