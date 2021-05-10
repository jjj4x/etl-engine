from datetime import date, timedelta
from os import environ

from pyspark import SparkConf, sql
from pyspark.sql.types import LongType, StructType, StructField, StringType, IntegerType, DateType

from etl_engine import engine


class TestHDFSWriter:
    def test_write_one_column_parquet_overwrite(self):
        filename = 'hdfs://localhost:8020/test_write_one_column_parquet_overwrite.parquet'

        # Conf
        conf = engine.ETLConf()
        conf.spark.write.mode = 'overwrite'
        conf.spark.write.format = 'parquet'
        conf.spark.write.options = {'spark.sql.parquet.compression.codec': 'snappy'}
        conf.spark.conf = [('spark.app.name', __name__), ('spark.master', 'local[2]')]
        conf.etl.target.name = filename

        # Session
        spark_conf = SparkConf().setAll(conf.spark.conf)
        with sql.SparkSession.builder.config(conf=spark_conf).getOrCreate() as spark:
            df = spark.range(500).toDF("number")
            # Run
            engine.HDFSWriter().write(df, conf)

            df = spark.read.parquet(conf.etl.target.name)

            # Assert
            assert df.count() == 500
            assert df.schema[0].name == 'number'
            assert isinstance(df.schema[0].dataType, LongType)

    def test_write_many_columns_parquet_append(self):
        filename = 'hdfs://localhost:8020/test_write_many_columns_parquet_append.parquet'

        # Conf
        conf = engine.ETLConf()
        conf.spark.write.mode = 'append'
        conf.spark.write.format = 'parquet'
        conf.spark.conf = [('spark.app.name', __name__), ('spark.master', 'local[2]')]
        conf.etl.target.name = filename

        # Session
        spark_conf = SparkConf().setAll(conf.spark.conf)
        with sql.SparkSession.builder.config(conf=spark_conf).getOrCreate() as spark:
            # Cleanup
            hdfs = (
                spark._sc._jvm
                .org.apache.hadoop.fs.FileSystem
                .get(spark._sc._jsc.hadoopConfiguration())
            )
            hdfs.delete(spark._sc._jvm.org.apache.hadoop.fs.Path(filename), True)

            # Fixture
            ids = range(100)
            names = (f'Max_{i}' for i in range(100))
            dates = (date(2000, 1, 1) + timedelta(days=i) for i in range(100))
            schema = StructType([
                StructField('id', IntegerType(), nullable=False),
                StructField('name', StringType(), nullable=False),
                StructField('dt', DateType(), nullable=False),
            ])

            df = spark.createDataFrame(zip(ids, names, dates), schema=schema)
            df.createTempView('my_view')

            # First Run
            writer = engine.HDFSWriter()
            writer.write(df, conf)
            df_after_first_run = spark.read.parquet(conf.etl.target.name)

            # Assert
            assert df_after_first_run.count() == 100
            assert df_after_first_run.schema.fieldNames() == ['id', 'name', 'dt']

            # Second Run
            writer.write(df, conf)
            df_after_second_run = spark.read.parquet(conf.etl.target.name)

            # Assert
            assert df_after_second_run.count() == 200
            assert df_after_first_run.schema.fieldNames() == ['id', 'name', 'dt']

    def test_write_many_columns_parquet_partitioned_overwrite(self):
        filename = 'hdfs://localhost:8020/test_write_many_columns_parquet_partitioned_overwrite.parquet'

        # Conf
        conf = engine.ETLConf()
        conf.spark.write.mode = 'overwrite'
        conf.spark.write.format = 'parquet'
        conf.spark.write.partitionBy = 'sex'  # Partitioned
        conf.spark.conf = [('spark.app.name', __name__), ('spark.master', 'local[2]')]
        conf.etl.target.name = filename

        # Session
        spark_conf = SparkConf().setAll(conf.spark.conf)
        with sql.SparkSession.builder.config(conf=spark_conf).getOrCreate() as spark:
            # Fixture
            ids = range(100)
            sexes = ['m' for _ in range(50)] + ['f' for _ in range(50)]
            dates = (date(2000, 1, 1) + timedelta(days=i) for i in range(100))
            schema = StructType([
                StructField('id', IntegerType(), nullable=False),
                StructField('dt', DateType(), nullable=False),
                StructField('sex', StringType(), nullable=False),
            ])

            df = spark.createDataFrame(zip(ids, dates, sexes), schema=schema)

            # Run
            writer = engine.HDFSWriter()
            writer.write(df, conf)
            df_after_first_run = spark.read.parquet(conf.etl.target.name)

            # Assert
            assert df_after_first_run.count() == 100
            assert sorted(df_after_first_run.schema.fieldNames()) == sorted(['id', 'dt', 'sex'])
            assert df_after_first_run.rdd.getNumPartitions() == 2  # male/female


# noinspection SqlResolve
class TestTableWriters:
    def test_save_one_column_df_as_table(self):
        # Conf
        conf = engine.ETLConf()
        conf.spark.write.mode = 'overwrite'
        conf.spark.write.format = 'parquet'
        conf.spark.write.options = {
            'spark.sql.parquet.compression.codec': 'snappy',
            'spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation': 'true',
            'hive.metastore.uris': 'thrift://localhost:10000'
        }
        conf.spark.conf = [
            ('spark.app.name', __name__),
            ('spark.master', 'local[2]'),
        ]
        conf.etl.target.schema = 'default'
        conf.etl.target.name = 'my_table'

        # Session
        spark_conf = SparkConf().setAll(conf.spark.conf)
        with sql.SparkSession.builder.config(conf=spark_conf).enableHiveSupport().getOrCreate() as spark:
            df = spark.range(500).toDF("number")
            # Run
            engine.TableWriter().write(df, conf)

            df = spark.sql('select * from my_table')

            # Assert
            assert df.count() == 500
            assert df.schema[0].name == 'number'
            assert isinstance(df.schema[0].dataType, LongType)


# noinspection SqlResolve
class TestExternalTableWriters:
    def test_external_table(self):
        # Conf
        conf = engine.ETLConf()
        conf.spark.write.mode = 'overwrite'
        conf.spark.write.format = 'parquet'
        conf.spark.write.options = {
            'spark.sql.parquet.compression.codec': 'snappy',
            'spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation': 'true',
            'spark.sql.hive.metastore.uris': 'thrift://hpdc:10000',
            'spark.hadoop.metastore.catalog.default': 'hive',
        }
        conf.spark.conf = [
            ('spark.app.name', __name__),
            ('spark.master', 'local[2]'),
            ('spark.jars', 'https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.19/postgresql-42.2.19.jar'),
        ]
        conf.etl.target.schema = 'default'
        conf.etl.target.name = 'another_table'
        conf.etl.target.location = 'hdfs://hpdc:8020/default__my_table'

        # Session
        spark_conf = SparkConf().setAll(conf.spark.conf)
        with sql.SparkSession.builder.config(conf=spark_conf).enableHiveSupport().getOrCreate() as spark:
            ids = range(100)
            sexes = ['m' for _ in range(50)] + ['f' for _ in range(50)]
            dates = (date(2000, 1, 1) + timedelta(days=i) for i in range(100))
            schema = StructType([
                StructField('id', IntegerType(), nullable=False),
                StructField('dt', DateType(), nullable=False),
                StructField('sex', StringType(), nullable=False),
            ])

            df = spark.createDataFrame(zip(ids, dates, sexes), schema=schema)
            engine.ExternalTableWriter(spark).write(df, conf)

            pass


class TestJDBCWriter:
    def test_load_without_predefined_table(self):
        # Conf
        conf = engine.ETLConf()
        conf.etl.target.name = 'non_existent_table'
        conf.spark.write.mode = 'overwrite'
        conf.spark.write.format = 'jdbc'
        conf.spark.write.options = {
            'url': 'jdbc:postgresql://localhost:5432/postgres',
            'user': 'postgres',
            'password': 'password',
            'driver': 'org.postgresql.Driver',
        }
        conf.spark.read.options = conf.spark.write.options
        conf.spark.conf = [
            ('spark.app.name', __name__),
            ('spark.master', 'local[2]'),
            ('spark.jars', 'https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.19/postgresql-42.2.19.jar'),
        ]

        # Session
        spark_conf = SparkConf().setAll(conf.spark.conf)
        with sql.SparkSession.builder.config(conf=spark_conf).getOrCreate() as spark:
            df = spark.range(500).toDF("number")
            # Run
            engine.JDBCWriter().write(df, conf)

            df = engine.JDBCReader().read(spark, conf, query='(select count(*) qty from non_existent_table) as t')

            # Assert
            assert df.take(1)[0].qty == 500
