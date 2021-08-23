from datetime import date, timedelta
from os import environ

from pyspark import SparkConf, sql
from pyspark.sql.types import LongType, StructType, StructField, StringType, IntegerType, DateType

from etl_engine import io_adapters


# noinspection PyUnresolvedReferences
class TestHDFSWriter:
    def test_write_one_column_parquet_overwrite(self):
        filename = 'hdfs://hpdc:8020/test_write_one_column_parquet_overwrite.parquet'

        # Conf
        conf = io_adapters.ETLConf()
        conf.spark.write.mode = 'overwrite'
        conf.spark.write.format = 'parquet'
        conf.spark.write.options = {'spark.sql.parquet.compression.codec': 'snappy'}
        conf.spark.conf = [
            ('spark.app.name', __name__),
            ('spark.master', 'local[2]'),
            ('spark.jars', 'https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.19/postgresql-42.2.19.jar'),
        ]
        conf.etl.target.location = filename

        # Session
        spark_conf = SparkConf().setAll(conf.spark.conf)
        with sql.SparkSession.builder.config(conf=spark_conf).getOrCreate() as spark:
            df = spark.range(500).toDF("number")
            # Run
            io_adapters.HDFSWriter().write(df, conf)

            df = spark.read.parquet(conf.etl.target.location)

            # Assert
            assert df.count() == 500
            assert df.schema[0].name == 'number'
            assert isinstance(df.schema[0].dataType, LongType)

    def test_write_many_columns_parquet_append(self):
        filename = 'hdfs://hpdc:8020/test_write_many_columns_parquet_append.parquet'

        # Conf
        conf = io_adapters.ETLConf()
        conf.spark.write.mode = 'append'
        conf.spark.write.format = 'parquet'
        conf.spark.conf = [
            ('spark.app.name', __name__),
            ('spark.master', 'local[2]'),
            ('spark.jars', 'https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.19/postgresql-42.2.19.jar'),
        ]
        conf.etl.target.location = filename

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
            writer = io_adapters.HDFSWriter()
            writer.write(df, conf)
            df_after_first_run = spark.read.parquet(conf.etl.target.location)

            # Assert
            assert df_after_first_run.count() == 100
            assert df_after_first_run.schema.fieldNames() == ['id', 'name', 'dt']

            # Second Run
            writer.write(df, conf)
            df_after_second_run = spark.read.parquet(conf.etl.target.location)

            # Assert
            assert df_after_second_run.count() == 200
            assert df_after_first_run.schema.fieldNames() == ['id', 'name', 'dt']

    def test_write_many_columns_parquet_partitioned_overwrite(self):
        filename = 'hdfs://hpdc:8020/test_write_many_columns_parquet_partitioned_overwrite.parquet'

        # Conf
        conf = io_adapters.ETLConf()
        conf.spark.write.mode = 'overwrite'
        conf.spark.write.format = 'parquet'
        conf.spark.write.partitionBy = 'sex'  # Partitioned
        conf.spark.conf = [
            ('spark.app.name', __name__),
            ('spark.master', 'local[2]'),
            ('spark.jars', 'https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.19/postgresql-42.2.19.jar'),
        ]
        conf.etl.target.location = filename

        # Session
        spark_conf = SparkConf().setAll(conf.spark.conf)
        with sql.SparkSession.builder.config(conf=spark_conf).getOrCreate() as spark:
            # Fixture
            ids = range(200)
            sexes = ['m' for _ in range(100)] + ['f' for _ in range(100)]
            dates = (date(2000, 1, 1) + timedelta(days=i) for i in range(200))
            schema = StructType([
                StructField('id', IntegerType(), nullable=False),
                StructField('dt', DateType(), nullable=False),
                StructField('sex', StringType(), nullable=False),
            ])

            df = spark.createDataFrame(zip(ids, dates, sexes), schema=schema)

            # Run
            writer = io_adapters.HDFSWriter()
            writer.write(df, conf)
            df_after_first_run = spark.read.parquet(conf.etl.target.location)

            # Assert
            assert df_after_first_run.count() == 200
            assert sorted(df_after_first_run.schema.fieldNames()) == sorted(['id', 'dt', 'sex'])
            assert df_after_first_run.rdd.getNumPartitions() == 2  # male/female


# noinspection SqlResolve,SqlDialectInspection,SqlNoDataSourceInspection
class TestTableWriters:
    def test_save_one_column_df_as_table(self):
        # Conf
        conf = io_adapters.ETLConf()
        conf.spark.write.mode = 'overwrite'
        conf.spark.write.format = 'parquet'
        conf.spark.write.options = {
            'spark.sql.parquet.compression.codec': 'snappy',
            'spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation': 'true',
        }
        conf.spark.conf = [
            ('spark.app.name', __name__),
            ('spark.master', 'local[2]'),
            ('spark.jars', 'https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.19/postgresql-42.2.19.jar'),
            ('spark.sql.hive.metastore.uris', 'thrift://hpdc:9083'),  # Optional; will be inferred using hive-site.xml
            ('spark.sql.hive.metastore.version', '2.3'),  # Optional; will be inferred using hive-site.xml
            # Because hive.metastore.warehouse.dir property in hive-site.xml is deprecated since Spark 2.0.0.
            ('spark.sql.warehouse.dir', 'hdfs://hpdc:8020/hive/warehouse'),  # Optional for EXTERNAL TABLES
            ('spark.sql.catalogImplementation', 'hive'),  # Optional; will be set automatically by enableHiveSupport()
            ('spark.sql.hive.metastore.jars', f'{environ["HIVE_HOME"]}/lib/*'),  # Other options: maven, builtin
        ]
        conf.etl.target.schema = 'default'
        conf.etl.target.name = 'my_table'

        # Session
        spark_conf = SparkConf().setAll(conf.spark.conf)
        with sql.SparkSession.builder.config(conf=spark_conf).enableHiveSupport().getOrCreate() as spark:
            df = spark.range(500).toDF("number")
            # Run
            io_adapters.TableWriter().write(df, conf, spark)

            df = spark.sql('select * from my_table')

            # Assert
            assert df.count() == 500
            assert df.schema[0].name == 'number'
            assert isinstance(df.schema[0].dataType, LongType)


# noinspection SqlResolve,SqlDialectInspection,SqlNoDataSourceInspection
class TestExternalTableWriters:
    def test_external_table(self):
        # Conf
        conf = io_adapters.ETLConf()
        conf.spark.write.mode = 'overwrite'
        conf.spark.write.format = 'parquet'
        conf.spark.write.options = {
            'spark.sql.parquet.compression.codec': 'snappy',
        }
        conf.spark.conf = [
            ('spark.app.name', __name__),
            ('spark.master', 'local[2]'),
            ('spark.jars', 'https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.19/postgresql-42.2.19.jar'),
            ('spark.sql.hive.metastore.jars', f'{environ["HIVE_HOME"]}/lib/*'),  # Other options: maven, builtin
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
            io_adapters.ExternalTableWriter().write(df, conf, spark)

            df = spark.sql(f'select * from {conf.etl.target.fqdn}')
            assert df.count() == 100


class TestJDBCWriter:
    def test_load_without_predefined_table(self):
        # Conf
        conf = io_adapters.ETLConf()
        conf.etl.target.name = 'non_existent_table'
        conf.spark.write.mode = 'overwrite'
        conf.spark.write.format = 'jdbc'
        conf.spark.write.options = {
            'url': 'jdbc:postgresql://postgres:5432/postgres',
            'user': 'postgres',
            'password': 'postgres',
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
            io_adapters.JDBCWriter().write(df, conf)

            df = (
                spark
                .read
                .format('jdbc')
                .option('dbtable', '(select count(*) qty from non_existent_table) as t')
                .options(**conf.spark.read.options)
                .load()
            )

            # Assert
            assert df.take(1)[0].qty == 500
