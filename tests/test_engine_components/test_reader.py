from datetime import date, timedelta

from pyspark import SparkConf, sql
from pyspark.sql.types import StringType, StructType, StructField, LongType, IntegerType, DateType

from etl_engine import engine


class TestHDFSReader:
    def test_read_one_column_parquet(self):
        filename = 'hdfs://localhost:8020/test_read_one_column_parquet.parquet'

        # Conf
        conf = engine.ETLConf()
        conf.spark.write.mode = 'overwrite'
        conf.spark.write.format = 'parquet'
        conf.spark.read.format = 'parquet'
        conf.spark.conf = [('spark.app.name', __name__), ('spark.master', 'local[2]')]
        conf.etl.target.name = filename
        conf.etl.source.name = filename

        # Session
        spark_conf = SparkConf().setAll(conf.spark.conf)
        with sql.SparkSession.builder.config(conf=spark_conf).getOrCreate() as spark:
            df_write = spark.range(500).toDF("number")
            # Run
            engine.HDFSWriter().write(df_write, conf)

            df_read = engine.HDFSReader().read(spark, conf, df_write.schema)

            # Assert
            assert df_read.count() == 500
            assert df_read.schema[0].name == 'number'
            assert isinstance(df_read.schema[0].dataType, LongType)

    def test_read_many_columns_parquet_schema_evolution(self):
        filename = 'hdfs://localhost:8020/test_read_many_columns_parquet_schema_evolution.parquet'

        # Conf
        conf = engine.ETLConf()
        conf.spark.write.mode = 'append'
        conf.spark.write.format = 'parquet'
        conf.spark.read.format = 'parquet'
        # conf.spark.partitionBy = 'dow'
        conf.spark.read.options = {'mergeSchema': 'true'}
        conf.spark.conf = [('spark.app.name', __name__), ('spark.master', 'local[2]')]
        conf.etl.target.name = filename
        conf.etl.source.name = filename

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
            data1 = [{'id': i, 'dow': 1} for i in range(10)]
            data2 = [{'id': i, 'name': f'Max_{i}', 'dow': 2} for i in range(10, 20)]
            data3 = [
                {'id': i, 'name': f'Max_{i}', 'dt': date(2000, 1, 1) + timedelta(days=i), 'dow': 3}
                for i in range(30, 40)
            ]
            schema1 = StructType([
                StructField('id', IntegerType(), nullable=True),
                StructField('dow', IntegerType(), nullable=True),
            ])
            schema2 = StructType([
                StructField('id', IntegerType(), nullable=True),
                StructField('dow', IntegerType(), nullable=True),
                StructField('name', StringType(), nullable=True),
            ])
            schema3 = StructType([
                StructField('id', IntegerType(), nullable=True),
                StructField('dow', IntegerType(), nullable=True),
                StructField('name', StringType(), nullable=True),
                StructField('dt', DateType(), nullable=True),
            ])

            # Write
            writer = engine.HDFSWriter()

            writer.write(spark.createDataFrame(data1, schema=schema1), conf)
            writer.write(spark.createDataFrame(data2, schema=schema2), conf)
            writer.write(spark.createDataFrame(data3, schema=schema3), conf)

            df = engine.HDFSReader().read(spark, conf)

            assert sorted(df.schema.names) == sorted(['id', 'dow', 'name', 'dt'])
            assert df.count() == 30


class TestJDBCReader:
    def test_read_simple_jdbc(self):
        # Conf
        conf = engine.ETLConf()
        conf.spark.read.format = 'jdbc'
        conf.spark.read.options = {
            'url': 'jdbc:postgresql://localhost:5432/postgres',
            'fetchsize': '100',
            'isolationLevel': 'READ_COMMITTED',
            'sessionInitStatement': 'select 300',
            'user': 'postgres',
            'password': 'password',
            'driver': 'org.postgresql.Driver',
        }
        conf.spark.conf = [
            ('spark.app.name', __name__),
            ('spark.master', 'local[2]'),
            ('spark.jars', 'https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.19/postgresql-42.2.19.jar'),
        ]

        # Session
        spark_conf = SparkConf().setAll(conf.spark.conf)
        with sql.SparkSession.builder.config(conf=spark_conf).getOrCreate() as spark:
            df = engine.JDBCReader().read(spark, conf, '(select generate_series(1, 100) n) as fake')

            # Assert
            assert df.count() == 100
