from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, split, explode, count, size, when, isnan, \
    reverse, input_file_name, struct, lit, array, trim
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import (StructType, StructField, IntegerType,
                               StringType, FloatType, LongType, DoubleType,TimestampType)
from airflow.contrib.hooks.aws_hook import AwsHook
import os


class ETLFlow:
    
    os.environ["AWS_ACCESS_KEY_ID"]= ''
    os.environ["AWS_SECRET_ACCESS_KEY"]= ''

    
    def __init__(self, 
                 aws_credentials_id=""):
        self.aws_credentials_id = aws_credentials_id

    def create_spark_session(self):
        """
            Create Spark session.

            Return a Spark session object.

        """
        spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()
        return spark
    
    def pivot_df(self, df, fixed_columns, col_name):
        """
            Pivot dataframe
            Args:
                df : datafram to be pivoted
                fixed_columns: columns name will be fix
                col_name: name of new column
           
        """
        cols, dtypes = zip(*((c, t) for (c, t) in df.dtypes if c not in fixed_columns))
        kvs = explode(array([struct(lit(c).alias(col_name), col(c).alias("value")) for c in cols])).alias("kvs")
        df = df.select(fixed_columns + [kvs]).select(fixed_columns + ["kvs.{}".format(col_name), "kvs.value"])
        return df

    def process_country_data(self, spark, input_data, output_data):
        """
            Read json country data, extract columns to create country table and write country table to parquet files
            Args:
                spark: Spark session object.
                input_data: input S3 bucket path.
                output_data: output S3 bucket path.
        
        """
        

        country_data = input_data + 'all.json'


        df = spark.read.json(country_data)
        df.persist()


        country_table = df.select(col('alpha-3').alias('code'), \
                                  col('name'), \
                                  col('region'), \
                                  col('sub-region').alias('sub_region')).dropDuplicates()

        
        country_table.write.parquet(output_data + 'dim_country.parquet', mode = 'overwrite')
        
    def process_indicator_data(self, spark, input_data, output_data):
        """
            Read csv indicator data, extract columns to create indicator table and write country table to parquet files
            Args:
                spark: Spark session object.
                input_data: input S3 bucket path.
                output_data: output S3 bucket path.
        
        """
        
        ind_data = input_data + 'Indicators.csv'
        df_indicator = spark.read.csv(ind_data, header='true')
        df_indicator.persist()
        
        indicator_table = df_indicator.select(trim(col('Series Code')).alias('code'), 
                                              trim(col('Indicator Name')).alias('name'), 
                                              trim(col('Unit of measure')).alias('measure'), 
                                              trim(col('Periodicity')).alias('periodicity'), 
                                              trim(col('Aggregation method')).alias('method')).dropDuplicates()
        
        
        indicator_table.write.parquet(output_data + 'dim_indicator.parquet', mode = 'overwrite')
        
    def process_fact_data(self, spark, input_data, output_data):
        """
            Read csv score data, extract columns to create fact score table and write score table to parquet files
            Args:
                spark: Spark session object.
                input_data: input S3 bucket path.
                output_data: output S3 bucket path.
        
        """
        
        wdi_data = input_data + 'WDIData.csv'
        
        df_wdi = spark.read.csv(wdi_data, header='true')
        df_wdi.persist()
        
        fixed_columns = ['Country Name', 'Country Code', 'Indicator Name', 'Indicator Code']
        df_wdi = self.pivot_df(df_wdi, fixed_columns, 'year')
        
        
        # -- score table
        df_score = df_wdi.select(col('year').alias('time_year'), \
                                 col('Country Code').alias('country_code'), \
                                 col('Indicator Code').alias('indicator_code'), \
                                 col("value").cast(DoubleType()))
        

        df_score.write.partitionBy('time_year').parquet(output_data + 'fact_score.parquet', mode = 'overwrite')
        
       
    def execute(self):
        
        spark = self.create_spark_session()
        input_data = "s3a://capstone-lemoura-udacity/"
        output_data = "s3a://capstone-lemoura-udacity/transform/"
        
        self.process_country_data(spark, input_data, output_data)
        self.process_indicator_data(spark, input_data, output_data)
        self.process_fact_data(spark, input_data, output_data)
        
       