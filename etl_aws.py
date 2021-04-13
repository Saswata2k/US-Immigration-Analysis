import configparser
import os
import datetime as dt

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

config = configparser.ConfigParser()
config.read('config.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


class ImmigrationSparkifyETL:

    def __init__(self, spark, data_im=None, data_us_dem=None, data_temp=None, data_air_traffic=None):
        self.data_im = data_im
        self.data_us_dem = data_us_dem
        self.data_temp = data_temp
        self.spark = spark
        self.data_air_traffic = data_air_traffic

    def create_t_us_dem_dimension(self, output_data):
        """
            This function creates a us demographics dimension table from the us cities demographics data
        """
        dim_df = self.data_us_dem.withColumnRenamed('Median Age', 'median_age') \
            .withColumnRenamed('Male Population', 'male_population') \
            .withColumnRenamed('Female Population', 'female_population') \
            .withColumnRenamed('Total Population', 'total_population') \
            .withColumnRenamed('Number of Veterans', 'num_of_veterans') \
            .withColumnRenamed('Foreign-born', 'foreign_born') \
            .withColumnRenamed('Average Household Size', 'avg_household_size') \
            .withColumnRenamed('State Code', 'state_code')

        # lets add an id column
        dim_df = dim_df.withColumn('id', F.monotonically_increasing_id())

        # write dimension to parquet file
        dim_df.write.parquet(output_data + "demographics", mode="overwrite")

        return dim_df

    def create_t_immigration_fact(self, output_data):
        """
            This function creates an country dimension from the immigration and global land temperatures data.
        """
        # create a udf to cast arrival date to datetime
        get_datetime = F.udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)

        # rename columns to align with data model
        df = self.data_im.withColumnRenamed('ccid', 'immigrant_id') \
            .withColumnRenamed('i94res', 'country_residence_code') \
            .withColumnRenamed('i94addr', 'state_code')

        # convert arrival date into datetime object
        df = df.withColumn("arrdate", get_datetime(df.arrdate))

        # write dimension to parquet file
        df.write.mode("overwrite").parquet(output_data + "immigration_fact.parquet")
        return df

    def create_country_dimension_table(self, output_data, mapping_file):
        """
            It creates a country dimension from the immigration and global land temperatures data.
        """
        # create temporary view for immigration data
        self.data_im.createOrReplaceTempView("vu_immigration")

        # create temporary view for countries codes data
        mapping_file.createOrReplaceTempView("vu_country_code")

        # get the aggregated temperature data
        agg_temp = self.data_temp.select(['Country', 'AverageTemperature']).groupby('Country').avg().withColumnRenamed(
            'avg(AverageTemperature)', 'average_temperature')

        # create temporary view for countries average temps data
        agg_temp.createOrReplaceTempView("vu_avg_temperature")

        # create country dimension using SQL
        df_country = self.spark.sql(
            """
            SELECT 
                i94res as country_code,
                Name as country_name
            FROM vu_immigration
            LEFT JOIN vu_country_code
            ON vu_immigration.i94res=vu_country_code.code
            """
        ).distinct()

        # create temp country view
        df_country.createOrReplaceTempView("vu_country")

        df_country = self.spark.sql(
            """
            SELECT 
                country_code,
                country_name,
                average_temperature
            FROM vu_country
            LEFT JOIN vu_avg_temperature
            ON vu_country.country_name=vu_avg_temperature.Country
            """
        ).distinct()

        # write the dimension to a parquet file
        df_country.write.mode("overwrite").parquet(output_data + "country")

        return df_country

    def create_immigration_calendar_dimension(self, output_data):
        """
            This function creates an immigration calendar based on arrival date
        """
        # create a udf to convert arrival date in SAS format to datetime object
        get_datetime = F.udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)

        # create initial calendar df from arrdate column
        calendar_df = self.data_im.select(['arrdate']).withColumn("arrdate",
                                                                  get_datetime(self.data_im["arrdate"])).distinct()

        # expand df by adding other calendar columns
        calendar_df = calendar_df.withColumn('arrival_day', F.dayofmonth('arrdate'))
        calendar_df = calendar_df.withColumn('arrival_week', F.weekofyear('arrdate'))
        calendar_df = calendar_df.withColumn('arrival_month', F.month('arrdate'))
        calendar_df = calendar_df.withColumn('arrival_year', F.year('arrdate'))
        calendar_df = calendar_df.withColumn('arrival_weekday', F.dayofweek('arrdate'))

        # create an id field in calendar df
        calendar_df = calendar_df.withColumn('id', F.monotonically_increasing_id())

        # write the calendar dimension to parquet file
        partition_columns = ['arrival_year', 'arrival_month', 'arrival_week']
        calendar_df.write.parquet(output_data + "immigration_calendar", partitionBy=partition_columns, mode="overwrite")

        return calendar_df

    def create_air_traffic_dimension(self, output_data):
        """
            This function creates an immigration calendar based on arrival date
        """

        # rename columns to align with data model
        df_air = self.data_air_traffic.withColumnRenamed('ident', 'id') \
            .withColumnRenamed('iso_country', 'country') \
            .withColumnRenamed('iso_region', 'region')

        # write the calendar dimension to parquet file
        df_air.write.parquet(output_data + "air_traffic", mode="overwrite")

        return df_air

    # Perform quality checks here
    @staticmethod
    def check_data_sanity(dataframe, table_name):
        """
            Count checks on fact and dimension table to ensure completeness of data.
        """
        count_rows = dataframe.shape[0]

        if count_rows == 0:
            print(f"Data quality check failed for {table_name} with zero records!")
        else:
            print(f"Data quality check passed for {table_name} with {count_rows:,} records.")
        return 0
