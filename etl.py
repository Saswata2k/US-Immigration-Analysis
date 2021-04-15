import os
import configparser
import etl_aws
from pyspark.sql import SparkSession
from etl_aws import ImmigrationSparkifyETL

config = configparser.ConfigParser()
config.read('config.cfg')
from utils import Utils

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


class ETL:
    def __init__(self):
        self.spark = None

    def create_spark_session(self):
        self.spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.2") \
            .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
            .enableHiveSupport() \
            .getOrCreate()
        return spark

    def process_immigration_data(self, input_data, output_data, im_file_name, fname_temp, f_mapping):
        """
            Process the immigration dataset and creates fact table and calendar and country dimension table.
        """
        immigration_file = input_data + im_file_name
        # read immigration data file
        immigration_df = self.spark.read.format('com.github.saurfang.sas.spark').load(immigration_file)

        # Get processed immigration data
        immigration_df = Utils.process_immigration_data(immigration_df)

        # get processed global temp data
        temp_df = self.process_global_land_temperatures(input_data, fname_temp)

        # clean immigration spark dataframe
        spark_etl = ImmigrationSparkifyETL(spark=self.spark, data_im=immigration_df, data_temp=temp_df)

        # create arrival of immigrants calendar dimension table
        df_im_calender = spark_etl.create_t_immigration_calendar_dim(output_data)

        # create country dimension table
        df_country = spark_etl.create_t_country_dim(output_data, f_mapping)

        # create immigration fact table
        df_im_fact = spark_etl.create_t_immigration_fact(output_data)

        # Check data sanity here
        spark_etl.check_data_sanity(df_im_calender, "t_immigration_calendar")
        spark_etl.check_data_sanity(df_country, "t_country")
        spark_etl.check_data_sanity(df_im_fact, "t_immigration_fact")

    def process_demographics_data(self, input_data, output_data, file_name):
        """
            Process the demographics data and create the t_us_dem table
        """

        # load demographics data
        file = input_data + file_name
        df_us_dem = self.spark.read.csv(file, inferSchema=True, header=True, sep=';')

        # clean demographics data
        df_us_dem = Utils.process_us_demographic_data(df_us_dem)

        # create demographic dimension table
        etl_us_dem = ImmigrationSparkifyETL(self.spark, data_us_dem=df_us_dem)
        df_us_dem = etl_us_dem.create_t_us_dem_dimension(output_data)

        # Check data sanity
        etl_us_dem.check_data_sanity(df_us_dem, "t_us_demographics")

    def process_global_land_temperatures(self, input_data, file_name):
        """
            Process the global land temperatures data and return a dataframe
        """
        # load data
        file = input_data + file_name
        temperature_df = self.spark.read.csv(file, header=True, inferSchema=True)

        # clean the temperature data
        df_temp_cleaned = Utils.process_temp_data(temperature_df)
        return df_temp_cleaned

    def process_air_traffic_data(self, input_data, output_data, file_name):
        """
            Process the global land temperatures data and return a dataframe
        """
        # load data
        file = input_data + file_name
        df_air_traffic = self.spark.read.json(file, header=True, inferSchema=True)

        # clean the temperature data
        df_air_traffic = Utils.process_air_traffic_data(df_air_traffic)

        # create air traffic table
        etl_im = ImmigrationSparkifyETL(self.spark, data_air_traffic=df_air_traffic)
        df_air_traffic = etl_im.create_t_air_traffic_dim(df_air_traffic, output_data)

        # Check data sanity
        etl_im.check_data_sanity(df_air_traffic, "t_air_traffic")

        return df_air_traffic


if __name__ == "__main__":
    dir_input = "s3://data_lakes_immigration/"
    dir_output = "s3://data_lakes_immigration/outputs/"

    immigration_file_name = 'i94_apr16_sub.sas7bdat'
    temperature_file_name = 'GlobalLandTemperaturesByCity.csv'
    usa_demographics_file_name = 'us-cities-demographics.csv'
    air_traffic_file_name = 'airport-codes_json.json'
    mapping_file = dir_input + "i94res.csv"

    # First we will create a spark session
    etl = ETL()
    spark = etl.create_spark_session()

    # Then we will be loading the i94res to country mapping file
    mapping_file = spark.read.csv(mapping_file, header=True, inferSchema=True)

    # Now let's execute all code flows including data cleaning and data sanity checking
    etl.process_immigration_data(dir_input, dir_output, immigration_file_name, temperature_file_name,
                                 mapping_file)

    etl.process_demographics_data(dir_input, dir_output, usa_demographics_file_name)
    etl.process_air_traffic_data(dir_input, dir_output, file_name=air_traffic_file_name)
