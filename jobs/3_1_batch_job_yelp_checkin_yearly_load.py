import numpy as np
import pandas as pd

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import * #needed for all the sql types
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, col, split, explode
from pyspark.sql.window import Window

## Create a spark session
spark = SparkSession \
    .builder \
    .appName("3_1_batch_job_yelp_checkin_yearly_load") \
    .getOrCreate()

## Import dependencies libraries
spark \
    .sparkContext \
    .addPyFile("/home/cloudera/project_yelp/dependencies/yelp_ETL.py")

import yelp_ETL as yetl

spark.sparkContext.setLogLevel("INFO")

# Path to data source
INPUT_PATH = "/user/cloudera/input/"
OUTPUT_PATH = "/user/cloudera/output/"

def main():
	"""
	Main ETL script definition
	:return: None
	"""
	## Check in batch ETL ##
	df_checkin_combined = yetl.import_yelp_yearly_data(spark, 'checkin', '2017', OUTPUT_PATH)
	df_checkin_combined = yetl.transform_checkin_business_split(df_checkin_combined)
	yetl.load_yelp_data_yearly(df_checkin_combined, 'checkin', '2017', 'parquet',  OUTPUT_PATH)
	# yetl.load_yelp_data_yearly(df_checkin_combined, 'checkin', '2017', 'csv',  OUTPUT_PATH)

	return None

# entry point for PySpark ETL application
if __name__ == '__main__':
    main()