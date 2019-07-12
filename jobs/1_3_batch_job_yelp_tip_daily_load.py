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
    .appName("1_3_batch_job_yelp_tip_daily_load") \
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
	df_business = yetl.import_yelp_data(spark, 'business', INPUT_PATH)
	df_tip = yetl.import_yelp_data(spark, 'tip', INPUT_PATH)

	df_business = yetl.transform_business(df_business)
	df_tip = yetl.transform_tip(df_tip)

	df_business.cache()
	df_tip.cache()

	df_tip_business = yetl.transform_tip_business(df_tip, df_business)

	yetl.load_yelp_data_daily(df_tip_business, 'tip', '2017-01', OUTPUT_PATH)
	
	return None

# entry point for PySpark ETL application
if __name__ == '__main__':
    main()