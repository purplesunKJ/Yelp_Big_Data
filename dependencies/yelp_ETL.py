import pyspark
import numpy as np
import pandas as pd
import pandas.api.types

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf #needed for UDF
from pyspark.sql.types import * #needed for all the sql types
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, col, split, explode
from datetime import date, timedelta
from pyspark.sql.window import Window

import datetime
import operator

#############
## Extract ##
#############
def import_yelp_data(spark, data, input_path):
    """
    This module import yelp data from HDFS

    Parameters
    ----------
    spark: sparkSession object
    data:
    path: 
    output:
    """

    df = spark.read.json(input_path + 'yelp_academic_dataset_%s.json' % (data))

    return df

def import_yelp_monthly_data(spark, data, monthstr, input_path):

    df = spark.read.parquet(input_path +'/%s/%s/*/' % (data, monthstr))

    return df

def import_yelp_yearly_data(spark, data, yearstr, input_path):

    df = spark.read.parquet(input_path +'/%s/%s*/*/' % (data, yearstr))

    return df

###############
## Transform ##
###############
#############
## Checkin ##
#############
## Checkin batch data daily loading
def transform_business(df):

    df_business = df \
        .withColumnRenamed('stars', 'avg_stars') \
        .select('business_id', 'name', 'avg_stars', 'review_count', 'city', 'address', 'latitude', 'longitude', 'categories')

    return df_business

def transform_checkin(df):

    df_checkin = df \
        .orderBy('business_id')

    return df_checkin

def transform_checkin_business(df_checkin, df_business):

    df_checkin_business = df_checkin \
        .join(df_business, 'business_id', 'left') \
        .withColumnRenamed('review_count', 'total_review_count') \
        .withColumn('checkin_date',explode(split('date',', '))) \
        .withColumn('timestamp', col('checkin_date').cast(TimestampType())) \
        .withColumn('date', col('checkin_date').cast(DateType())) \
        .withColumn('hour', F.hour(col('timestamp'))) \
        .drop('business_id', 'checkin_date', 'timestamp',) \
		.orderBy('date', 'hour','name', 'categories') \
        .select('date', 'hour', 'name', 'avg_stars', 'total_review_count', 'city', 'address', 'latitude', 'longitude', 'categories')

    return df_checkin_business


## Check in batch data category splitting
def transform_checkin_business_split(df_checkin_business):

	df_checkin_business_split = df_checkin_business \
		.withColumn('categories',explode(split('categories',', '))) \
		.orderBy('date', 'hour','name', 'categories') \
        .select('date', 'hour', 'name', 'avg_stars', 'total_review_count', 'city', 'address', 'latitude', 'longitude', 'categories')

	return df_checkin_business_split

############
## Review ##
############
## Review batch data daily loading
def transform_review(df):

	df_review = df

	return df_review

def transform_review_business(df_review, df_business):

	df_review_business = df_review \
		.join(df_business, 'business_id', 'left') \
		.withColumnRenamed('review_count', 'total_review_count') \
		.withColumnRenamed('date', 'review_date') \
		.withColumnRenamed('text', 'review_text') \
		.withColumn('timestamp', col('review_date').cast(TimestampType())) \
		.withColumn('date', col('review_date').cast(DateType())) \
		.withColumn('hour', F.hour(col('timestamp'))) \
		.drop('business_id', 'review_date', 'timestamp',) \
		.orderBy('date', 'hour','name', 'categories') \
		.select('date', 'hour','name', 'avg_stars', 'total_review_count', 'city', 'address', 'latitude', 'longitude', 'categories', 'cool', 'funny', 'useful', 'stars', 'review_text')

	return df_review_business

## Review batch data category splitting
def transform_review_business_split(df_review_business):

	df_review_business_split = df_review_business \
		.withColumn('categories',explode(split('categories',', '))) \
		.orderBy('date', 'hour','name', 'categories') \
		.select('date', 'hour','name', 'avg_stars', 'total_review_count', 'city', 'address', 'latitude', 'longitude', 'categories', 'cool', 'funny', 'useful', 'stars', 'review_text')

	return df_review_business_split

############
## Review ##
############
## Tip batch data daily loading
def transform_tip(df):

	df_tip = df

	return df_tip

def transform_tip_business(df_tip, df_business):

	df_tip_business = df_tip \
		.join(df_business, 'business_id', 'left') \
		.withColumnRenamed('review_count', 'total_review_count') \
		.withColumnRenamed('date', 'tip_date') \
		.withColumnRenamed('text', 'tip_text') \
		.withColumn('timestamp', col('tip_date').cast(TimestampType())) \
		.withColumn('date', col('tip_date').cast(DateType())) \
		.withColumn('hour', F.hour(col('timestamp'))) \
		.drop('business_id', 'tip_date', 'timestamp',) \
		.orderBy('date', 'hour','name', 'categories') \
		.select('date', 'hour', 'name', 'avg_stars', 'total_review_count', 'city', 'address', 'latitude', 'longitude', 'categories', 'compliment_count', 'tip_text')

	return df_tip_business

## Tip batch data category splitting
def transform_tip_business_split(transform_tip_business):

	transform_tip_business_split = transform_tip_business \
		.withColumn('categories', explode(split('categories',', '))) \
		.orderBy('date', 'hour','name', 'categories') \
		.select('date', 'hour', 'name', 'avg_stars', 'total_review_count', 'city', 'address', 'latitude', 'longitude', 'categories', 'compliment_count', 'tip_text')

	return transform_tip_business_split

##########
## Load ##
##########
def load_yelp_data_daily(df, data, year_month, output_path):
    """
    This module import yelp data from HDFS

    Parameters
    ----------
    spark: sparkSession object
    data:
    path: 
    output:
    """
    checkin_date_list = df \
        .filter(col('date')[0:7] == year_month) \
        .select('date') \
        .distinct() \
        .orderBy('date') \
        .rdd.flatMap(lambda x: x) \
        .collect()

    for item in checkin_date_list:
        itemstr = str(item)
        df \
            .filter(col('date') == item) \
            .repartition(20) \
            .write.parquet(output_path + data + '/%s/%s/' % (itemstr[0:4] + itemstr[5:7], itemstr[0:4] + itemstr[5:7] + itemstr[8:10]), mode='overwrite')

    return None



def load_yelp_data_monthly(df, data, monthstr, data_format, output_path):

	if data_format == 'csv':
		df \
			.repartition(1) \
			.write.csv(output_path + data + '/' + monthstr + '_csv/', mode='overwrite')
	elif data_format == 'parquet':
		df \
			.withColumn('date', col('date').cast(StringType())) \
			.repartition(200) \
			.write.parquet(output_path + data + '/' + monthstr + '_parquet/', mode='overwrite')

	return None

def load_yelp_data_yearly(df, data, yearstr, data_format, output_path):

	if data_format == 'csv':
		df \
			.repartition(1) \
			.write.csv(output_path + data + '/' + yearstr + '_csv/', mode='overwrite')
	elif data_format == 'parquet':
		df \
			.withColumn('date', col('date').cast(StringType())) \
			.repartition(1000) \
			.write.parquet(output_path + data + '/' + yearstr + '_parquet/' , mode='overwrite')

	return None
