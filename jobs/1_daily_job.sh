#!/bin/bash
cd /home/cloudera/project_yelp/jobs

bash SPARK_CONFIG_DAILY.sh 1_1_batch_job_yelp_checkin_daily_load.py
bash SPARK_CONFIG_DAILY.sh 1_2_batch_job_yelp_review_daily_load.py
bash SPARK_CONFIG_DAILY.sh 1_3_batch_job_yelp_tip_daily_load.py
