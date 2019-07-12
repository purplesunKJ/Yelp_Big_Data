#!/bin/bash
cd /home/cloudera/project_yelp/jobs

bash SPARK_CONFIG_MONTHLY.sh 2_1_batch_job_yelp_checkin_monthly_load.py
bash SPARK_CONFIG_MONTHLY.sh 2_2_batch_job_yelp_review_monthly_load.py
bash SPARK_CONFIG_MONTHLY.sh 2_3_batch_job_yelp_tip_monthly_load.py
