#!/bin/bash
cd /home/cloudera/project_yelp/jobs

bash SPARK_CONFIG_YEARLY.sh 3_1_batch_job_yelp_checkin_yearly_load.py
bash SPARK_CONFIG_YEARLY.sh 3_2_batch_job_yelp_review_yearly_load.py
bash SPARK_CONFIG_YEARLY.sh 3_3_batch_job_yelp_tip_yearly_load.py
