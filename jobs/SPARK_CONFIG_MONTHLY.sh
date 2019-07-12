a=${@}
SCRIPTPATH='/home/cloudera/project_yelp/jobs/'

/usr/bin/spark2-submit  \
	--master yarn \
	--driver-memory 1G \
	--executor-memory 1G \
	--executor-cores 1 \
	--num-executors 1 \
	--queue production $SCRIPTPATH/${a}