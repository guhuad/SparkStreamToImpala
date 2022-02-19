 -- 1
 nohup sudo -u spark spark-submit --class com.jaeyeong.datalake.datapipeline.ingest.SparkTest \
 --master yarn \
 --deploy-mode cluster \
 --conf spark.driver.extraJavaOptions=" -Dfile.encoding=utf-8 " \
 --conf spark.executor.extraJavaOptions=" -Dfile.encoding=utf-8 " \
 /usr/apps/jobs/SparkStreaming-1.0-SNAPSHOT.jar > ./spark_data_shark.log 2>&1 &



 -- 2
 nohup sudo -u spark spark-submit --class com.jaeyeong.datalake.datapipeline.ingest.SparkTest \
 --master yarn \
 --deploy-mode client \
 --conf spark.driver.extraJavaOptions=" -Dfile.encoding=utf-8 " \
 --conf spark.executor.extraJavaOptions=" -Dfile.encoding=utf-8 " \
 --driver-memory 1g \
 --executor-memory 1g \
 --num-executors 3 \
 --executor-cores 2 \
 /usr/apps/jobs/SparkStreaming-1.0-SNAPSHOT.jar \ > /root/jobs/logs/flumestream.log 2>&1 &