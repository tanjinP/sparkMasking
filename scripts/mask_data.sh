echo "Beginning spark application"

export SPARK_HOME
cd ~/dev/sparkMasking
$SPARK_HOME/bin/spark-submit --class insight.MaskData --master spark://ip-10-0-0-5.ec2.internal:7077 target/scala-2.11/masking-data_2.11-0.1.jar vts-dummyData

echo "Spark job is complete"
