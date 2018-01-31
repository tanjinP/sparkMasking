#!/bin/bash
export SPARK_HOME

echo "Beginning Spark application"

cd ~/dev/sparkMasking
$SPARK_HOME/bin/spark-submit --class insight.MaskData \
--master spark://ip-10-0-0-5.ec2.internal:7077 \
--files src/main/resources/application.conf \
--jars jars/config-1.3.1.jar \
target/scala-2.11/masking-data_2.11-0.1.jar

echo "Spark job is complete"
