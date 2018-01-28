package insight

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

object MaskData {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("MaskData")
      .getOrCreate

    val bucketName = args.head

    // obtaining aws creds
    val accessKeyId = sys.env("AWS_ACCESS_KEY_ID")
    val secretAccessKey = sys.env("AWS_SECRET_ACCESS_KEY")

    // setting aws creds so s3 access can be done to create dataframes
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", accessKeyId)
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", secretAccessKey)

    // reading data from s3
    // TODO figure out how to loop through config to generate multiple dataframes
    spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(s"s3n://$bucketName/csv/activity-logs.csv") // TODO parameterize the time stamp
      .withColumn("status", lit("MASKED")) // TODO be dynamic in the type of mask (hardcoded to String here)
      .write
      .mode("append")
      .save(s"s3n://$bucketName/data/csv")

    spark.stop
  }
}