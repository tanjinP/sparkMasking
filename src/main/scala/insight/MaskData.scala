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
    val timeStamp = args(1)

    // obtaining aws creds
    val accessKeyId = sys.env("AWS_ACCESS_KEY_ID")
    val secretAccessKey = sys.env("AWS_SECRET_ACCESS_KEY")

    // setting aws creds so s3 access can be done to create dataframes
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", accessKeyId)
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", secretAccessKey)

    // reading data from s3
    // TODO figure out how to loop through config to generate multiple dataframes
    val df = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(s"s3n://$bucketName/csv/company-20180121_185043.csv") // TODO parameterize the time stamp

    // apply masking logic
    // TODO be dynamic in the type of mask (hardcoded to String here)
    val masked = df.withColumn("name", lit("MASKED"))

    // writing back to s3
    masked.coalesce(1).write // 1 could be bad?
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .mode("append")
      .save(s"s3n://$bucketName/data/csv-$timeStamp")

    spark.stop
  }
}