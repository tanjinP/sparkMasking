package insight

import com.typesafe.config.{ConfigFactory, ConfigValue}
import insight.DataFrameHelper.RichDataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, SparkSession}

import scala.collection.JavaConverters._

object MaskData {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("MaskData")
      .getOrCreate

    // obtaining aws creds
    val accessKeyId = sys.env("AWS_ACCESS_KEY_ID")
    val secretAccessKey = sys.env("AWS_SECRET_ACCESS_KEY")

    // setting aws creds so s3 access can be done to create dataframes
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", accessKeyId)
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", secretAccessKey)

    // creating config objects
    val conf = ConfigFactory.load("application.conf").getObject("conf")
    val configs: Iterable[Config] = conf.asScala.map(toConfig)

    // reading data from s3, masking, then saving back into s3 - all using config info
    configs.foreach { config =>
      spark.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .load(s"s3n://vts-dummyData/csv/${config.name}.csv")
        .withMultipleColumns(config.mask.map(masking):_*)
        .write
        .option("mapreduce.fileoutputcommitter.algorithm.version", "2")
        .mode("append")
        .csv(s"s3n://vts-dummyData/data/${config.name}")
    }

    spark.stop
  }

  // simple mapping to take Config EntrySet and turn into Config case class
  private def toConfig(kv: (String, ConfigValue)) = {
    val (name, cO) = kv
    val masks = cO.atKey("table")
      .getConfigList("table.mask")
      .asScala
      .map(c => Mask(c.getString("field"), c.getString("type"), c.getString("value")))

    Config(name, masks)
  }

  // masking logic based on the type of interest - inferred from config file (not exhaustive list of types)
  private def masking(mask: Mask): (String, Column) = {
    val litValue = mask.t match {
      case "String" => mask.value
      case "Int" => mask.value.toInt
      case "Boolean" => mask.value.toBoolean
      case _ => null
    }

    (mask.field, lit(litValue))
  }
}

// case classes to store values from application.conf file and easily use in application code
case class Config(
                   name: String,
                   mask: Seq[Mask]
                 )
case class Mask(
                 field: String,
                 t: String,
                 value: String
               )