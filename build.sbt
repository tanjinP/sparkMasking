
name := "masking-data"

version := "0.1"

scalaVersion := "2.11.6"

val sparkVersion = "2.2.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.hadoop" % "hadoop-aws" % "2.9.0",
  "com.amazonaws" % "aws-java-sdk" % "1.11.0",
  "com.typesafe" % "config" % "1.3.1"
)
        