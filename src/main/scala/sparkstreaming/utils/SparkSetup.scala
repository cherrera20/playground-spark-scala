package utils

import org.apache.spark.sql.SparkSession

trait SparkSetup {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("examples")
    .getOrCreate()
}
