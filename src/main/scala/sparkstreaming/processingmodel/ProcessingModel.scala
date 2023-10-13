package processingmodel

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, window}

object ProcessingModel extends App {
  val spark = SparkSession.builder.master("local").appName("processing model").getOrCreate

  import spark.implicits._

  val sourceDF = spark
    .readStream
    .format("rate")
    .option("rowsPerSecond", 2)
    .load

  val aggregateStream = sourceDF
    .groupBy(window('timestamp, "10 seconds"))
    .agg(avg('value))

  val sink = aggregateStream
    .writeStream
    .format("console")
    .option("numRows", 10)
    .option("truncate", false)
    .outputMode("complete")

  val process = sink.start

  //process.awaitTermination
  spark.streams.awaitAnyTermination
}
