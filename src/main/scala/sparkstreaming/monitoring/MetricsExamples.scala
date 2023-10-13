package monitoring

import org.apache.spark.sql.streaming.Trigger
import utils.SparkSetup

class MetricsExamples



//Using Listener
object ExampleMetrics1 extends App with SparkSetup with SparkMetrics {

  import spark.implicits._

  val inputDF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "company")
    .load
    .select('value.cast("string"))

  val query = inputDF
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start()

  metrics()
  query.awaitTermination
}



/**
 * Using metric sinks
 * .config("spark.sql.streaming.metricsEnabled", "true")
 * .config("spark.metrics.conf.*.sink.csv.class", "org.apache.spark.metrics.sink.CsvSink")
 * .config("spark.metrics.conf.*.sink.csv.period", "1")
 * .config("spark.metrics.conf.*.sink.csv.unit", "seconds")
 * .config("spark.metrics.conf.*.sink.csv.directory", "/tmp/metrics/")
 */
//Using Sink
object ExampleMetrics2 extends App with SparkSetup {

  import spark.implicits._

  val inputDF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "company")
    .load
    .select('value.cast("string"))

  val query = inputDF
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start()

  query.awaitTermination
}
