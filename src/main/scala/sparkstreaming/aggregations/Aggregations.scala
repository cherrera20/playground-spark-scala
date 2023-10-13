package sparkstreaming.aggregations

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import sourcesandsinks.examples.Example4.sqlContext
import utils.SparkSetup

/*
Calculate an aggregation, the avg of the column "value", by 10 seconds tumbling windows,
 with 10 minutes watermark, reading from rate source(5 rows per second) and using console sink in append mode
 */
object Example1 extends App with SparkSetup {
  import spark.implicits._

  val input = spark
    .readStream
    .format("rate")
    .option("rowPerSecond", 5)
    .load

  val dataAgg = input
    .withWatermark("timestamp", "10 minutes")
    .groupBy(window('timestamp, "10 seconds"))
    .agg(avg('value))

  dataAgg
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", false)
    .start
    .awaitTermination
}

/*
Calculate an aggregation, the avg of the column "value", by 10 seconds tumbling windows,
 with 1 minutes watermark, reading from rate source(5 rows per second) and using console sink in append mode
 */
object Example2 extends App with SparkSetup {
  import spark.implicits._

  val input = spark
    .readStream
    .format("rate")
    .option("rowPerSecond", 5)
    .load

  val dataAgg = input
    .withWatermark("timestamp", "1 minutes")
    .groupBy(window('timestamp, "10 seconds"))
    .agg(avg('value))

  dataAgg
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", false)
    .start
    .awaitTermination
}


/*
Calculate an aggregation, the avg of the column "value", by 1 minutes tumbling windows,
 with 1 minutes watermark, reading from rate source(5 rows per second) and using console sink in update mode
 */
object Example3 extends App with SparkSetup {
  import spark.implicits._

  val input = spark
    .readStream
    .format("rate")
    .option("rowPerSecond", 5)
    .load

  val dataAgg = input
    .withWatermark("timestamp", "1 minutes")
    .groupBy(window('timestamp, "1 minute"))
    .agg(avg('value))

  dataAgg
    .writeStream
    .format("console")
    .outputMode("update")
    .option("truncate", false)
    .queryName("NameParaSparkUI")
    .start
    .awaitTermination
}


/*
Calculate an aggregation, the avg of the column "followers", by 1 minutes tumbling windows, with 1 minutes watermark, reading from socket source and using console sink in append mode.
The events look like this

{"company": "databricks", "followers":200, "timestamp": "2021-04-03 16:00:00"}
{"company": "databricks", "followers":220, "timestamp": "2021-04-03 16:00:00"}
{"company": "databricks", "followers":250, "timestamp": "2021-04-03 16:00:00"}

{"company": "databricks", "followers":250, "timestamp": "2021-04-03 16:02:00"}


{"company": "databricks", "followers":250, "timestamp": "2021-04-03 16:03:00"}
{"company": "databricks", "followers":250, "timestamp": "2021-04-03 16:05:50"}

 */
object Example4 extends App with SparkSetup {
  import spark.implicits._

  val schema = "company STRING, followers INT, timestamp STRING"
  val schemaStruct = StructType.fromDDL(schema)

  val inputStream = spark
    .readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "8083")
    .load()

  val inputParsedStream = inputStream
    .withColumn("parsed", from_json('value, schemaStruct))
    .select("parsed.*")
    .withColumn("ts", 'timestamp.cast("timestamp"))

  inputParsedStream
    .withWatermark("ts", "1 minute")
    .groupBy(window('ts, "1 minute"))
    .agg(avg('followers))
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", false)
    .queryName("NameParaSparkUI")
    .start
    .awaitTermination
}


/*
Calculate an aggregation, the avg of the column "followers", by 1 minutes tumbling windows, with 1 minutes watermark, reading from socket source and using console sink in update mode.
The events look like this {"company": "databricks", "followers":200, "timestamp": "2021-04-03 16:00:00"}

{"company": "databricks", "followers":200, "timestamp": "2021-04-03 16:00:00"}
{"company": "databricks", "followers":220, "timestamp": "2021-04-03 16:00:00"}
{"company": "databricks", "followers":250, "timestamp": "2021-04-03 16:00:00"} -- Use it as an example for late event

{"company": "databricks", "followers":250, "timestamp": "2021-04-03 16:02:00"}
{"company": "databricks", "followers":250, "timestamp": "2021-04-03 16:02:09"} -- Use it as an example for update



{"company": "databricks", "followers":250, "timestamp": "2021-04-03 16:03:00"}
{"company": "databricks", "followers":250, "timestamp": "2021-04-03 16:03:10"}

 */
object Example5 extends App with SparkSetup {
  import spark.implicits._

  val schema = "company STRING, followers INT, timestamp STRING"
  val schemaStruct = StructType.fromDDL(schema)

  val inputStream = spark
    .readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "8083")
    .load()

  val inputParsedStream = inputStream
    .withColumn("parsed", from_json('value, schemaStruct))
    .select("parsed.*")
    .withColumn("ts", 'timestamp.cast("timestamp"))

  inputParsedStream
    .withWatermark("ts", "1 minute")
    .groupBy(window('ts, "1 minute"))
    .agg(avg('followers))
    .writeStream
    .format("console")
    .outputMode("update")
    .option("truncate", false)
    .queryName("NameParaSparkUI")
    .start
    .awaitTermination
}



/*
Calculate an aggregation, the avg of the column "followers", by 1 minutes tumbling windows, with 1 minutes watermark, reading from socket source and using console sink in complete mode.
The events look like this {"company": "databricks", "followers":200, "timestamp": "2021-04-03 16:00:00"}

{"company": "databricks", "followers":200, "timestamp": "2021-04-03 16:00:00"}
{"company": "databricks", "followers":220, "timestamp": "2021-04-03 16:00:00"}
{"company": "databricks", "followers":250, "timestamp": "2021-04-03 16:00:00"} -- Use it as an example for late event

{"company": "databricks", "followers":250, "timestamp": "2021-04-03 16:02:00"}
{"company": "databricks", "followers":250, "timestamp": "2021-04-03 16:02:09"} -- Use it as an example for update



{"company": "databricks", "followers":250, "timestamp": "2021-04-03 16:03:00"}
{"company": "databricks", "followers":250, "timestamp": "2021-04-03 16:03:10"}

 */
// Complete mode does not need watermark because it requires all the data to be preserved for outputting the whole result table to a sink.
object Example6 extends App with SparkSetup {
  import spark.implicits._

  val schema = "company STRING, followers INT, timestamp STRING"
  val schemaStruct = StructType.fromDDL(schema)

  val inputStream = spark
    .readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "8083")
    .load()

  val inputParsedStream = inputStream
    .withColumn("parsed", from_json('value, schemaStruct))
    .select("parsed.*")
    .withColumn("ts", 'timestamp.cast("timestamp"))

  inputParsedStream
    .withWatermark("ts", "1 minute")
    .groupBy(window('ts, "1 minute"))
    .agg(avg('followers))
    .writeStream
    .format("console")
    .outputMode("complete")
    .option("truncate", false)
    .queryName("NameParaSparkUI")
    .start
    .awaitTermination
}

