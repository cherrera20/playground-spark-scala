package aggregations

import org.apache.spark.sql.functions.current_timestamp
import utils.SparkSetup

class Deduplication

/*
Deduplication without watermark. We need to deduplicate data that is in json format in 'src/main/resources/data/aggregations/duplicates'
then you can use the console sink
 */

object DeduplicationWithoutWatermark extends App with SparkSetup {
  val schema = "key STRING, value INT"

  val inputStream = spark
    .readStream
    .schema(schema)
    .json("src/main/resources/data/aggregations/duplicates")

  val dataAgg = inputStream
    .dropDuplicates(Seq("key","value"))

  dataAgg
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", false)
    .queryName("NameParaSparkUI")
    .start
    .awaitTermination
}

/*
Deduplication with watermark. We need to deduplicate data that is in json format in 'src/main/resources/data/aggregations/duplicates'
, we can wait up to 5 seconds for late events, then you can use the console sink
 */
object DeduplicationWithWatermark extends App with SparkSetup {
  val schema = "key STRING, value INT"

  val inputStream = spark
    .readStream
    .schema(schema)
    .json("src/main/resources/data/aggregations/duplicates")
    .withColumn("processingTime", current_timestamp)

  val dataAgg = inputStream
    .withWatermark("processingTime", "10 seconds")
    .dropDuplicates(Seq("key","value", "processingTime"))

  dataAgg
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", false)
    .queryName("NameParaSparkUI")
    .start
    .awaitTermination
}