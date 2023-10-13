package aggregations

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import utils.SparkSetup

class CompositeKeys

/*
Create a composite key aggregation, using the column "value" and a window of 10 seconds. The input is a rate source and the output
is the console in append mode, then calculate the AVG(value)
 */

object CompositeKeysExample1 extends App with SparkSetup {
  import spark.implicits._

  val input = spark
    .readStream
    .format("rate")
    .option("rowPerSecond", 5)
    .load

  val dataAgg = input
    .withWatermark("timestamp", "10 seconds")
    .groupBy('value, window('timestamp, "10 seconds"))
    .agg(avg('value))

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
The Mexican government hired you to implement the count voting system in real time for the upcoming elections, where you want to show your country,
the vote counts of all parties every time new data comes in.
To do that, you are receiving the files in a Streaming fashion with the votes in the path 'src/main/resources/data/aggregations/political_parties_input' in json format.

The vote count should be stored in a column called "votes".
To show the government that you can do it, you can show them the *ordered desc* data in the console as a preview for now

 */


object ExampleFinal extends App with SparkSetup {
  import spark.implicits._

  val schema = "political_party STRING, votes INT, timestamp STRING"

  val inputStream = spark
    .readStream
    .schema(schema)
    .json("src/main/resources/data/aggregations/political_parties_input")

  val inputParsedStream = inputStream
    .withColumn("ts", 'timestamp.cast("timestamp"))

  val dataAgg = inputParsedStream
    .groupBy('political_party, window('timestamp, "1 day")) //tambien se podria quitar la ventana
    .agg(sum('votes).as("votes"))
    .orderBy('votes.desc)

  dataAgg
    .writeStream
    .format("console")
    .outputMode("complete")
    .option("truncate", false)
    .queryName("NameParaSparkUI")
    .start
    .awaitTermination

}
