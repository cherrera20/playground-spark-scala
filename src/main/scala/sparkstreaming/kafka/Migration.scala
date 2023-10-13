package kafka

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import utils.SparkSetup

class Migration

/***
 * Este ejemplo muestra como teniendo un proceso Spark Batch (ExampleBatch) que lee un csv y como podr'iamos migrarlo a
 * Streaming (ExampleStreaming)
 */
object ExampleBatch extends App with SparkSetup {
  import spark.implicits._

  val df = spark
    .read
    .option("header", "true")
    .csv("src/main/resources/data/migration/")

  val aggDf = df
    .groupBy('type)
    .count()

  aggDf
    .coalesce(1)
    .write
    .mode("overwrite")
    .json("/tmp/migrationResult")

}

// Lo migramos a streaming
object ExampleStreaming extends App with SparkSetup {
  import spark.implicits._

  val schema = "tailnum STRING, type STRING, manufacturer STRING, issue_date STRING, model STRING, status STRING, aircraft_type STRING, engine_type STRING, year STRING"

  val df = spark
    .readStream
    .schema(schema)
    .option("header", "true")
    .csv("src/main/resources/data/migration/")

  df
    .writeStream
    .foreachBatch{ (dataframe: DataFrame, id: Long) => {
      val aggDf = dataframe
        .groupBy('type)
        .count()

      aggDf
        .coalesce(1)
        .write
        .mode("overwrite")
        .json("/tmp/migrationStreamResult")
    }
    }
    .trigger(Trigger.Once())
    .start()
    .awaitTermination()

}