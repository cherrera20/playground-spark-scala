package kafka

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import utils.SparkSetup

class Examples

/*
Read Company data using the Kafka Source. The data is being written in the topic 'company'. Print the output in the console every 10 seconds
 */
object Example1 extends App with SparkSetup {
  import spark.implicits._

  val inputStream = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("subscribe", "company")
    .load()
    .select('value.cast("string"))
    .as[String]

  inputStream
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", false)
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .queryName("NameParaSparkUI")
    .start
    .awaitTermination

}


/*
Read Company data using the Kafka Source. The data is being written in the topic 'company', this time parse the json-string using a schema in order to have columns instead of a single string
Print the output in the console every 10 seconds
 */
object Example2 extends App with SparkSetup {
  import spark.implicits._

  val schema = "company STRING, employees INT, date STRING"
  val schemaStruct = StructType.fromDDL(schema)

  val inputStream = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("subscribe", "company")
    .load()
    .select('value.cast("string"))
    .as[String]

  val inputStreamParsed = inputStream
    .withColumn("parsed", from_json('value, schemaStruct))
    .select("parsed.*")
    .withColumn("timestamp", 'date.cast("timestamp"))

  inputStreamParsed
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", false)
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .queryName("NameParaSparkUI")
    .start
    .awaitTermination
}


/*
Read from multiple Topics. Print the output in the console. This time, also print the topic from each event
 */

object Example3 extends App with SparkSetup {
  import spark.implicits._

  val inputStream = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("subscribe", "topic_x1,topic_x2,topic_x3")
    .load()
    .select('topic.cast("string"), 'value.cast("string"))
    .as[(String, String)]

  inputStream
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", false)
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .queryName("NameParaSparkUI")
    .start
    .awaitTermination
}

/*
Read from multiple Topics specified by a pattern(regex). Print the output in the console. This time, also print the topic from each event
 */


object Example4 extends App with SparkSetup {
  import spark.implicits._
  val inputStream = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("subscribePattern", "topic_x.*")
    .load()
    .select('topic.cast("string"), 'value.cast("string"))
    .as[(String, String)]

  inputStream
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", false)
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .queryName("NameParaSparkUI")
    .start
    .awaitTermination
}


/*
Read from multiple Topics specified by a pattern(regex). This time, also get the topic from each event and store the data in
parquet format in /tmp/example5/ every 10 seconds
 */


object Example5 extends App with SparkSetup {
  import spark.implicits._

  val inputStream = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("subscribePattern", "topic_x.*")
    .load()
    .select('topic.cast("string"), 'value.cast("string"))
    .as[(String, String)]

  inputStream
    .writeStream
    .format("parquet")
    .option("checkpointLocation", "/tmp/check/")
    .option("path", "/tmp/example5/")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .queryName("NameParaSparkUI")
    .start
    .awaitTermination

}

/*
Read from multiple Topics specified by a pattern(regex). This time, also get the topic from each event and send the data back to kafka
 as fast as you can to a topic called 'output'
 */
object Example6 extends App with SparkSetup {
  import spark.implicits._

  val inputStream = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("subscribePattern", "topic_x.*")
    .load()
    .select('topic.cast("string").as("topicOrigen"), 'value.cast("string"))
    .as[(String, String)]

  inputStream
    .writeStream
    .format("kafka")
    .option("checkpointLocation","/tmp/example6_checkpoint")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("topic","outputTest")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .queryName("NameParaSparkUI")
    .start
    .awaitTermination
}

/*
Read Company data using the Kafka Source. The data is being written in the topic 'company', this time parse the json-string
using a schema in order to have columns instead of a single string (in this point is where we would be working with our event)
In the final event, send the topic, company and employees back to kafka as fast as you can to a topic called 'output'
 */
object Example7 extends App with SparkSetup {
  import spark.implicits._

  val schema = "company STRING, employees INT, date STRING"
  val schemaStruct = StructType.fromDDL(schema)

  val inputStream = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("subscribe", "company")
    .load()
    .select('topic.cast("string"), 'value.cast("string"))
    .as[(String, String)]

  val inputStreamParsed = inputStream
    .withColumn("parsed", from_json('value, schemaStruct))
    .select("topic", "parsed.company", "parsed.employees")

  //Trabajariamos con las columnas y cuando terminasemos y quisieramos enviar de nuevo a kafka, tenemos que mandar
  //todo el contenido en un solo campo llamado value. El comando to_json nos serializa, pero tenemos que pasarle
  //un struct o un map

  val outputStream = inputStreamParsed
    .select(to_json(struct('topic,'company, 'employees)).as("value"))

  outputStream
     .writeStream
     .format("kafka")
     .option("checkpointLocation","/tmp/example7_checkpoint")
     .option("kafka.bootstrap.servers", "127.0.0.1:9092")
     .option("topic","output")
     .queryName("NameParaSparkUI")
     .start
     .awaitTermination
}
