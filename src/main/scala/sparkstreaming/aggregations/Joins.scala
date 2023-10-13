package aggregations

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import utils.SparkSetup

/*
{"event": "df1","company": "databricks", "followers":200, "timestamp": "2021-04-03 16:00:00"}
{"event": "df2","company": "databricks", "employee_name":"Peter", "df2_timestamp": "2021-04-03 16:00:00"}
{"event": "df1","company": "databricks", "followers":200, "timestamp": "2021-04-03 16:01:00"}
{"event": "df1","company": "databricks", "followers":200, "timestamp": "2021-04-03 16:02:00"}
{"event": "df2","company": "databricks", "employee_name":"John", "df2_timestamp": "2021-04-03 16:00:00"}
{"event": "df2","company": "databricks", "employee_name":"Winston", "df2_timestamp": "2021-04-03 16:00:00"}

 */

/*
Hacer join de 2 streams por company. Las 2 fuentes son sockets,
uno en el puerto 9999 y otro en el 9998. Sacar el resultado por consola
df1 - 9999
df2 - 9998
 */
object JoinsExample1 extends App with SparkSetup {

  import spark.implicits._

  val schema1       = "event STRING, company STRING, followers INT, timestamp STRING"
  val schemaStruct1 = StructType.fromDDL(schema1)

  val inputStream1 = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "9999")
    .load()

  val inputStreamParsed1 = inputStream1
    .withColumn("parsed", from_json('value, schemaStruct1))
    .select("parsed.*")
    .withColumn("ts", 'timestamp.cast("timestamp"))

  val schema2       = "event STRING, company STRING, employee_name STRING, df2_timestamp STRING"
  val schemaStruct2 = StructType.fromDDL(schema2)

  val inputStream2 = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "9998")
    .load()

  val inputStreamParsed2 = inputStream2
    .withColumn("parsed", from_json('value, schemaStruct2))
    .select("parsed.*")
    .withColumn("ts", 'df2_timestamp.cast("timestamp"))

  val joinStream = inputStreamParsed1
    .join(inputStreamParsed2, "company")

  joinStream.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", value = false)
    .queryName("NameParaSparkUI")
    .start
    .awaitTermination
}

/*
Hacer join de 2 streams por company. Las 2 fuentes son sockets,
uno en el puerto 9999 y otro en el 9998. Sacar el resultado por consola. Añadir watermark de 10 min.
df1 - 9999
df2 - 9998
 */
object JoinsExample2 extends App with SparkSetup {
  import spark.implicits._

  val schema1       = "event STRING, company STRING, followers INT, timestamp STRING"
  val schemaStruct1 = StructType.fromDDL(schema1)

  val inputStream1 = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "9999")
    .load()

  val inputStreamParsed1 = inputStream1
    .withColumn("parsed", from_json('value, schemaStruct1))
    .select("parsed.*")
    .withColumn("timestamp", 'timestamp.cast("timestamp"))
    .withWatermark("timestamp", "10 minutes")

  val schema2       = "event STRING, company STRING, employee_name STRING, df2_timestamp STRING"
  val schemaStruct2 = StructType.fromDDL(schema2)

  val inputStream2 = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "9998")
    .load()

  val inputStreamParsed2 = inputStream2
    .withColumn("parsed", from_json('value, schemaStruct2))
    .select("parsed.*")
    .withColumn("df2_timestamp", 'df2_timestamp.cast("timestamp"))
    .withWatermark("df2_timestamp", "10 minutes")

  val joinStream = inputStreamParsed1
    .alias("df1")
    .join(
      inputStreamParsed2.alias("df2"),
      expr("""
             |df1.company = df2.company
             |and timestamp >= df2_timestamp
             |and timestamp <= df2_timestamp + interval 1 hour
             |""".stripMargin)
    )

  joinStream.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", false)
    .queryName("NameParaSparkUI")
    .start
    .awaitTermination
}
