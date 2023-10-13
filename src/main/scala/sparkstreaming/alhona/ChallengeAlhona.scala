package sparkstreaming.alhona

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SQLContext}
import utils.SparkSetup
import org.apache.spark.sql.functions._

/* Conociendo el nombre de las columnas */
object ChallengeAlhonaExample1 extends App with SparkSetup with LazyLogging {
  implicit val sqlContext: SQLContext = spark.sqlContext
  import spark.implicits._

  val alertThreshold      = 130
  val alertWindowDuration = "10 seconds"

  private val configSignalOK =
    """
      |{
      |	"signalKey": "Int_11_o(001)_09",
      |	"signalName": "WeldsCounter",
      |	"dataType": "Integer",
      |	"plant": "Girona",
      |	"line": "Hot Stamping"
      |}
      |""".stripMargin

  // Asumiendo que `configSignalOK` es accesible, parsearlo y cachearlo
  val signalDF = spark.read
    .schema(
      "signalKey String, signalName String, dataType String, plant String, line String"
    )
    .json(Seq(configSignalOK).toDS)
    .cache()

  val inputStream = spark.readStream
    .schema("key STRING, TimeStamp STRING")
    .json("src/main/resources/data/sensor-data")

  // Ojo hay que conocer el nombre de las columnas imagino que no será así de fácil :-)
  val transformedInputStream = inputStream
    .toDF()
    .melt(
      Array(inputStream.col("`Int_11_o(001)_09`"), inputStream.col("TimeStamp")),
      Array(inputStream.col("`Int_11_o(001)_09`")),
      "signalKey",
      "value"
    )

  val joinedStream =
    transformedInputStream
      .join(signalDF, "signalKey")

  val processedStream = joinedStream
    .selectExpr(
      "cast(value as Integer) as intValue",
      "cast(TimeStamp as timestamp) as timestamp",
      "signalName",
      "plant",
      "line"
    )

  val windowedAlerts = processedStream
    .withWatermark("timestamp", "10 seconds")
    .groupBy(
      window($"timestamp", alertWindowDuration, "10 seconds"),
      $"signalName",
      $"plant",
      $"line"
    )
    .agg(min("intValue").as("minValue"))
    .withColumn("isAlert", when(col("minValue") > alertThreshold, true).otherwise(false))

  // Alerta cuando hay una fila con isAlert=true.
  val query = windowedAlerts.writeStream
    .outputMode("append")
    .foreachBatch { (batchDF: DataFrame, _: Long) =>
      if (batchDF.filter("isAlert = true").count() > 0) {
        logger.error(
          "ALERTA: El valor ha excedido el umbral durante la ventana de tiempo especificada!"
        )
      }
    }
    .start()

  query.processAllAvailable()
}

object ChallengeAlhonaExample2 extends App with SparkSetup with LazyLogging {
  implicit val sqlContext: SQLContext = spark.sqlContext
  import spark.implicits._

  val alertThreshold      = 130
  val alertWindowDuration = "10 seconds"

  private val configSignalOK =
    """
      |{
      |	"signalKey": "Int_11_o(001)_09",
      |	"signalName": "WeldsCounter",
      |	"dataType": "Integer",
      |	"plant": "Girona",
      |	"line": "Hot Stamping"
      |}
      |""".stripMargin

  // Asumiendo que `configSignalOK` es accesible, parsearlo y cachearlo
  val signalDF = spark.read
    .schema(
      "signalKey String, signalName String, dataType String, plant String, line String"
    )
    .json(Seq(configSignalOK).toDS)
    .cache()

  val sampleDF = spark.read
    .schema("key STRING, TimeStamp STRING") // Ajusta el esquema a tus necesidades
    .json("src/main/resources/data/sensor-data")
    .limit(1) // lee solo una fila

  val columns: Array[String] = sampleDF.columns
  val signalKeyColumnName    = columns.filterNot(_ == "TimeStamp").head // Asumiendo solo hay dos columnas

  val inputStream = spark.readStream
    .schema(s"$signalKeyColumnName STRING, TimeStamp STRING")
    .json("src/main/resources/data/sensor-data")

  val transformedInputStream = inputStream
    .toDF()
    .melt(
      Array(col(signalKeyColumnName), col("TimeStamp")),
      Array(col(signalKeyColumnName)),
      "signalKey",
      "value"
    )

  val joinedStream =
    transformedInputStream
      .join(signalDF, "signalKey")

  val processedStream = joinedStream
    .selectExpr(
      "cast(value as Integer) as intValue",
      "cast(TimeStamp as timestamp) as timestamp",
      "signalName",
      "plant",
      "line"
    )

  val windowedAlerts = processedStream
    .withWatermark("timestamp", "10 seconds")
    .groupBy(
      window($"timestamp", alertWindowDuration, "10 seconds"),
      $"signalName",
      $"plant",
      $"line"
    )
    .agg(min("intValue").as("minValue"))
    .withColumn("isAlert", when(col("minValue") > alertThreshold, true).otherwise(false))

  // Alerta cuando hay una fila con isAlert=true.
  val query = windowedAlerts.writeStream
    .outputMode("append")
    .foreachBatch { (batchDF: DataFrame, _: Long) =>
      if (batchDF.filter("isAlert = true").count() > 0) {
        logger.error(
          "ALERTA: El valor promedio excede el umbral durante la ventana de tiempo especificada!"
        )
      }
    }
    .start()

  query.processAllAvailable()

  /*
  inputStream.toDF.writeStream.foreachBatch { (df: DataFrame, id: Long) =>
    val keyColumnName = df.columns.filter(_ != "TimeStamp").head

    // Crear una expresión SQL para stack usando el nombre de columna dinámico.
    val transformed = df.selectExpr(
      s"stack(1, '$keyColumnName', `$keyColumnName`) as (signalKey, value)",
      "TimeStamp"
    )
    //signalDF.show(false)
    // Utilizar el DataFrame transformado, por ejemplo, escribiéndolo en la consola.
    val join = transformed.join(signalDF)

    join.write
      .format("console")
      .option("truncate", "false")
      .mode("append")
      .save()

  } */

}
