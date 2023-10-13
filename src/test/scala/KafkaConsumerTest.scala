import KafkaConsumerTest.{configSignalOK, dataEventGeneratorConf, producerTimeoutInMillis}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import utils.{Config, DataEventsGenerator, KafkaConsumerTrait}

object KafkaConsumerTest {

  private val producerTimeoutInMillis = 5000

  private val dataEventGeneratorConf = Config(
    incompletePercentage = 0,
    inconsistentPercentage = 0,
    fullyValidPercentage = 100,
    lateEventProbability = 0.05,
    intervalMs = 500
  )

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
}

class KafkaConsumerTest extends AnyFlatSpec with Matchers with KafkaConsumerTrait {

  "consumer" should "rehydrate events with config signals" in {

    val kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))

    kafkaContainer.start()

    try {
      val bootstrapServers = kafkaContainer.getBootstrapServers

      // Configura e inicia el generador de datos
      val dataEventGenerator = new DataEventsGenerator(bootstrapServers, dataEventGeneratorConf)

      // Inicia un hilo para generar datos en el background mientras el test está corriendo
      val producerEventThread = new Thread(() => dataEventGenerator.startGenerating())
      producerEventThread.start()

      // Esperar para asegurar que algunos mensajes son producidos
      Thread.sleep(producerTimeoutInMillis)

      // Detén la producción de mensajes y cierra el productor
      producerEventThread.interrupt()
      dataEventGenerator.close()

      // Definir el esquema del evento y la señal
      val eventSchema = (new StructType)
        .add("Int_11_o(001)_09", StringType)
        .add("TimeStamp", StringType)

      val signalSchema = (new StructType)
        .add("signalKey", StringType)
        .add("signalName", StringType)
        .add("dataType", StringType)
        .add("plant", StringType)
        .add("line", StringType)

      // Inicializacion de spark
      val spark = SparkSession
        .builder()
        .appName("processingModel")
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._

      // Leer los mensajes de Kafka
      val rawEvents: DataFrame = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("subscribe", "event-topic")
        .load()
        .selectExpr("CAST(value AS STRING)")
        .select(from_json($"value", eventSchema).as("data"))
        .select("data.*")

      // Asumiendo que `configSignalOK` es accesible, parsearlo y cachearlo
      val signalDF = spark.read.schema(signalSchema).json(Seq(configSignalOK).toDS)
      signalDF.cache()

      // Tu lógica para tratar los diferentes errores
      def handleErrors(df: DataFrame): DataFrame =
        // Tu lógica aquí
        df

      // Procesar y realizar join
      val processedEvents: DataFrame = rawEvents
        .transform(handleErrors)
        .withColumn("Int_11_o(001)_09", $"Int_11_o(001)_09".cast(IntegerType))
        .join(broadcast(signalDF), $"Int_11_o(001)_09" === $"signalKey")
        .withColumn("value", $"Int_11_o(001)_09")
        .select("signalName", "value", "plant", "line", "TimeStamp")

      val finalOutput: DataFrame = processedEvents.selectExpr(
        "CAST(signalName AS STRING) AS key",
        "to_json(struct(*)) AS value"
      )

      // Escribir los resultados de nuevo a Kafka o almacenarlo según necesites
      finalOutput.writeStream
        .outputMode("append")
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("topic", "processed-event-topic")
        .option("checkpointLocation", "tmp/checkpoint/")
        .start()

      spark.streams.awaitAnyTermination()

      Thread.sleep(5000)

      val consumer = createConsumer(bootstrapServers)
      // Suscríbete al topic y consume mensajes
      val messages = consumeMessages(consumer, "processed-event-topic", 5)
      printMessages(messages)

    } finally {
      kafkaContainer.stop()
    }

  }
}
