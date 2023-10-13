package utils

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Properties
import scala.util.Random

// Clase de configuración
case class Config(
    incompletePercentage: Int,
    inconsistentPercentage: Int,
    fullyValidPercentage: Int,
    lateEventProbability: Double,
    intervalMs: Long
)

// Generador de Datos
class DataEventsGenerator(bootstrapServers: String, config: Config) extends LazyLogging {

  private val producerProps = new Properties()
  producerProps.put("bootstrap.servers", bootstrapServers)
  producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  private val producer = new KafkaProducer[String, String](producerProps)

  def startGenerating(): Unit = {
    while (true) {
      generateAndSendEvent()
      Thread.sleep(config.intervalMs)
    }
  }

  private def generateAndSendEvent(): Unit = {
    val randPercentage = Random.nextInt(100) + 1 // Genera un número entre 1 y 100

    val eventJson = if (randPercentage <= config.incompletePercentage) {
      generateIncompleteEvent()
    } else if (randPercentage <= config.incompletePercentage + config.inconsistentPercentage) {
      generateInconsistentEvent()
    } else {
      generateFullyValidEvent()
    }

    producer.send(new ProducerRecord[String, String]("event-topic", "key", eventJson))
  }

  // Métodos para generar los distintos tipos de eventos
  private def generateIncompleteEvent(): String =
    // ... lógica para generar un evento incompleto
    s"""{
       |"Int_11_o(001)_09": "134",
       |"TimeStamp":null
       |}""".stripMargin

  private def generateInconsistentEvent(): String =
    // ... lógica para generar un evento inconsistente
    s"""{
       |"Int_11_o(001)_09": "ABC",  // value no es un número
       |"TimeStamp":"${Instant.now().toString}"
       |}""".stripMargin

  private def generateFullyValidEvent(): String = {
    val isLateEvent = Random.nextDouble() < config.lateEventProbability
    val timestamp = if (isLateEvent) {
      Instant.now().minus(Random.nextInt(60) + 1L, ChronoUnit.MINUTES).toString
    } else {
      Instant.now().toString
    }

    s"""{
       |"Int_11_o(001)_09": "${Random.nextInt(1000)}",
       |"TimeStamp":"$timestamp"
       |}""".stripMargin
  }

  def close(): Unit = producer.close()
}
