package utils

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}

import java.util.{Collections, Properties}
import scala.collection.JavaConverters._

trait KafkaConsumerTrait {

  def createConsumer(bootstrapServers: String): KafkaConsumer[String, String] = {
    val consumerProps = new Properties()
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "testGroup")
    consumerProps.put(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer"
    )
    consumerProps.put(
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer"
    )
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    new KafkaConsumer[String, String](consumerProps)
  }

  def consumeMessages(
      consumer: KafkaConsumer[String, String],
      topic: String,
      durationSeconds: Long
  ): Seq[ConsumerRecord[String, String]] = {
    consumer.subscribe(Collections.singletonList(topic))

    val records = consumer.poll(java.time.Duration.ofSeconds(durationSeconds)).asScala.toSeq
    consumer.close()
    records
  }

  def printMessages(records: Seq[ConsumerRecord[String, String]]): Unit = {
    records.foreach { record =>
      println(s"Consumed record with key ${record.key()} and value ${record.value()}")
    }
  }
}
