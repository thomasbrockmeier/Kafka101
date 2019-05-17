package com.github.thomasbrockmeier.Kafka101

import java.util.Properties

import org.apache.kafka.clients.producer._

object ProducerDemoKeys extends App {

  val properties = new Properties()
  properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](properties)

  val topic = "first_topic"

  for (i <- 1 to 10) {
    producer.send(
      new ProducerRecord(topic, s"id_$i", s"Hello, World $i!"),
      new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =
          Option(exception) match {
            case Some(err) => err.printStackTrace()
            case _ =>
              println("Topic: " + metadata.topic())
              println("Partition: " + metadata.partition())
              println("Offset: " + metadata.offset())
              println("Timestamp: " + metadata.timestamp())
          }
      }
    ).get()
  }
  producer.close()

}
