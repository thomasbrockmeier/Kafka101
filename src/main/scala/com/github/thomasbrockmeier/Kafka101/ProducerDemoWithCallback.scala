package com.github.thomasbrockmeier.Kafka101

import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}

object ProducerDemoWithCallback extends App {

  val properties = new Properties()
  properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](properties)

  val record = new ProducerRecord[String, String]("first_topic", "Hello, World!")

  for (_ <- 1 to 5) {
    producer.send(record, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =
        Option(exception) match {
          case Some(err) => err.printStackTrace()
          case _ =>
            println("Topic: " + metadata.topic())
            println("Partition: " + metadata.partition())
            println("Offset: " + metadata.offset())
            println("Timestamp: " + metadata.timestamp())
        }
    })
  }
  producer.close()

}
