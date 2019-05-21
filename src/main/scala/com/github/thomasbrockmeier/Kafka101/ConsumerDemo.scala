package com.github.thomasbrockmeier.Kafka101

import java.time.Duration
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

object ConsumerDemo extends App {

  val bootstrapServers = "127.0.0.1:9092"
  val groupId = "my-fourth-application"
  val topic = "first_topic"

  val properties = new Properties()
  properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
  properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

  val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](properties)
  consumer.subscribe(Collections.singletonList(topic))

  while (true) {
    consumer.poll(Duration.ofMillis(100)).forEach { println }
  }
}
