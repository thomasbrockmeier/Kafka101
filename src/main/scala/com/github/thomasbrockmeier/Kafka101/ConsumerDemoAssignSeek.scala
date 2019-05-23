package com.github.thomasbrockmeier.Kafka101

import java.time.Duration
import java.util.{Collections, Properties}

import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

object ConsumerDemoAssignSeek extends App {

  val bootstrapServers = "127.0.0.1:9092"
  val groupId = "my-fourth-application"
  val topic = "first_topic"

  val properties = new Properties()
  properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

  val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](properties)
  val topicPartition = new TopicPartition(topic, 0)

  consumer.assign(Collections.singletonList(topicPartition))
  consumer.seek(topicPartition, 0)

  def poll(): Option[Iterable[ConsumerRecord[String, String]]] = {
    Some(consumer.poll(Duration.ofMillis(100)).asScala.take(5))
  }

  
}
