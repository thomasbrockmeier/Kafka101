package com.github.thomasbrockmeier.Kafka101

import java.time.Duration
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer

object ConsumerDemoWithTreads extends App {

  class ConsumerRunnable(topic: String, bootstrapServers: String, groupId: String) extends Runnable {
    private val properties = new Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

    private val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](properties)
    consumer.subscribe(Collections.singletonList(topic))


    def run(): Unit = {
      println("ConsumerRunnable.run()")
      try {
        while (true) {
          consumer.poll(Duration.ofMillis(100)).forEach { println }
        }
      } catch {
        case e: WakeupException => println("Received shutdown signal!")
        case e: Throwable => println(e)
      } finally {
        consumer.close()
      }
    }

    def shutdown(): Unit = {
      consumer.wakeup()  // throws WakeUpException
    }
  }

  val topic = "first_topic"
  val bootstrapServers = "127.0.0.1:9092"
  val groupId = "my-sixth-application"

  val consumerRunnable = new ConsumerRunnable(topic, bootstrapServers, groupId)
  val thread: Thread = new Thread(consumerRunnable)

  sys.addShutdownHook({
    println("ShutdownHook")
    consumerRunnable.shutdown()
    thread.interrupt()
  })

  thread.start()

}
