package org.sss.communication

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.sss.utilities.{JsonMapDeserializer, JsonMapSerializer}

import java.time.Duration
import java.util.Collections.singletonList
import java.util.Properties
import scala.jdk.CollectionConverters.iterableAsScalaIterableConverter
import scala.util.control.Breaks.break

object TestStreamer {

  def main(args: Array[String]): Unit = {

    val bootStrap1 = System.getenv("BROKER1")
    val bootStrap2 = System.getenv("BROKER2")
    val bootStrap3 = System.getenv("BROKER3")

    val topic: String = "my-topic"
    // val bootstrapServers: String = "localhost:29092,localhost:39092,localhost-3:49092"//"broker-1:29092,broker-2:39092,broker-3:49092"
    val productionBootstrapServers = s"${bootStrap1}," +
      s"${bootStrap2}," +
      s"${bootStrap3}"

    val localStrapServers = "localhost:29092,localhost:39092,localhost:49092"

    val bootstrapServers = localStrapServers

    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[JsonMapSerializer].getName)
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all") //

    val consumerProps = new Properties()
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[JsonMapDeserializer].getName)
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "scala-kafka-consumer-group")
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") // To start from the earliest message
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")

    val producer = new KafkaProducer[String, Map[String, String]](producerProps)
    val consumer = new KafkaConsumer[String, Map[String, String]](consumerProps)
    var partitionsAssigned = false
    consumer.subscribe(
      singletonList(topic),
      new ConsumerRebalanceListener {
        override def onPartitionsRevoked(partitions: java.util.Collection[TopicPartition]): Unit = {}
        override def onPartitionsAssigned(partitions: java.util.Collection[TopicPartition]): Unit = {
          partitionsAssigned = true
        }
      }
    )

    // Wait until partitions are assigned
    while (!partitionsAssigned) {
      consumer.poll(Duration.ofMillis(100))
    }
    println("Partition assigned")

    // Produce messages in advance
    producer.send(new ProducerRecord[String, Map[String, String]](topic, Map("keyz" -> "value1")))
    producer.send(new ProducerRecord[String, Map[String, String]](topic, Map("keyz" -> "value2")))

    while (true) {
      val records = consumer.poll(Duration.ofMillis(100))
      records.asScala.foreach { record =>
        println(s"Key: ${record.key()}, Value: ${record.value()}")
        break
      }
    }
  }
}
