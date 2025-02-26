package org.sss.core

import courier.Defaults.executionContext
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.sss.utilities.JsonMapSerializer

import java.util.Properties
import scala.concurrent.{Future, Promise}

object TradeEvent {

  private val bootStrap1 = Option(System.getenv("BROKER1")).getOrElse("BROKER1")
  private val bootStrap2 = Option(System.getenv("BROKER2")).getOrElse("BROKER2")
  private val bootStrap3 = Option(System.getenv("BROKER3")).getOrElse("BROKER3")

  private val topic = "email-service.notifications" // global on constant

  val bootstrapServers: String = "localhost:29092,localhost:39092,localhost-3:49092" //"broker-1:29092,broker-2:39092,broker-3:49092"
  val botstrapServers = s"${bootStrap1}," +
    s"${bootStrap2}," +
    s"${bootStrap3}"

  val localStrapServers = "localhost:29092,localhost:39092,localhost:49092"

  private val producerProps = new Properties()
  producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[JsonMapSerializer].getName)
  producerProps.put(ProducerConfig.ACKS_CONFIG, "all")

  val producer = new KafkaProducer[String, Map[String, String]](producerProps)

  // Helper method to wrap producer.send into a Scala Future.
  def emit(record: ProducerRecord[String, Map[String, String]]): Future[RecordMetadata] = {
    val promise = Promise[RecordMetadata]()
    producer.send(record, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception != null) promise.failure(exception)
        else promise.success(metadata)
      }
    })
    promise.future
  }

  // Using the helper to send a message and then attach onComplete.
  val record = new ProducerRecord[String, Map[String, String]](topic, Map("keyz" -> "value1"))
  emit(record).onComplete {
    case scala.util.Success(metadata) =>
      println(s"Message sent successfully: $metadata")
    case scala.util.Failure(error) =>
      println(s"Failed to send message: ${error.getMessage}")
  }
}
