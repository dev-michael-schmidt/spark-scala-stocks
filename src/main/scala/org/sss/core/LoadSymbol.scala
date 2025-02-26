package org.sss.core

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.sss.communication.MailPerson
import org.sss.technicals.PipelineActions

object LoadSymbol {

  private val sym = System.getenv("SYMBOL")
  private val table = sym

  def main(args: Array[String]): Unit = {

    val spy = new DataPipeline()
      .loadFromUrl(sym, 1727833579, 1730511979, "1d")
      .interpolate
      .EMA(10)
      .SMA(10)

    val foo: Map[String, String] = Map(
      "foo" -> "bar",
      "baz" -> "bonkers"
    )

    val topic = "email-service.notifications"

    val trade = new ProducerRecord[String, Map[String, String]](topic, foo)

    TradeEvent.emit(trade)

    spy.writeToDatabase(table)
    println("we've reached the end")
  }
}