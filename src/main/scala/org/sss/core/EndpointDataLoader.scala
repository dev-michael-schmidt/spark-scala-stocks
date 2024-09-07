package org.sss.core

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import java.net.URI
import java.net.http.HttpResponse.BodyHandlers
import java.net.http.{HttpClient, HttpRequest}
import scala.math.BigDecimal.RoundingMode

object EndpointDataLoader {

  // TODO: Robust logging
  private val spark = SparkSessionProvider.getSparkSession

  /* API */
  private val period1 = System.getenv("P1")
  private val period2 = System.getenv("P2")
  private val interval = System.getenv("INTERVAL")
  private val symbol = System.getenv("SYMBOL")
  private val events = System.getenv("EVENTS")
  private val schema = Common.yahooAPISchema

  /* Postgres */
  private val p_host = System.getenv("POSTGRES_HOST")
  private val p_port = System.getenv("POSTGRES_PORT")
  private val user = System.getenv("POSTGRES_USER")
  private val password = System.getenv("POSTGRES_PASSWORD") // TODO: WARNING - Unacceptable, Use a secrets manager
  private val mode = System.getenv("DB_SAVE_MODE")   // ! currently overwrite
  private val driver = System.getenv("DB_DRIVER")
  private val database = System.getenv("POSTGRES_DB")

  private val url = s"jdbc:postgresql://${p_host}:${p_port}/${database}"


  def fromAPI(): DataFrame = {
    /* "extract" */
    val client = HttpClient.newHttpClient()
    val request = HttpRequest.newBuilder()
    .uri(URI.create(s"${Common.YAHOO_FINANCE_ENDPOINT}" +
      s"$symbol?" +
      s"period1=$period1&" +
      s"period2=$period2&" +
      s"interval=$interval&" +
      s"events=$events"
      )
    )
    .GET() // request type
    .build()

    /* "extract" */
    val response = client.send(request, BodyHandlers.ofString)
    val splitIntoLines = response.body.split('\n')
    val rowElements = splitIntoLines.map(row => row.split(','))
    val rowData = rowElements.tail.map { rows =>
      val date = rows.head // The Date column
      val values = rows.tail.map(BigDecimal(_).setScale(4, RoundingMode.HALF_UP).toDouble)
      Row.fromSeq(date +: values) // date += prices
    }

    /* transform */
    val dataFrame = spark.createDataFrame(spark.sparkContext.parallelize(rowData), schema)
      .withColumn("symbol", lit(symbol))
      .withColumnRenamed("Open", "open")
      .withColumnRenamed("High", "high")
      .withColumnRenamed("Low", "low")
      .withColumnRenamed("Close", "close")
      .withColumnRenamed("Adj Close", "adj_close")
      .withColumnRenamed("Volume", "volume")
      .withColumn("event_ts", unix_timestamp(col("Date"), "yyyy-MM-dd").cast(LongType))
      .drop("Date")

    dataFrame
  }
  //noinspection AccessorLikeMethodIsUnit
  def toDatabase(dataFrame: DataFrame, table: String): Unit = {
    /* load */
    dataFrame.write
    .format("jdbc")
    .option("url", url)
    .option("dbtable", table)
    .option("user", user)
    .option("password", password)
    .option("driver", driver)
    .mode(mode)
    .save()
  }

  /* defined, but not used */
  def fromDatabase(table: String): DataFrame = {

    val dataFrame = spark.read
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("user", user)
      .option("password", password) // TODO: unacceptable secret's manager
      .option("dbtable", table)
      .load()

    dataFrame
  }
}
