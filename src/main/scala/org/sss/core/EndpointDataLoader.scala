package org.sss.core

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import java.net.URI
import java.net.http.HttpResponse.BodyHandlers
import java.net.http.{HttpClient, HttpRequest}
import scala.math.BigDecimal.RoundingMode

object EndpointDataLoader {

  private val spark = SparkSessionProvider.getSparkSession

  private val period1 = 915170400 // System.getenv("P1")
  private val period2 = 1704088800 // System.getenv("P2")
  private val interval = System.getenv("INTERVAL")
  private val symbol = System.getenv("SYMBOL")
  private val events = System.getenv("EVENTS")

  private val driver = System.getenv("DB_DRIVER")
  private val user = System.getenv("POSTGRES_USER")
  private val password = "airflow" // System.getenv("POSTGRES_PASSWORD") // don't use env's in prod either
  private val database = System.getenv("POSTGRES_DB")
  private val mode = System.getenv("DB_SAVE_MODE")   //! currently overwrite
  private val schema = Common.yahooAPISchema

  def makeV7Url(symbol: String, period1: Int, period2: Int, interval: String, events: String): String = {
    s"https://query1.finance.yahoo.com/v7/finance/download/" +
      s"${symbol}?" +
      s"period1=${period1}&" +
      s"period2=${period2}&" +
      s"interval=${interval}&" +
      s"events=${events}"
  }

  def makeV8Url(symbol: String, period1: Int, period2: Int, interval: String, events: String): String = {
    s"https://query2.finance.yahoo.com/v8/finance/chart/" +
      s"${symbol}?" +
      s"period1=${period1}&" +
      s"period2=${period2}&" +
      s"interval=${interval}&" +
      s"events=${events}"
  }

  private val dbUrl = s"jdbc:postgresql://sss-postgres:5432/$database"
  private val financeUrl = makeV8Url(symbol = symbol,
    period1 = period1,
    period2 = period2,
    interval = interval,
    events = events)

  // val url = makeV8Url(symbol, period1, period2, interval, events)

  println(s"make v8 ${makeV8Url(symbol = symbol, period1=period1, period2 = period2, events = events, interval = interval)}")
  println(s"make v7 ${makeV7Url(symbol = symbol, period1=period1, period2 = period2, events = events, interval = interval)}")

  def fromAPI(): DataFrame = {
    val client = HttpClient.newHttpClient()
    val request = HttpRequest.newBuilder()
    .uri(URI.create(financeUrl))
    .GET() // request type
    .build()

    val response = client.send(request, BodyHandlers.ofString)
    val splitIntoLines = response.body.split('\n')
    val rowElements = splitIntoLines.map(row => row.split(','))
    val rowData = rowElements.tail.map { rows =>
      val date = rows.head // The Date column
      val values = rows.tail.map(BigDecimal(_).setScale(4, RoundingMode.HALF_UP).toDouble)
      Row.fromSeq(date +: values)
    }
    // TODO: logging
    println(s"######################## Got return code: ${response.statusCode()}")

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

    println("########################  We've made the dataFrame")

    dataFrame
  }
  //noinspection AccessorLikeMethodIsUnit
  def toDatabase(dataFrame: DataFrame, table: String): Unit = {

    dataFrame.show(3)

    println("########################  Attempting to write")
    dataFrame.write
    .format("jdbc")
    .option("url", dbUrl)
    .option("dbtable", table)
    .option("user", user)
    .option("password", password)
    .option("driver", driver)
    .mode(mode)
    .save()

    println("@@@@@@@@@@@@@@@@@@@@ Write COMPLETED @@@@@@@@@@@@@@@@@@@@")
  }

  def fromDatabase(table: String): DataFrame = {

    val dataFrame = spark.read
      .format("jdbc")
      .option("driver", driver)
      .option("url", dbUrl)
      .option("user", user)
      .option("password", "airflow") // in local, dev environment, this is not a concern TODO: secret's manager
      .option("dbtable", table)
      .load()

    dataFrame
  }
}
