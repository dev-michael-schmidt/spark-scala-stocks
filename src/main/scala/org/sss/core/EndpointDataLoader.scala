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

  private val period1 = System.getenv("P1")
  private val period2 = System.getenv("P2")
  private val interval = System.getenv("INTERVAL")
  private val symbol = System.getenv("SYMBOL")
  private val events = System.getenv("EVENTS")

  private val driver = System.getenv("DB_DRIVER")
  private val user = System.getenv("POSTGRES_USER")
  private val password = System.getenv("POSTGRES_PASSWORD")
  private val database = System.getenv("POSTGRES_DB")
  private val mode = System.getenv("DB_SAVE_MODE")   //! currently overwrite

  private val url = s"jdbc:postgresql://localhost:5432/$database" // TODO actually parameterize this, hardcoding is terrible!!

  def fromAPI(): DataFrame = {
    val client = HttpClient.newHttpClient()
    val request = HttpRequest.newBuilder()
    .uri(URI.create(s"https://query1.finance.yahoo.com/v7/finance/download/" +
      s"$symbol?" +
      s"period1=$period1&" +
      s"period2=$period2&" +
      s"interval=$interval&" +
      s"events=$events"
      )
    )
    .GET() // request type
    .build()
    val response = client.send(request, BodyHandlers.ofString)
    val splitIntoLines = response.body.split('\n')
    val rowElements = splitIntoLines.map(row => row.split(','))
    val rowData = rowElements.tail.map { rows =>
      val date = rows.head // The Date column
      val values = rows.tail.map(BigDecimal(_).setScale(4, RoundingMode.HALF_UP).toDouble)
      Row.fromSeq(date.toString +: values)
    }

    // val schema = spark.read.json("src/main/resources/schemas/y_prices.json").schema
    val schema = StructType(Array(
      StructField("Date", StringType, nullable = true),
      StructField("Open", DoubleType, nullable = true),
      StructField("High", DoubleType, nullable = true),
      StructField("Low", DoubleType, nullable = true),
      StructField("Close", DoubleType, nullable = true),
      StructField("Adj Close", DoubleType, nullable = true),
      StructField("Volume", DoubleType, nullable = true)
    ))
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

  def toDatabase(dataFrame: DataFrame, table: String) = {

    dataFrame.write
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("dbtable", table)
      .mode(mode) // currently overwrite (or see val above)
      .save()
  }

  def fromDatabase(table: String) = {

    val dataFrame = spark.read
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("dbtable", table)
      .load()

    dataFrame
  }
}
