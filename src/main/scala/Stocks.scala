import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, StringType, StructField, StructType}

import java.sql.Date
import java.text.SimpleDateFormat

import java.net.URI
import java.net.http.{HttpClient, HttpRequest}
import java.net.http.HttpResponse.BodyHandlers


object Stocks extends App {

  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("SparkScalaStocks")
    .getOrCreate()

  def stringToDate(dateStr: String): Date = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    new Date(format.parse(dateStr).getTime) // Unix Epoch
  }

  // Note: assert period2 > period1
  val period1 = 1546318799 // 2019-01-01 00:00:00
  val period2 = 1609477199 // 2024-05-31 23:59:59
  val interval = "1d"
  val events = "history"
  val symbol = "SPY"
  val client = HttpClient.newHttpClient()

  private val request = HttpRequest.newBuilder()
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

  private val response = client.send(request, BodyHandlers.ofString)

  private val splitIntoLines = response.body.split('\n')
  private val rowElements = splitIntoLines.map(row => row.split(','))

  private val colNames = rowElements.head.map(name => name.toLowerCase.replace(' ', '_'))
  private val data = rowElements.tail.map { rows =>
    val date = stringToDate(rows.head)
    val values = rows.tail.map(_.toDouble)
    Row.fromSeq(date +: values)
  }

  val schema = StructType(
     // first is date +: rest are doubles
    StructField("Date", DateType, nullable = false) +:
    colNames.tail.map(name => StructField(name, DoubleType, nullable = true))
  )

  val stocksDF = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

  val resultStocksDF = stocksDF
    .withColumn("symbol", lit(symbol))
    .withColumn("open", round(col("open")))
    .withColumn("open", round(col("open"), 4))
    .withColumn("close", round(col("close"), 4))
    .drop("high", "low", "adj_close")
}
