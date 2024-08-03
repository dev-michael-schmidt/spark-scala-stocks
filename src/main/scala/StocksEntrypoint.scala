import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import technicals.TechnicalBuilder

import java.net.URI
import java.net.http.HttpResponse.BodyHandlers
import java.net.http.{HttpClient, HttpRequest}
import java.sql.Date
import java.text.SimpleDateFormat
import scala.math.BigDecimal.RoundingMode

object StocksEntrypoint extends App {

 val spark = SparkSession.builder()
   .config("spark.master", "local")
   .appName("foo")
   .getOrCreate()
  //val spark = getSparkSession

  def stringToDate(dateStr: String): Date = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    new Date(format.parse(dateStr).getTime) // Unix Epoch
  }

  /* Notes:
   * Assert period2 > period1.
   * We use daily data.  Yahoo Finance's API has restrictions on how much data based on interval size.
   * While values [1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo] are acceptable, daily (1d)
   * granularity as this affords us several years. (nearly 25y as of this writing)
   */
  //val period1 = 946706400 // 2000-01-01 00:00:00
  val period1 = 1709272800 // 2024-03-01 00:00:00
  val period2 = 1717217999 // 2024-05-31 23:59:59
  val interval = "1d" // Valid intervals: [1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo,
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
  private val rowData = rowElements.tail.map { rows =>
    val date = stringToDate(rows.head)
    val values = rows.tail.map(BigDecimal(_).setScale(4, RoundingMode.HALF_UP).toDouble)
    Row.fromSeq(date +: values)
  }

  val schema = StructType(
      // first is date +: rest are doubles
    StructField("date", DateType, nullable = false) +:
    colNames.tail.map(name => StructField(name, DoubleType, nullable = true))
  )

  val stockHistoryDF = spark.createDataFrame(spark.sparkContext.parallelize(rowData), schema)
    .withColumn("symbol", lit(symbol))

  val stockHistoryWithTechnicals = TechnicalBuilder(stockHistoryDF)
    .SMA(10)
    .EMA(10)
    .build()

  val foo = RestAPIBatchFetch(1722607200, "SPY")

  stockHistoryWithTechnicals.show()
}
