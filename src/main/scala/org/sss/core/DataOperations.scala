package org.sss.core

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, MetadataBuilder}
import org.apache.spark.sql.{DataFrame, Row}
import org.json4s.native.JsonMethods._
import org.json4s._
import org.sss.core.DataOperations.{createUrl, fetchDataFromUrl, toDatabase}

import java.net.URI
import java.net.http.HttpResponse.BodyHandlers
import java.net.http.{HttpClient, HttpRequest}
import scala.math.BigDecimal.RoundingMode

case class DataOperations(val tickerSymbol: String,
                          val period1: Long,
                          val period2: Long,
                          val interval: String) extends StockMeta {

  def fetchData: DataFrame = {

    val financeURL: String = createUrl(tickerSymbol, period1, period2, interval, EVENTS, API_VERSION)
    val result: DataFrame = fetchDataFromUrl(financeURL)
    result
  }

  def pushData(df: DataFrame, table: String): Unit = {
    val df = copy(table).fetchData
    toDatabase(df, table)
  }

  def pushDatal(ticketSymbol: String): Unit = {
    val df = copy(TABLE).fetchData
    toDatabase(df, ticketSymbol)
  }
}

object DataOperations {

  /* Postgres */
  private val p_host = "localhost" //System.getenv("POSTGRES_HOST")
  private val p_port = System.getenv("POSTGRES_PORT")

  val user: String = System.getenv("POSTGRES_USER")
  private val password = "airflow" // System.getenv("POSTGRES_PASSWORD") // don't use env's in prod either

  val database: String = System.getenv("POSTGRES_DB")
  val mode: String = System.getenv("DB_SAVE_MODE") // ! currently overwrite

  private val driver = System.getenv("DB_DRIVER")
  private val dbUrl = s"jdbc:postgresql://${p_host}:${p_port}/$database"

  private val spark = SparkSessionProvider.getSparkSession
  implicit val formats: DefaultFormats.type = DefaultFormats // Required for extracting values (json4s)

 private val schema = DataMappings.getYahooAPISchema

 def createUrl(sym: String,
               period1: Long,
               period2: Long,
               interval: String,
               events: String,
               version: String): String = {
   val url: String = version.toLowerCase match {
     case "v8" => DataMappings.makeV8Url(sym, period1, period2, interval, events)
     case "v7" => DataMappings.makeV7Url(sym, period1, period2, interval, events)
     case _ => throw new IllegalArgumentException(s"Unsupported API version: $version")
   }
   url
 }
  private def fetchDataFromUrl(url: String): DataFrame = {
    val client = HttpClient.newHttpClient()
    val request = HttpRequest.newBuilder()
      .uri(URI.create(url))
      .GET() // request type
      .build()

    // Send the request and get the response as a String
    val responseBody: String = client.send(request, BodyHandlers.ofString).body()
    val result = url match {
      case u if url.contains("v7") => fromV7API(responseBody)
      case u if url.contains("v8") => fromV8API(responseBody)
    }
    result
  }

  private val roundPrecision = 4
  def roundAt(precision: Int)(n: Double): Double = { val s = math pow (10, precision); (math round n * s) / s }

  def flattenJson(json: JObject, prefix: String = ""): Map[String, Any] = {
    json.obj.flatMap {
      case (key, JString(value)) =>
        Map(s"$prefix$key" -> value)
      case (key, JInt(value)) =>
        Map(s"$prefix$key" -> value)
      case (key, JBool(value)) =>
        Map(s"$prefix$key" -> value)
      case (key, JDouble(value)) =>
        Map(s"$prefix$key" -> value)

      // Recursive call to handle nested objects.
      // Note: the recursive calls need to happen *last* for Tail-end recursion
      case (key, JObject(fields)) =>
        flattenJson(JObject(fields), s"$prefix$key.") // recursive call
      case (key, JArray(values)) =>
        values.zipWithIndex.flatMap {
          case (nestedValue, idx) =>
            flattenJson(JObject(key -> nestedValue), s"$prefix$key[$idx].") // Recursive call
        }.toMap
      case (key, _) =>
        // Handle other cases as needed
        Map.empty[String, Any]
    }.toMap
  }

  def fromV8API(responseBody: String): DataFrame = {

    val json = parse(responseBody)

    val chart = (json \ "chart").asInstanceOf[JObject]
    val result = (chart \ "result").asInstanceOf[JArray]
    val metadataJObject = (result \ "meta") // JValue
      .asInstanceOf[JArray]                 // JArray
      .arr                                  // List[JsonAST.JValue]
      .head                                 // JsonAST.JValue
      .asInstanceOf[JObject]                // JObject

    val metaData = flattenJson(metadataJObject)
    val quotes = for {
      JObject(resultItem) <- result.arr
      JField("timestamp", JArray(timestamp)) <- resultItem
      JField("indicators", JObject(indicators)) <- resultItem
      JField("quote", JArray(quoteItems)) <- indicators
    } yield {
      quoteItems.map { quote =>
        val high = (quote \ "high")
          .extract[List[Double]]
          .map(n => roundAt(roundPrecision)(n))
        val low = (quote \ "low")
          .extract[List[Double]]
          .map(n => roundAt(roundPrecision)(n))
        val open = (quote \ "open")
          .extract[List[Double]]
          .map(n => roundAt(roundPrecision)(n))
        val close = (quote \ "close")
          .extract[List[Double]]
          .map(n => roundAt(roundPrecision)(n))
        val volume = (quote \ "volume").extract[List[Long]]
        (timestamp.map(_.extract[Long]), high, low, open, close, volume)
      }
    }

      // The data must be explicitly flattened before it is mapped
      val flatData = quotes.flatten.map {
        case (timestamp, highs, lows, opens, closes, volumes) =>
          timestamp.zipWithIndex.map { case (time, idx) =>
            Row(time, highs(idx), lows(idx), opens(idx), closes(idx), volumes(idx))
          }
      }

      // While it appears hacky, the nature of the JSON schema is deeply nested, so it must be flattened one more time
      val dataFrame: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(flatData.flatten), schema)
        .withColumn("symbol", lit("symbol"))
      dataFrame
    }

  def fromV7API(responseBody: String): DataFrame = {

      val splitIntoLines = responseBody.split('\n')
      val rowElements = splitIntoLines.map(row => row.split(','))
      val rowData = rowElements.tail.map { rows =>
        val date = rows.head // The Date column
        val values = rows.tail.map(BigDecimal(_).setScale(4, RoundingMode.HALF_UP).toDouble)
        Row.fromSeq(date +: values) // date += prices
      }

      val dataFrame = spark.createDataFrame(spark.sparkContext.parallelize(rowData), schema)
        .withColumn("symbol", lit("symbol"))
        .withColumnRenamed("Open", "open")
        .withColumnRenamed("High", "high")
        .withColumnRenamed("Low", "low")
        .withColumnRenamed("Close", "close")
        .withColumnRenamed("Adj Close", "adj_close")
        .withColumnRenamed("Volume", "volume")
        .withColumn("tstamp", unix_timestamp(col("Date"), "yyyy-MM-dd").cast(LongType))
        .drop("Date")

      dataFrame
    }

  //noinspection AccessorLikeMethodIsUnit
  def toDatabase(dataFrame: DataFrame, table: String, mode: String = "overwrite"): Unit = {

      dataFrame.write
        .format("jdbc")
        .option("url", dbUrl)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .option("driver", driver)
        .mode(mode)
        .save()
    }

  def fromDatabase(table: String): DataFrame = {

    val dataFrame = spark.read
      .format("jdbc")
      .option("driver", driver)
      .option("url", dbUrl)
      .option("user", user)
      .option("password", password) // TODO: unacceptable secret's manager
      .option("dbtable", table)
      .load()
    dataFrame
  }
}
