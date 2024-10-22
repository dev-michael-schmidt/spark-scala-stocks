package org.sss.core

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, MetadataBuilder}
import org.apache.spark.sql.{DataFrame, Row}
import org.json4s.native.JsonMethods._
import org.json4s.{DefaultFormats, _}

import java.net.URI
import java.net.http.HttpResponse.BodyHandlers
import java.net.http.{HttpClient, HttpRequest}
import scala.math.BigDecimal.RoundingMode


object EndpointDataLoader {

  private val spark = SparkSessionProvider.getSparkSession
  implicit val formats: DefaultFormats.type = DefaultFormats   // Required for extracting values (json4s)

  /* API */
  private val period1 = System.getenv("P1").toLong
  private val period2: Long = System.getenv("P2").toInt // System.currentTimeMillis / 1000
  private val interval = System.getenv("INTERVAL")
  private val symbol = System.getenv("SYMBOL")
  private val events = System.getenv("EVENTS")

  /* Postgres */
  private val p_host = "localhost" //System.getenv("POSTGRES_HOST")
  private val p_port = System.getenv("POSTGRES_PORT")
  private val user = System.getenv("POSTGRES_USER")
  private val password = "airflow" // System.getenv("POSTGRES_PASSWORD") // don't use env's in prod either
  private val driver = System.getenv("DB_DRIVER")
  private val database = System.getenv("POSTGRES_DB")
  private val mode = System.getenv("DB_SAVE_MODE")   //! currently overwrite
  private val dbUrl = s"jdbc:postgresql://${p_host}:${p_port}/$database"

  private val schema = DataMappings.getYahooAPISchema
  private val financeUrl = DataMappings.makeV8Url(symbol = symbol,
    period1 = period1,
    period2 = period2,
    interval = interval,
    events = events)

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

  def fromV8API(): DataFrame = {
    val client = HttpClient.newHttpClient()
    val request = HttpRequest.newBuilder()
      .uri(URI.create(financeUrl))
      .GET() // request type
      .build()

    val response = client.send(request, BodyHandlers.ofString)
    val json = parse(response.body())

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
    val dataFrame = spark.createDataFrame(spark.sparkContext.parallelize(flatData.flatten), schema)
    dataFrame
  }

  def fromV7Api(): DataFrame = {
    // Todo: remove duplicated code
    val client = HttpClient.newHttpClient()
    val request = HttpRequest.newBuilder()
      .uri(URI.create(DataMappings.makeV7Url(
        symbol = symbol,
        period1 = period1,
        period2 = period2,
        interval = interval,
        events = events
      ))
      )
      .GET() // request type
      .build()

    val response = client.send(request, BodyHandlers.ofString)
    val splitIntoLines = response.body.split('\n')
    val rowElements = splitIntoLines.map(row => row.split(','))
    val rowData = rowElements.tail.map { rows =>
      val date = rows.head // The Date column
      val values = rows.tail.map(BigDecimal(_).setScale(4, RoundingMode.HALF_UP).toDouble)
      Row.fromSeq(date +: values) // date += prices
    }

    val dataFrame = spark.createDataFrame(spark.sparkContext.parallelize(rowData), schema)
      .withColumn("symbol", lit(symbol))
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

  /* defined, but not used */
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
