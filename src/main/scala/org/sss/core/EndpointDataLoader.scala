package org.sss.core

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import org.json4s.native.JsonMethods._
import org.json4s.{DefaultFormats, _}

import java.net.URI
import java.net.http.HttpResponse.BodyHandlers
import java.net.http.{HttpClient, HttpRequest}


object EndpointDataLoader {

  private val spark = SparkSessionProvider.getSparkSession
  implicit val formats: DefaultFormats.type = DefaultFormats   // Required for extracting values (json4s)

  /* API */
  private val period1 = System.getenv("P1").toInt
  private val period2 = System.getenv("P2").toInt
  private val interval = System.getenv("INTERVAL")
  private val symbol = System.getenv("SYMBOL")
  private val events = System.getenv("EVENTS")

  /* Postgres */
  private val p_host = System.getenv("POSTGRES_HOST")
  private val p_port = System.getenv("POSTGRES_PORT")
  private val user = System.getenv("POSTGRES_USER")
  private val password = "airflow" // System.getenv("POSTGRES_PASSWORD") // don't use env's in prod either
  private val driver = System.getenv("DB_DRIVER")
  private val database = System.getenv("POSTGRES_DB")
  private val mode = System.getenv("DB_SAVE_MODE")   //! currently overwrite
  private val dbUrl = s"jdbc:postgresql://${p_host}/${p_port}$database"

  private val schema = DataMappings.getYahooAPISchema
  private val financeUrl = DataMappings.makeV8Url(symbol = symbol,
    period1 = period1,
    period2 = period2,
    interval = interval,
    events = events)

  def fromV8API(): DataFrame = {
    val client = HttpClient.newHttpClient()
    val request = HttpRequest.newBuilder()
      .uri(URI.create(financeUrl))
      .GET() // request type
      .build()

    val response = client.send(request, BodyHandlers.ofString)
    val json = parse(response.body())

    val chart = (json \ "chart").asInstanceOf[JObject]
    val results = (chart \ "result").asInstanceOf[JArray]
    val quotes = for {
      JObject(resultItem) <- results.arr
      JField("indicators", JObject(indicators)) <- resultItem
      JField("timestamp", JArray(timestamp)) <- resultItem
      JField("quote", JArray(quoteItems)) <- indicators
    } yield {
      quoteItems.map { quote =>
        val high = (quote \ "high").extract[List[Double]]
        val low = (quote \ "low").extract[List[Double]]
        val open = (quote \ "open").extract[List[Double]]
        val close = (quote \ "close").extract[List[Double]]
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

    // TODO: logging
    println("########################  We've made the dataFrame")
    dataFrame.show(1)

    dataFrame
  }
