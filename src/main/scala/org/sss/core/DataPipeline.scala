package org.sss.core

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, lit, unix_timestamp}
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.log4j.Logger
import org.json4s.JsonAST.{JBool, JDouble, JField, JInt, JString}
import org.json4s.native.JsonMethods.parse
import org.json4s.{DefaultFormats, JArray, JObject}

import java.net.URI
import java.net.http.HttpResponse.BodyHandlers
import java.net.http.{HttpClient, HttpRequest}
import scala.math.BigDecimal.RoundingMode

class DataPipeline(private var dataFrame: DataFrame = null) extends DataPipelineabstract {

  private val logger = Logger.getLogger(getClass.getName)
  private val spark = SparkSessionProvider.getSparkSession

  import spark.implicits._
  implicit val formats: DefaultFormats.type = DefaultFormats   // Required for extracting values (json4s)


  /* Postgres */
  val user: String = "airflow" //getEnvVariable(System.getenv("POSTGRES_USER"))// .getOrElse("'POSTGRES_USER' not set")
  val password = "airflow" //getEnvVariable()//Option(System.getenv("POSTGRES_PASSWORD"))//.getOrElse("'POSTGRES_PASSWORD' not set") // don't use env's in prod either

  val host = Option(System.getenv("POSTGRES_HOST")).getOrElse("localhost")
  val port: String = System.getenv("POSTGRES_PORT")

  val format: String = "jdbc"
  val schema: StructType = DataMappings.getYahooAPISchema

  val database: String = System.getenv("POSTGRES_DB")
  val mode: String = System.getenv("DB_SAVE_MODE") // ! currently overwrite

  val driver = System.getenv("DB_DRIVER")
  val dbUrl = s"jdbc:postgresql://${host}:${port}/$database"


  override def loadFromDatabase(table: String): DataPipelineabstract = {
    dataFrame = spark.read
      .format("jdbc")
      .option("driver", driver)
      .option("url", dbUrl)
      .option("user", user)
      .option("password", password) // TODO: unacceptable secret's manager
      .option("dbtable", table)
      .load()

    this
  }

  override def loadFromUrl(tickerSymbol: String,
                           period1: Long,
                           period2: Long,
                           interval: String,
                           events: String = "history",
                           apiVersion: String = "v8"): DataPipeline = {
    val url = createUrl(tickerSymbol, period1, period2, interval)
    val client = HttpClient.newHttpClient()
    val request = HttpRequest.newBuilder()
      .uri(URI.create(url))
      .GET() // request type
      .build()

    // Send the request and get the response as a String
    // val responseBody: String = client.send(request, BodyHandlers.ofString).body()
    val response = client.send(request, BodyHandlers.ofString())
    val responseBody = if (response.statusCode() == 200) {
      response.body()
    } else {
      throw new RuntimeException(s"response return code ${response.statusCode()}, not 200")
    }

    dataFrame = apiVersion.toLowerCase match {
      case "v8" => fromV8API(responseBody)
      case "v7" => fromV7API(responseBody)
      case _ => {
        println("foo")
        throw new RuntimeException("foo")
      }
    }
    this
  }

  override def interpolate(): DataPipelineabstract = {

    val windowSpec = Window.orderBy("tstamp")
    val dataWithCurrPrev = this.dataFrame
      .withColumn("prev_tstamp", lag("tstamp", 1).over(windowSpec))
      .withColumn("prev_high", lag("high", 1).over(windowSpec))
      .withColumn("prev_low", lag("low", 1).over(windowSpec))
      .withColumn("prev_open", lag("open", 1).over(windowSpec))
      .withColumn("prev_close", lag("close", 1).over(windowSpec))
      .withColumn("prev_volume", lag("volume", 1).over(windowSpec))
      .withColumn("diff", col("tstamp") - col("prev_tstamp"))

    val contiguousDataframe = dataWithCurrPrev.flatMap(row => {
      val tstamp = row.getAs[Long]("tstamp")
      val prevTstamp = row.getAs[Long]("prev_tstamp")
      val high = row.getAs[Double]("high")
      val prevHigh = row.getAs[Double]("prev_high")
      val low = row.getAs[Double]("low")
      val prevLow = row.getAs[Double]("prev_low")
      val open = row.getAs[Double]("open")
      val prevOpen = row.getAs[Double]("prev_open")
      val close = row.getAs[Double]("close")
      val prevClose = row.getAs[Double]("prev_close")
      val volume = row.getAs[Long]("volume")
      val diff = row.getAs[Long]("diff")

      // If the diff is greater than 86400 (1 day), split into two rows
      if (diff > 86400) {
        val interpolatedTstamp = prevTstamp + diff / 2
        val interpolatedHigh = (high + prevHigh) / 2
        val interpolatedLow = (low + prevLow) / 2
        val interpolatedOpen = (open + prevOpen) / 2
        val interpolatedClose = (close + prevClose) / 2

        // Return both the interpolated row (with volume = 0) and the original row since.
        // this has the effect of providing what is already present, and also adds the interpolated values
        Seq(
          (interpolatedTstamp, interpolatedHigh, interpolatedLow, interpolatedOpen, interpolatedClose, 0L), // Volume set to 0 for interpolated row
          (tstamp, high, low, open, close, volume) // Original row
        )
      } else {
        // Since there is no gap, return the original row
        Seq((tstamp, high, low, open, close, volume))
      }
    }).toDF("tstamp", "high", "low", "open", "close", "volume")

    dataFrame = contiguousDataframe
      .drop("prev_tstamp")
      .drop("diff")
    this
  }

  override def writeToDatabase(df: DataFrame, table: String): Unit = {

    def getEnvVariable(key: String): String = {
      val value = Option(System.getenv(key)).getOrElse {
        throw new IllegalArgumentException(s"$key is NOT set")
      }
      value
    }

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

  override def getDataFrame: DataFrame = { dataFrame }

  // utilities
  private def createUrl(sym: String,
                period1: Long,
                period2: Long,
                interval: String,
                events: String = "history",
                version: String = "v8"): String = {

    val url: String = version.toLowerCase match {
      case "v8" => DataMappings.makeV8Url(sym, period1, period2, interval, events)
      case "v7" => DataMappings.makeV7Url(sym, period1, period2, interval, events)
      case _ => throw new IllegalArgumentException(s"Unsupported API version: $version")
    }
    url
  }

  private def fromV8API(responseBody: String): DataFrame = {

    val json = parse(responseBody)
    val schema: StructType = DataMappings.getYahooAPISchema

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
    val df = spark.createDataFrame(spark.sparkContext.parallelize(flatData.flatten), schema)
      .withColumn("symbol", lit("symbol"))
   df
  }

  private def fromV7API(responseBody: String): DataFrame = {

    val splitIntoLines = responseBody.split('\n')
    val rowElements = splitIntoLines.map(row => row.split(','))
    val rowData = rowElements.tail.map { rows =>
      val date = rows.head // The Date column
      val values = rows.tail.map(BigDecimal(_).setScale(4, RoundingMode.HALF_UP).toDouble)
      Row.fromSeq(date +: values) // date += prices
    }

    val schema = DataMappings.getYahooAPISchema
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

  val roundPrecision = 3
  def roundAt(precision: Int)(n: Double): Double = {
    BigDecimal(n).setScale(precision, RoundingMode.HALF_UP).toDouble
  }

}
