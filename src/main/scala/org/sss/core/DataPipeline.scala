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

class DataPipeline(private var dataFrame: DataFrame = null,
                   private var metaData: Option[DataFrame] = null) extends DataPipelineabstract {

  private val logger = Logger.getLogger(getClass.getName)
  private val spark = SparkSessionProvider.getSparkSession

  import spark.implicits._
  implicit val formats: DefaultFormats.type = DefaultFormats   // Required for extracting values (json4s)


  /* Postgres */
  private val user: String = "airflow" //getEnvVariable(System.getenv("POSTGRES_USER"))// .getOrElse("'POSTGRES_USER' not set")
  private val password = "airflow" //getEnvVariable()//Option(System.getenv("POSTGRES_PASSWORD"))//.getOrElse("'POSTGRES_PASSWORD' not set") // don't use env's in prod either

  private val host = Option(System.getenv("POSTGRES_HOST")).getOrElse("localhost")
  private val port: String = System.getenv("POSTGRES_PORT")

  private val format: String = "jdbc"
  private val schema: StructType = DataMappings.getYahooAPISchema

  private val database: String = System.getenv("POSTGRES_DB")
  private val mode: String = System.getenv("DB_SAVE_MODE") // ! currently overwrite

  private val driver = System.getenv("DB_DRIVER")
  private val dbUrl = s"jdbc:postgresql://${host}:${port}/$database"

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
      logger.error(s"YahooAPI returned a non-200 code, it was ${response.statusCode()} instead")
      throw new RuntimeException(s"Unable to capture stock data")
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

  override def writeToDatabase(table: String): Unit = {

    val df = getOrCreateEmpty()
    df.write
      .format("jdbc")
      .option("url", dbUrl)
      .option("dbtable", table)
      .option("user", user)
      .option("password", password)
      .option("driver", driver)
      .mode(mode)
      .save()
  }

  override def getDataFrame: DataFrame = {
   getOrCreateEmpty()
  }

  override def dropData(table: String): DataPipeline = {
    logger.warn(s"This pipeline's dataframe will empty")
    val emptyRDD = spark.sparkContext.emptyRDD[Row]
    dataFrame = spark.createDataFrame(emptyRDD, schema)
    this
  }

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

  private def getOrCreateEmpty(): DataFrame = {
    val df = Option(dataFrame)
    dataFrame = df.getOrElse {

      logger.warn("dataFrame does not exist! creating an empty one with a schema")

      val emptyRDD = spark.sparkContext.emptyRDD[Row]
      val emptyDF = spark.createDataFrame(emptyRDD, schema)

      emptyDF
    }
    dataFrame
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
