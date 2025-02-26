package org.sss.core

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, lit, unix_timestamp}
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.json4s.JsonAST._
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
  implicit val formats: DefaultFormats.type = DefaultFormats  // Required for extracting values (json4s)

  private val dbConfig = Map(
    "appName" -> System.getenv("APP_NAME"),
    "dbUser" -> Option(System.getenv("POSTGRES_USER")).getOrElse("airflow"),
    "password" ->Option(System.getenv("POSTGRES_PASSWORD")).getOrElse("airflow"),
    "host" -> Option(System.getenv("POSTGRES_HOST")).getOrElse("airflow"),
    "port" -> Option(System.getenv("POSTGRES_PORT")).map(_.toInt).getOrElse(5432),
    "postgresDb" -> Option(System.getenv("POSTGRES_DB")).getOrElse("airflow"),
    "connectTimeout" -> Option(System.getenv("CONNECT_TIMEOUT")).map(_.toInt).getOrElse(30),
    "charSet" -> Option(System.getenv("CHAR_SET")).getOrElse("UTF-8"),
    "currentSchema" -> Option(System.getenv("CURRENT_SCHEMA")).getOrElse("public")
  )

  private val dBmode = Option(System.getenv("MODE")).getOrElse("overwrite")
  private val dBformat = Option(System.getenv("DATABASE_FORMAT")).getOrElse("jdbc")
  private val dBDriver = Option(System.getenv("DATABASE_DRIVER")).getOrElse("org.postgresql.Driver")

  private val yahooSchema: StructType = DataMappings.getYahooAPISchema
  private val dbUrl = s"jdbc:postgresql://${dbConfig("Host")}:${dbConfig("port")}/${dbConfig("postgresDb")}?" +
    s"user=${dbConfig("user")}&" +
    s"password=${dbConfig("password")}&" +
    s"connectTimeout=${dbConfig("connectTimeout")}&" +
    s"currentSchema=${dbConfig("currentSchema")}&" +
    s"charSet=${dbConfig("charSet")}&" +
    s"applicationName=${dbConfig("appName")}"

  override def loadFromDatabase(table: String): DataPipelineabstract = {
    dataFrame = spark.read
      .format(dBformat)
      .option("driver", dBDriver)
      .option("url", dbUrl)
      .option("user", user)
      .option("password", password) // TODO: secret's manager??
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
    val queryURL = createUrl(tickerSymbol, period1, period2, interval)
    val client = HttpClient.newHttpClient()
    val request = HttpRequest.newBuilder()
      .uri(URI.create(queryURL))
      .GET() // request type
      .build()

    // Send the request and get the response as a String
    // val responseBody: String = client.send(request, BodyHandlers.ofString).body()
    val response = client.send(request, BodyHandlers.ofString())
    val responseBody = response.statusCode() match {
      case 200 => response.body()
      case _ => throw new RuntimeException(s"YahooAPI returned a non-200 code, it was ${response.statusCode()}")
    }

    dataFrame = apiVersion.toLowerCase match {
      case "v8" => fromV8API(responseBody)
      case "v7" => fromV7API(responseBody)
      case _ => throw new RuntimeException("Unsupport api version or version not implemented")
    }
    this
  }

  override def writeToDatabase(table: String): Unit = {


    val df = getOrCreateEmpty()

    df.write
      .format(dBformat)
      .option("url", dbUrl)
      .option("dbtable", table)
      .option("user", user)
      .option("password", password)
      .option("driver", dBDriver)
      .mode(dBmode)
      .save()
  }

  override def getDataFrame: DataFrame = {
   getOrCreateEmpty()
  }

  override def dropData(table: String): DataPipeline = {
    logger.warn(s"This pipeline's dataframe contents will be emptied")
    val emptyRDD = spark.sparkContext.emptyRDD[Row]
    dataFrame = spark.createDataFrame(emptyRDD, yahooSchema)
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
      val emptyDF = spark.createDataFrame(emptyRDD, yahooSchema)

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
        // Handle other cases, but none needed at this time.
        Map.empty[String, Any]
      }.toMap
    }

  private val roundPrecision = 3
  def roundAt(precision: Int)(n: Double): Double = {
    BigDecimal(n).setScale(precision, RoundingMode.HALF_UP).toDouble
  }
}
