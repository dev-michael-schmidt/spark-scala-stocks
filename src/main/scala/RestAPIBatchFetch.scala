import java.net.URI
import java.net.http.HttpResponse.BodyHandlers
import java.net.http.{HttpClient, HttpRequest}
import java.time.Instant
import java.time.Instant.now
import java.util.concurrent.atomic.AtomicBoolean
import scala.annotation.tailrec
import scala.collection.mutable

case class RestAPIBatchFetch(val startAt: Long, val tickerSymbol: String) {

  private val spark = SparkSessionProvider.getSparkSession
  private val client = HttpClient.newHttpClient()

  private val running = new AtomicBoolean(true)
  private val results = mutable.ListBuffer.empty[String]

  private val interval = "1m"
  private val events = "history"

  private val backOff = 300 // seconds
  private var period1: Instant = Instant.ofEpochSecond(now.minusSeconds(backOff).getEpochSecond)
  private var period2: Instant = Instant.ofEpochSecond(startAt)
  require(period1.isBefore(period2.minusSeconds(backOff)))

  @tailrec
  private def run(): Unit = {

    if (running.get()) {
      val request = HttpRequest.newBuilder()
        .uri(URI.create(Constants.YAHOO_FINANCE_ENDPOINT +
          s"$tickerSymbol?" +
          s"period1=${period1.getEpochSecond}&" +
          s"period2=${period2.getEpochSecond}&" +
          s"interval=$interval&" +
          s"events=$events")
        )
        .GET() // request type
        .build()

      val result = client.send(request, BodyHandlers.ofString)
      results.synchronized {
        results += result.body() // this needs to parse out a row / the tail to ignore the header
      }

      period1 = period2
      period2 = now().minusSeconds(backOff)
      run()
    }
  }
  // val colNames = getColumnNames()
  run()

  def halt(): Unit = {
    running.set(false)
  }

  def getColumnNames(): Unit = {
    val request = HttpRequest.newBuilder()
      .uri(URI.create(Constants.YAHOO_FINANCE_ENDPOINT +
        s"$tickerSymbol?" +
        s"period1=${period1.getEpochSecond}&" +
        s"period2=${period2.getEpochSecond}&" +
        s"interval=$interval&" +
        s"events=$events")
      )
      .GET() // request type
      .build()
  }

}
