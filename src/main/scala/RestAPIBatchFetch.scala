import java.net.URI
import java.net.http.HttpResponse.BodyHandlers
import java.net.http.{HttpClient, HttpRequest}
import java.time.Instant.now
import java.util.concurrent.atomic.AtomicBoolean
import scala.annotation.tailrec
import scala.collection.mutable

case class RestAPIBatchFetch(var startAt: Long, val tickerSymbol: String) {

  private val running = new AtomicBoolean(true)
  private val results = mutable.ListBuffer.empty[String]
  private val client = HttpClient.newHttpClient()
  private var endAt: Long = now.getEpochSecond

  @tailrec
  private def run(url: String,
          symbol: String,
          interval: String = "1m",
          events: String = "history"): Unit = {

    if (running.get()) {
      val request = HttpRequest.newBuilder()
        .uri(URI.create(url +
          s"$symbol?" +
          s"period1=$startAt&" +
          s"period2=$endAt&" +
          s"interval=$interval&" +
          s"events=$events")
        )
        .GET() // request type
        .build()

      val result = client.send(request, BodyHandlers.ofString)
      results.synchronized {
        results += result.body() // this needs to parse out a row / the tail to ignore the header
      }

      Thread.sleep(90000) // we wait 1.5 minutes between queries, when a duplicate is encountered we drop it
      startAt = endAt
      endAt = startAt
      run(url, symbol, interval, events)
    }
  }

  run(Constants.YAHOO_FINANCE_ENDPOINT, tickerSymbol, "1m", "history")

  def halt(): Unit = {
    running.set(false)
  }

}
