package org.sss.core

import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.json4s._
import org.json4s.jackson.JsonMethods._


object DataMappings extends App {

  private val validRanges: List[String] = List("1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max")
  val dayInSeconds = 86400
  private val sixtyDaySpan: Long = 5184000
  private val sevenDaySpan: Long = 604800

  case class Timestamp(timestamps: List[Long])
  case class TradingPeriod(timezone: String, end: Long, start: Long, gmtoffset: Int)
  case class CurrentTradingPeriod(pre: TradingPeriod, regular: TradingPeriod, post: TradingPeriod)
  case class Meta(currency: String,
                  symbol: String,
                  exchangeName: String,
                  fullExchangeName: String,
                  instrumentType: String,
                  firstTradeDate: Long,
                  regularMarketTime: Long,
                  hasPrePostMarketData: Boolean,
                  gmtoffset: Int,
                  timezone: String,
                  exchangeTimezoneName: String,
                  regularMarketPrice: Double,
                  fiftyTwoWeekHigh: Double,
                  fiftyTwoWeekLow: Double,
                  regularMarketDayHigh: Double,
                  regularMarketDayLow: Double,
                  regularMarketVolume: Long,
                  longName: String,
                  shortName: String,
                  chartPreviousClose: Double,
                  priceHint: Long,
                  currentTradingPeriod: CurrentTradingPeriod,
                  dataGranularity: String,
                  range: String,
                  validRanges: List[String] = validRanges)

  case class Quote(open: List[Double], close: List[Double], high: List[Double], low: List[Double], volume: List[Long])
  case class Indicators(quote: List[Quote], adjclose: List[AdjustedClose])
  case class AdjustedClose(adjclose: List[Double])
  case class Result(meta: Meta, timestamp: List[Long], indicators: Indicators)

  def getYahooAPISchema: StructType = { StructType(Array(
    StructField("tstamp", LongType, nullable = true),
    StructField("high", DoubleType, nullable = true),
    StructField("low", DoubleType, nullable = true),
    StructField("open", DoubleType, nullable = true),
    StructField("close", DoubleType, nullable = true),
    // StructField("adjclose", DoubleType, nullable = true),
    StructField("volume", LongType, nullable = true)))
  }

  private def urlParameters(sym: String, p1: Long, p2: Long, ivl: String, e: String ): String = {
    s"${sym}?period1=${p1}&period2=${p2}&interval=${ivl}&events=${e}"
  }

  def makeV7Url(symbol: String, period1: Long, period2: Long, interval: String, events: String): String = {
    "https://query1.finance.yahoo.com/v7/finance/download/" + urlParameters(
      sym = symbol,
      p1 = period1,
      p2 = period2,
      ivl = interval,
      e = events)

  }

  def makeV8Url(symbol: String, period1: Long, period2: Long, interval: String, events: String): String = {
    "https://query2.finance.yahoo.com/v8/finance/chart/" + urlParameters(
      sym = symbol,
      p1 = period1,
      p2 = period2,
      ivl = interval,
      e = events)
  }

  def validateTimescales(period1: Long, period2: Long, interval: String): Boolean = {

    // WIP
    if (period1 <= 0 || period2 <= 0) {
      false
    }

    val now: Long = System.currentTimeMillis / 1000
    val span: Long = period2 - period1

    true
  }

    /*
    intra-day
    1m - only 7 days worth
    2m - the requested range must be within the last 60 days
    5m - the requested range must be within the last 60 days
    15m - the requested range must be within the last 60 days
    30m - the requested range must be within the last 60 days
    90m - the requested range must be within the last 60 days
    1h - within the last 730 days

    days
    1d - No known limit
    5d - No known limit
    1wk - No known limit

    extended
    1mo - No known limit
    3mo - No known limit
     */
}













