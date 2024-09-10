package org.sss.core

//import io.circe._
//import io.circe.generic.auto._
//import io.circe.parser._

import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.json4s._
import org.json4s.jackson.JsonMethods._


object DataMappings extends App {

  val validRanges: List[String] = List("1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max")

  // case class Timestamp(timestamps: List[Long])
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
    "https://query1.finance.yahoo.com/v7/finance/download/" + urlParameters
  }

  def makeV8Url(symbol: String, period1: Long, period2: Long, interval: String, events: String): String = {
    "https://query2.finance.yahoo.com/v8/finance/chart/" + urlParameters(symbol, period1, period2, interval, events)
  }
}













