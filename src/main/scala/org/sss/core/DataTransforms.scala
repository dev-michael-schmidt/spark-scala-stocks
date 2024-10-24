package org.sss.core

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.sss.core.DataTransforms._interpolate


class DataTransforms(val tickerSymbol: String,
                     val period1: Long,
                     val period2: Long,
                     val interval: String,
                     val events: String = "history"
                     ) extends StockMeta {

  /*
  // Create an instance of DataTransforms
  val transformer = new DataTransforms("AAPL", 1609459200L, 1640995200L, "1d")

  // Interpolate by fetching data from a table and return a new instance
  val dataFrameWithInterpolation = transformer.interpolate(table) // String

  // Interpolate using a DataFrame directly and return a new instance
  val dataFrameWithInterpolation = transform.interpolate(dataFrame) // DataFrame

  // Access the updated data
  val fromTableDataFrame = newTransformWithTableData.data
  val fromDataFrame = newTransformWithDataFrame.data
  */

  // Method to copy the instance with new data
  private def copy(tickerSymbol: String = this.tickerSymbol,
                   period1: Long = this.period1,
                   period2: Long = this.period2,
                   interval: String = this.interval,
                   events: String = this.events,
                   data: Option[DataFrame] = this.data): DataTransforms = {
    new DataTransforms(tickerSymbol, period1, period2, interval, events, data)
  }

  // def interpolate(table: String, p1: String, p2: String) = { }
  // Advanced feature... interpolate within a time range!



  // Process the data using the companion object's methods
  def process(): Unit = {
    // If data is already provided, use it; otherwise, fetch it
    val df = data.getOrElse(DataTransforms.fetchData(sym)) // Fetch if not present
    val completedDf = DataTransforms.fillMissing(df) // Apply transformation
    DataTransforms.writeData(completedDf, sym) // Write the transformed data back
  }




  def withData(df: DataFrame): DataTransforms = {
    this.copy(data = Some(df)) // Return a new instance (of self) with the DataFrame attached
  }

//   TODO: Advance usage interpolate from table by periods 1 & 2
//  def interpolate(table: String, period1: Long, period2: Long): DataFrame = {
//    val df = fetchDataFromDatabase(table)
//    val filledDataFrame = fillMissing(df)
//    filledDataFrame
//  }
    // TODO: Advance usage interpolate from DataFrame by period 1 & 2
//  def interpolate(data: DataFrame): DataFrame = {
//    val filledDataFrame = fillMissing(data)
//    filledDataFrame
}

object DataTransforms {

  private val spark = SparkSessionProvider.getSparkSession
  import spark.implicits._
  /* is needed to store (Long, ... Long) instances in a Dataset. Primitive types (Int, String, etc.) and Product types
  (case classes) are supported by importing spark.implicits._  Support for serializing other types will be added in
  future releases. An encoder is use when `.toDF is called to create a data completed DataFrame */

  def _interpolate(df: DataFrame): DataFrame = {

    val windowSpec = Window.orderBy("tstamp")
    val dataWithCurrPrev = df
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
          (interpolatedTstamp, interpolatedHigh, interpolatedLow, interpolatedOpen, interpolatedClose, 0L),  // Volume set to 0 for interpolated row
          (tstamp, high, low, open, close, volume) // Original row
        )
      } else {
        // Since there is no gap, return the original row
        Seq((tstamp, high, low, open, close, volume))
      }
    }).toDF("tstamp", "high", "low", "open", "close", "volume")

    val completedDataFrame = contiguousDataframe
      .drop("prev_tstamp")
      .drop("diff")

    completedDataFrame
  }
}