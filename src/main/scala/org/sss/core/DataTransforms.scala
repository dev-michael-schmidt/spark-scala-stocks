package org.sss.core

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.json4s.JsonAST.JArray
import org.sss.core.EndpointDataLoader.{fromDatabase, toDatabase}

case class DataTransforms(symbol: String, data: Option[org.apache.spark.sql.DataFrame] = None)

object DataTransforms {

  private val spark = SparkSessionProvider.getSparkSession
  import spark.implicits._
  /* is needed to store (Long, ... Long) instances in a Dataset. Primitive types (Int, String, etc.) and Product types
  (case classes) are supported by importing spark.implicits._  Support for serializing other types will be added in
  future releases. An encoder is use when `.toDF is called to create a data completed DataFrame */

  def fetchData(table: String): DataFrame = fromDatabase(table)

  def fillMissing(df: DataFrame): DataFrame = {

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

  def writeData(df: DataFrame, table: String, mode: String = "overwrite"): Unit = {
    toDatabase(df, table, mode)
  }

  def process(table: String): Unit = {
    val df = fetchData(table)
    val completedDf = fillMissing(df)
    writeData(completedDf, table)
  }
}





/*

  def fillMissing() = {


  // Now, we can split the rows where the diff is greater than 86400

  // Function to create new rows with interpolated values
  private val completedDataframe = dataWithCurrPrev.flatMap(row => {
    val tstamp = row.getAs[Long]("tstamp")
    val prevTstamp = row.getAs[Long]("prev_tstamp")
    val high = row.getAs[Double]("high")
    val low = row.getAs[Double]("low")
    val open = row.getAs[Double]("open")
    val close = row.getAs[Double]("close")
    val volume = row.getAs[Long]("volume")
    val diff = row.getAs[Long]("diff")

    // If the diff is greater than 86400 (1 day), split into two rows
    if (diff > 86400) {
      val interpolatedTstamp = prevTstamp + diff / 2
      val interpolatedHigh = (high + row.getAs[Double]("high")) / 2
      val interpolatedLow = (low + row.getAs[Double]("low")) / 2
      val interpolatedOpen = (open + row.getAs[Double]("open")) / 2
      val interpolatedClose = (close + row.getAs[Double]("close")) / 2

      // Return both the interpolated row (with volume = 0) and the original row
      Seq(
        (interpolatedTstamp, interpolatedHigh, interpolatedLow, interpolatedOpen, interpolatedClose, 0L),  // Volume set to 0
        (tstamp, high, low, open, close, volume) // Original row
      )
    } else {
      // If no gap, return the original row
      Seq((tstamp, high, low, open, close, volume))
    }
  }).toDF("tstamp", "high", "low", "open", "close", "volume")


  toDatabase(completedDataFrame, "AMT_COMPLETE")
  println("we've reached the end")
}
*/