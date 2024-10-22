package org.sss.core

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.sss.core.EndpointDataLoader.{fromDatabase, toDatabase}

object MissingDataInterolator extends App{

  val spark = SparkSessionProvider.getSparkSession
  import spark.implicits._

  val missingData = fromDatabase("AMT") //System.getenv("SYMBOL"))
  val windowSpec = Window.orderBy("tstamp")

  val diffedMissingData = missingData.select(col("tstamp"),
      col("high"),
      col("low"),
      col("open"),
      col("close"),
      col("volume"))
    .withColumn("prev_tstamp", lag("tstamp", 1).over(windowSpec))
    .withColumn("diff",
      col("tstamp") - when(lag("tstamp", 1).over(windowSpec).isNull, 0)
        .otherwise(lag("tstamp", 1).over(windowSpec)))

  // Function to create new rows with interpolated values
  val splitRows = diffedMissingData.flatMap(row => {
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

  val result = splitRows
    .drop("prev_tstamp")
    .drop("diff")

  toDatabase(result, "AMT_COMPLETE")
  println("we've reached the end")
}
