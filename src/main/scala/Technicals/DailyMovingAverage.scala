package Technicals

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

class DailyMovingAverage(val stockDataFrame: DataFrame, val days: Int = 10) {
  private val windowSpec = Window.orderBy("date").rowsBetween(-days + 1, 0)
  private val columnName = s"$days" + "d_moving_avg"
  private val modifiedDataFrame = stockDataFrame.withColumn(columnName, avg(col("close")).over(windowSpec))

  def add(): DataFrame = modifiedDataFrame
  def getColumn: Column = modifiedDataFrame.col(columnName)
}
