package org.sss.technicals

import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}

case class TechnicalBuilder(var df: DataFrame) {

  // Simple moving average (default 10 in days)
  def SMA(days: Int = 10): TechnicalBuilder = {
    val windowSpec = Window.orderBy("date").rowsBetween(-days + 1, 0)
    val columnName = s"sma_${days}d"
    df = df.withColumn(columnName, avg(col("close")).over(windowSpec))
    this
  }

  // Exponential moving average (default 10 in days)
  // See: https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average
  def EMA(span: Int = 10): TechnicalBuilder = {
    val columnName = s"ema_${span}d"
    val alpha = 2.0 / (span + 1) // 0 ≤ alpha ≤ 1

    val emaUdf: UserDefinedFunction = udf((closePrices: Seq[Double]) => {
      closePrices.foldLeft(0.0)((prev, curr) => alpha * curr + (1 - alpha) * prev)
    })

    val windowSpec = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df = df.withColumn(columnName, emaUdf(collect_list(col("close")).over(windowSpec)))
    this
  }

  def build(): DataFrame = df
}