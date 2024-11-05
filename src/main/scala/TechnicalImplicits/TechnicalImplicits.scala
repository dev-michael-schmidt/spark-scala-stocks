import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{avg, col, collect_list, lag, lit, udf}
import org.sss.core.{DataPipeline, DataPipelineabstract, SparkSessionProvider}

package object TechnicalImplicits {

    implicit class PipelineActions(pl: DataPipeline) {

      def interpolate: DataPipeline = {
        val spark = SparkSessionProvider.getSparkSession
        import spark.implicits._

        val makeInterpolated = pl.getDataFrame

        val windowSpec = Window.orderBy("tstamp")
        val dataWithCurrPrev = makeInterpolated
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
              (interpolatedTstamp, interpolatedHigh, interpolatedLow, interpolatedOpen, interpolatedClose, 0L), // Volume set to 0 for interpolated row
              (tstamp, high, low, open, close, volume) // Original row
            )
          } else {
            // Since there is no gap, return the original row
            Seq((tstamp, high, low, open, close, volume))
          }
        }).toDF("tstamp", "high", "low", "open", "close", "volume")

        val result = contiguousDataframe
          .drop("prev_tstamp")
          .drop("diff")
        new DataPipeline(result)
      }

      def SMA(span: Int = 10): DataPipeline = {
        val windowSpec = Window.orderBy("tstamp").rowsBetween(-span + 1, 0)
        val columnName = s"sma_${span}d"
        val dataFrame = pl.getDataFrame
          .withColumn(columnName, avg(col("close")).over(windowSpec))
        new DataPipeline(dataFrame)
      }

      def EMA(span: Int = 10): DataPipeline = {
        val columnName = s"ema_${span}d"
        val alpha = 2.0 / (span + 1) // 0 ≤ alpha ≤ 1

        val emaUdf: UserDefinedFunction = udf((closePrices: Seq[Double]) => {
          closePrices.foldLeft(0.0)((prev, curr) => alpha * curr + (1 - alpha) * prev)
        })

        val windowSpec = Window.orderBy("tstamp").rowsBetween(Window.unboundedPreceding, Window.currentRow)
        val result = pl.getDataFrame
          .withColumn(columnName, emaUdf(collect_list(col("close")).over(windowSpec)))
        new DataPipeline(result)
      }
    }
}
