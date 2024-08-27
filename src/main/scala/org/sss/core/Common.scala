package org.sss.core

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object Common {
      // val schema = spark.read.json("src/main/resources/schemas/y_prices.json").schema
    val yahooAPISchema: StructType = StructType(Array(
      StructField("Date", StringType, nullable = true),
      StructField("Open", DoubleType, nullable = true),
      StructField("High", DoubleType, nullable = true),
      StructField("Low", DoubleType, nullable = true),
      StructField("Close", DoubleType, nullable = true),
      StructField("Adj Close", DoubleType, nullable = true),
      StructField("Volume", DoubleType, nullable = true)
    ))
}
