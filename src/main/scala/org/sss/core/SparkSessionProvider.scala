package org.sss.core

import org.apache.spark.sql.SparkSession

object SparkSessionProvider {

  private val configs = Map(
    "spark.master" -> "local",
    "spark.executor.memory" -> "6g",
    "spark.executor.cores" -> "4"
  )

  private val sparkBuilder = SparkSession.builder()
    .config("spark.master", "local")
    .appName(System.getenv("APP_NAME"))

   configs.foreach { case (key, value) => sparkBuilder.config(key, value) }

  def getSparkSession: SparkSession = sparkBuilder.getOrCreate()
}
