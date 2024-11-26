package org.sss.core

import org.apache.spark.sql.DataFrame

trait DataPipelineabstract{

  def loadFromUrl(tickerSymbol: String,
                   period1: Long,
                   period2: Long,
                   interval: String,
                   events: String = "history",
                   apiVersion: String = "v8"): DataPipelineabstract

  def loadFromDatabase(table: String): DataPipelineabstract

  def writeToDatabase(table: String): Unit

  def dropData(table: String): DataPipelineabstract

  def getDataFrame: DataFrame
}
