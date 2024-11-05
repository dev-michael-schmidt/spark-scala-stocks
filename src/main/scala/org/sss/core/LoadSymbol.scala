package org.sss.core

import TechnicalImplicits.PipelineActions

object LoadSymbol {

  private val sym = System.getenv("SYMBOL")
  private val table = sym

  def main(args: Array[String]): Unit = {

    val apple = new DataPipeline()
      .loadFromUrl(sym, 1719810000, 1722488400, "1d")
      .interpolate
      .EMA(10)
      .SMA(10)

    apple.writeToDatabase(sym)



    println("we've reached the end")
  }
}