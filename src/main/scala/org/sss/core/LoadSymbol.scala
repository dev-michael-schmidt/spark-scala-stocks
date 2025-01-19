package org.sss.core

import org.sss.technicals.PipelineActions

object LoadSymbol {

  private val sym = System.getenv("SYMBOL")
  private val table = sym

  def main(args: Array[String]): Unit = {

    val spy = new DataPipeline()
      .loadFromUrl(sym, 1727833579, 1730511979, "1d")
      .interpolate
      .EMA(10)
      .SMA(10)

    spy.writeToDatabase(table)
    println("we've reached the end")
  }
}