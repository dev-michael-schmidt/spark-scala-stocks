package org.sss.core


object LoadSymbol {

  private val sym = "aapl" // System.getenv("SYMBOL")
  private val table = sym // System.getenv("TABLE")

  def main(args: Array[String]): Unit = {

    val apple = new DataPipeline()
      .loadFromUrl(sym, 1719810000, 1722488400, "1d")
      .getDataFrame

    val appleWithTechnical = new TechnicalBuilder(apple)
      .SMA(10)
      .EMA(26)
      .getDataFrame

    appleWithTechnical.show(10)

    println("we've reached the end")
  }
}