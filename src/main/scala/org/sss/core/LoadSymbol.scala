package org.sss.core


object LoadSymbol {

  private val sym = System.getenv("SYMBOL")
  private val table = sym // System.getenv("TABLE")

  def main(args: Array[String]): Unit = {

    val loader = EndpointDataLoader(sym, 1722488400, 1725166800, "1d")
    val df = loader.getSymbol

    loader.loadSymbol(df, sym)

    println("we've reached the end")
  }
}