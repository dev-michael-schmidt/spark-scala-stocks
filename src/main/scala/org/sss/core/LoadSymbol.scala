package org.sss.core


object LoadSymbol {

  private val sym = System.getenv("SYMBOL")
  private val table = sym // System.getenv("TABLE")

  def main(args: Array[String]): Unit = {

    val loader = new DataTransforms("aapl", 1, 2, "1d")
    val df = loader.fetchData()


    println("we've reached the end")
  }
}