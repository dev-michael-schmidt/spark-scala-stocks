package org.sss.core


object LoadSymbol {

  private val table = System.getenv("SYMBOL")

  def main(args: Array[String]): Unit = {

    val symbolDataFrame = fromV8API()
    toDatabase(symbolDataFrame, table)

    println("we've reached the end")
  }
}