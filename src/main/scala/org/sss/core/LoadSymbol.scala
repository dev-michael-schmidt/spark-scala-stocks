package org.sss.core

import org.sss.core.EndpointDataLoader._

object LoadSymbol {

  private val table = System.getenv("SYMBOL")

  def main(args: Array[String]): Unit = {

    val symbolDataFrame = fromV8API()
    toDatabase(symbolDataFrame, table)

    println("we've reached the end")
  }
}