package org.sss.core

import org.sss.core.EndpointDataLoader.{fromAPI, toDatabase}

object LoadSymbol {

  private val table = System.getenv("PRICES")

  def main(args: Array[String]): Unit = {

    val symbolDataFrame = fromAPI()
    toDatabase(symbolDataFrame, table)

    println("we've reached the end")
  }
}
