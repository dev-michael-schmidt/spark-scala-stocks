package org.sss.core

import org.sss.core.EndpointDataLoader.{fromV8API, toDatabase}

object LoadSymbol {

  private val table = System.getenv("PRICES")

  def main(args: Array[String]): Unit = {

    val symbolDataFrame = fromV8API()
    toDatabase(symbolDataFrame, table)

  }
}
