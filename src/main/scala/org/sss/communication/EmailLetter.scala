package org.sss.communication

import scala.util.matching.Regex

object EmailLetter {

  val emailUserName = Option(System.getenv("EMAIL_USERNAME")).getOrElse {
    println("ERROR System ENV-VAR `EMAIL_USERNAME` was not set") // Logging
    throw new Exception("Unable to extract email user's password")
  }
   private val emailUserPassword = Option(System.getenv("EMAIL_USER_PASSWORD")).getOrElse {
    println("ERROR set ENV-VAR `EMAIL_USER_PASSWORD` was not set.") // Logging
    throw new Exception("Unable to extract email user's password")
  }
  val emailServerHostname: String = Option(System.getenv("EMAIL_HOST")).getOrElse {
    println("WARN email host is not set, defaulting to `localhost`")
    "localhost"
  }

  val emailServerPort: Int = Option(System.getenv("EMAIL_PORT")).map(_.toInt).getOrElse {
    println("WARN email port is not set, defaulting to 1025")
    1025
  }

  def toAddress(emailAddress: String): String = {
    // The address check is just a basic check instead of a (very) long regex string.
    if (!emailAddress.contains("@")) throw new IllegalArgumentException("`toAddress` is not a valid email address")
    else emailAddress
  }

  def content: String = "(no content)"

}
