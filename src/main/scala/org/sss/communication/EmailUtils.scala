package org.sss.communication

import scala.util.{Failure, Success, Try}

object EmailUtils {
  // A simple regex to validate an email address.
  private val EmailRegex = "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"

  def isValidEmail(email: String): Boolean =
    Option(email).exists(e => e.trim.nonEmpty && e.matches(EmailRegex))

  def tryUserDomainSplit(email: String, delimiter: String = "@"): Try[(String, String)] =
    if (!isValidEmail(email))
      Failure(new IllegalArgumentException("Not a valid email address"))
    else {
      email.split(delimiter, 2) match {
        case Array(user, domain) => Success((user, domain))
        case _ =>
          Failure(new IllegalArgumentException(s"Invalid email format: $email"))
      }
    }
}