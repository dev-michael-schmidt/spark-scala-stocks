package org.sss.communication

import courier.Defaults.executionContext
import courier._
import Defaults._

import scala.util.{Try, Success, Failure}


object PostOffice {

  private val emailUserName: String = Option(System.getenv("EMAIL_USERNAME")).getOrElse {
    println("ERROR System ENV-VAR EMAIL_USERNAME was not set") // Logging
    throw new Exception("Unable to extract email user's password")
  }

  private val emailUserPassword: String = Option(System.getenv("EMAIL_USER_PASSWORD")).getOrElse {
    println("ERROR set ENV-VAR EMAIL_USER_PASSWORD was not set.") // Logging
    throw new Exception("Unable to extract email user's password")
  }

  private val emailUserAddress = Option(System.getenv("EMAIL_USER_EMAIL_ADDRESS")).getOrElse{
    // we could use email address validation here, but maybe it should happened upstream
    throw new IllegalStateException("No user from address specified.")
  }

  private val emailServerHost: String = Option(System.getenv("EMAIL_HOST")).getOrElse {
    println("WARN email host is not set, defaulting to localhost")
    "localhost"
  }

  private val emailServerPort: Int = Option(System.getenv("EMAIL_PORT")).map(_.toInt).getOrElse {
    println("WARN email port is not set, defaulting to 587")
    587
  }

  private def tryUserDomainSplit(email: String, delimiter: String = "@"): Try[(String, String)] =
    email.split(delimiter, 2) match {
      case Array(user, domain) => Success((user, domain))
      case _                   => Failure(new IllegalArgumentException(s"Invalid email format: $email"))
    }

  private def getEnvVariable(key: String): String =
    Option(System.getenv(key)).getOrElse {
      throw new IllegalStateException(s"$key not set in environment")
    }

  private def getCredentials(): Map[String, String] = Map(
    "username" -> getEnvVariable("EMAIL_USERNAME"),
    "password" -> getEnvVariable("EMAIL_USER_PASSWORD")
  )

  def sendEmail(
                 toEmailAddress: String,
                 subject: String = "(no subject)",
                 content: String = "(no content)",
               ): Unit = {
    val credentials = getCredentials()
    val parsedToAddress = tryUserDomainSplit(toEmailAddress) match {
      case Success((user, domain)) => Map("recipient" -> user, "domain" -> domain)
      case Failure(exception) =>
        throw new IllegalArgumentException(s"Failed to parse email address: ${exception.getMessage}")
    }

    val emailUserName = credentials("username")

    val mailer = Mailer(emailServerHost, emailServerPort)
      .auth(true)
      .as(credentials("username"), credentials("password"))
      .startTls(true)()

    val email = Envelope.from(emailUserName.addr)
      .to(s"${parsedToAddress("recipient")}@${parsedToAddress("domain")}".addr)
      .subject(subject)
      .content(Text(content))

    mailer(email).onComplete {
      case Success(_) => println("Email sent successfully!")
      case Failure(exception) => println(s"Failed to send email: ${exception.getMessage}")
    }
  }
}
