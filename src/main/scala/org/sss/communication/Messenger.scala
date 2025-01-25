package org.sss.communication

import courier._
import scala.concurrent.ExecutionContext.Implicits.global


class Messenger(val to: String, val content: String) {




}


object Messenger {

//  val password: Option[String] = emailUserPassword.toOption
//  password match {
//    case Some(value) => function_foo(value)
//    case None => throw new Exception("Password was not found")
//  }

  private val emailUserName = Option(System.getenv("EMAIL_USERNAME")).getOrElse {
    println("ERROR System ENV-VAR `EMAIL_USERNAME` was not set") // Logging
    throw new Exception("Unable to extract email user's password")
  }
  private val emailUserPassword = Option(System.getenv("EMAIL_USER_PASSWORD")).getOrElse {
    println("ERROR set ENV-VAR `EMAIL_USER_PASSWORD` was not set.") // Logging
    throw new Exception("Unable to extract email user's password")
  }
  private val emailHost: String = Option(System.getenv("EMAIL_HOST")).getOrElse {
    println("WARN email host is not set, defaulting to `localhost`")
    "localhost"
  }

  private val emailPort: Int = Option(System.getenv("EMAIL_PORT").toInt).getOrElse{
    println("WARN email port is not set, defaulting to `1025`")
    1025
  }


  println("INFO mailer is using STARTTLS")

  private val mailer = Mailer(emailHost, emailPort)
    .auth(true)
    .as(emailUserName, emailUserPassword)
    .startTls(true)()

  private val email = Envelope.from(emailUserName.addr)
    .to("recipient@example.com".addr)
    .subject("Test Mail")
    .content(Text("This is a test email!"))

  mailer(email)

}
