package org.sss.communication

import courier.Defaults.executionContext
import courier._
import org.sss.communication.Messenger.{emailHost, emailPort, emailUserName, emailUserPassword}

object PostOffice {

  def sendEmail(to: String = EmailLetter.toAddress(""), content: String = EmailLetter.content): Unit = {

    val mailer = Mailer(EmailLetter.emailServerHostname, EmailLetter.emailServerPort)
      .auth(true)
      .as(EmailLetter.emailUserName, EmailLetter.emailUserPassword)
      .startTls(true)()

    val email = Envelope.from(EmailLetter.emailUserAddress.addr)
      .to("recipient@example.com".addr)
      .subject("Test Mail")
      .content(Text(content))

    mailer(email)
  }
}
