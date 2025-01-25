package org.sss.utilities

import org.apache.kafka.common.serialization.Deserializer
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods._

class JsonMapDeserializer extends Deserializer[Map[String, String]] {
  implicit val formats: DefaultFormats.type = DefaultFormats

  override def deserialize(s: String, bytes: Array[Byte]): Map[String, String] = {
    if (bytes == null) null
    else parse(new String(bytes, "UTF-8")).extract[Map[String, String]]
  }
}
