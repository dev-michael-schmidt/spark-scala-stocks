package org.sss.utilities

import org.apache.kafka.common.serialization.Serializer
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._

class JsonMapSerializer extends Serializer[Map[String, String]] {

  implicit val formats: DefaultFormats.type = DefaultFormats

  def mapToJsonAst(map: Map[String, String]): JValue = { map }
  def jsonAstToString(jsonAst: JValue): String = { compact(render(jsonAst)) }
  def jsonStringToBytes(jsonString: String): Array[Byte] = { jsonString.getBytes("UTF-8") }

  override def serialize(s: String, t: Map[String, String]): Array[Byte] = {
    if (t == null) null
    else {
      val jsonAst = mapToJsonAst(t)
      val jsonString = jsonAstToString(jsonAst)
      jsonStringToBytes(jsonString)
    }
  }
}
