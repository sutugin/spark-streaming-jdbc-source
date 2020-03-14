package org.apache.spark.sql.execution.streaming.sources.offset

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.sources.v2.reader.streaming.Offset

case class BatchOffsetRange(start: String, end: String, startInclusion: JDBCOffsetFilterType)

case class OffsetRange(start: Option[String], end: Option[String]) {

  override def toString: String =
    s"start = '$start', end = '$end'"
}

case class JDBCOffset(columnName: String, range: OffsetRange) extends Offset {
  override def toString: String =
    s"Offset column = '$columnName', Offset range = '$range'"
  override def json(): String =
    JDBCOffset.toJson(this).toString
}

case object JDBCOffset {
  private val jsonMapper = {
    val m = new ObjectMapper() with ScalaObjectMapper
    m.registerModule(DefaultScalaModule)
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
    m
  }

  def toJson(value: Any): String =
    jsonMapper.writeValueAsString(value)

  private def fromJson[T](json: String)(implicit m: Manifest[T]): T =
    jsonMapper.readValue[T](json)

  def fromJson(json: String): JDBCOffset = {
    println(json)
    val foo = fromJson[JDBCOffset](json)
    foo
  }
}
