package org.apache.spark.sql.execution.streaming.sources.offset

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.execution.streaming.Offset

case class BatchOffsetRange(start: String, end: String, startInclusion: JDBCOffsetFilterType)

case class OffsetRange(start: Option[String], end: Option[String]) {

  override def toString: String =
    s"start = '$start', end = '$end'"
}

case class JDBCOffset(columnName: String, range: OffsetRange) extends Offset {
  override def toString: String =
    s"Offset column = '$columnName', Offset range = '$range'"
  override def json(): String =
    JDBCOffset.toJson(JDBCOffset(columnName, range))
}

case object JDBCOffset {
  private val jsonMapper = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
    mapper
  }

  def toJson(value: JDBCOffset): String =
    jsonMapper.writeValueAsString(value)

  private def fromJson[T](json: String)(implicit m: Manifest[T]): T =
    jsonMapper.readValue[T](json)

  def fromJson(json: String): JDBCOffset = fromJson[JDBCOffset](json)
}
