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

case class OffsetWithColumn(columnName: String, range: OffsetRange) {
  override def toString: String =
    s"Offset column = '$columnName', Offset range = '$range'"
}

case class JDBCOffset(offset: OffsetWithColumn) extends Offset {

  override def json(): String =
    JDBCOffset.mapper.writeValueAsString(offset)

  override def toString: String =
    offset.toString

  def fromJson[T](json: String)(implicit m: Manifest[T]): T =
    JDBCOffset.mapper.readValue[T](json)
}

case object JDBCOffset {

  lazy val mapper  = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper
  }

  def toJson(offset: JDBCOffset): String = mapper.writeValueAsString(offset)

  def fromJs(json: String): JDBCOffset =
    mapper.readValue[JDBCOffset](json)
}
