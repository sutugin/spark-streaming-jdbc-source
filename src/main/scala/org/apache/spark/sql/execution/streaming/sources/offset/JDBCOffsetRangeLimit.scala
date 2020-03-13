package org.apache.spark.sql.execution.streaming.sources.offset

sealed trait JDBCOffsetRangeLimit

case object EarliestOffsetRangeLimit extends JDBCOffsetRangeLimit

case object LatestOffsetRangeLimit extends JDBCOffsetRangeLimit

case class SpecificOffsetRangeLimit[T](offset: T) extends JDBCOffsetRangeLimit {
  override def toString: String = offset.toString
}

object JDBCOffsetRangeLimit {
  val LATEST = "latest" // indicates resolution to the latest offset
  val EARLIEST = "earliest" // indicates resolution to the earliest offset
}
