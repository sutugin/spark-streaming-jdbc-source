package org.apache.spark.sql.execution.streaming.sources

sealed trait JDBCOffsetRangeLimit

case object EarliestOffsetRangeLimit extends JDBCOffsetRangeLimit

case object LatestOffsetRangeLimit extends JDBCOffsetRangeLimit

case class SpecificOffsetRangeLimit(longOffset: Long) extends JDBCOffsetRangeLimit

object JDBCOffsetRangeLimit {
  val LATEST = "latest" // indicates resolution to the latest offset
  val EARLIEST = "earliest" // indicates resolution to the earliest offset
}
