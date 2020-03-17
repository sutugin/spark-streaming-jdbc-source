package org.apache.spark.sql.execution.streaming.sources.offset

sealed trait JDBCOffsetRangeLimit

case object EarliestOffsetRangeLimit extends JDBCOffsetRangeLimit {
  override val toString: String = "earliest"
}

case object LatestOffsetRangeLimit extends JDBCOffsetRangeLimit {
  override val toString: String = "latest"
}

case class SpecificOffsetRangeLimit[T](offset: T) extends JDBCOffsetRangeLimit {
  override def toString: String = offset.toString
}
