package org.apache.spark.sql.execution.streaming.sources.offset

sealed trait JDBCOffsetFilterType

case object InclusiveJDBCOffsetFilterType extends JDBCOffsetFilterType
case object ExclusiveJDBCOffsetFilterType extends JDBCOffsetFilterType

object JDBCOffsetFilterType {
  def getStartOperator(offsetFilterType: JDBCOffsetFilterType): String =
    offsetFilterType match {
      case InclusiveJDBCOffsetFilterType => ">="
      case ExclusiveJDBCOffsetFilterType => ">"
    }

  def getEndOperator(offsetFilterType: JDBCOffsetFilterType): String =
    offsetFilterType match {
      case InclusiveJDBCOffsetFilterType => "<="
      case ExclusiveJDBCOffsetFilterType => "<"
    }
}
