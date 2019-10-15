package org.apache.spark.sql.execution.streaming.sources

import java.sql.{Date, Timestamp}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.v2.DataSourceV2
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext}

import scala.util.{Failure, Success, Try}

class JDBCStreamSource(sqlContext: SQLContext, providerName: String, parameters: Map[String, String])
  extends Source
    with Logging {

  import sqlContext.implicits._

  private lazy val offsetColumnName = "offsetColumn"


  private lazy val df = JDBCStreamSource.df(sqlContext, parameters)

  private lazy val offsetColumn = parameters(offsetColumnName)

  override def schema: StructType = df.schema

  private lazy val offsetColumnType = {
    val t =  df.col(offsetColumn).expr.dataType
    t match {
      case _: NumericType | DateType | TimestampType => t
      case _ =>
        throw new AnalysisException(
          s"$offsetColumn column type should be ${NumericType.simpleString}, " +
            s"${DateType.catalogString}, or ${TimestampType.catalogString}, but " +
            s"${df.col(offsetColumn).expr.dataType.catalogString} found.")
    }
  }

  private def getMaxOffset: Option[Long] = {
    val lastItem = df.select(col(offsetColumn)).orderBy(col(offsetColumn).desc)

    Try {
      offsetColumnType match {
        case _: TimestampType => lastItem.as[Timestamp].first().getTime
        case _: NumericType => lastItem.as[Long].first()
        case _: DataType => lastItem.as[java.sql.Date].first().getTime
      }
    } match {
      case Success(value) => Some(value)
      case Failure(ex)    => logWarning("Not found max value", ex); None
    }
  }

  import org.apache.spark.sql.catalyst.util.DateTimeUtils.{stringToDate, stringToTimestamp}

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val startOffset: Long = start match {
      case Some(offset) => offset.json().toLong
      case None         => 0
    }

    val endOffset: Long = end.json().toLong

    val rdd = offsetColumnType match {
      case _: NumericType =>
        df.filter(col(offsetColumn).gt(startOffset) && col(offsetColumn).leq(endOffset))
          .queryExecution
          .toRdd
      case _: TimestampType =>
        df.filter(col(offsetColumn).gt(new Timestamp(startOffset)) && col(offsetColumn).leq(new Timestamp(endOffset)))
          .queryExecution
          .toRdd

      case _: DataType =>
        df.filter(col(offsetColumn).gt(new Date(startOffset)) && col(offsetColumn).leq(new Date(endOffset)))
          .queryExecution
          .toRdd
    }

    logInfo(s"Offset: ${start.toString} to ${endOffset.toString}")
    val result = sqlContext.internalCreateDataFrame(rdd, schema, true)
    result
  }

  override def getOffset: Option[Offset] = {
    val max = getMaxOffset
    if (max.isDefined) Some(LongOffset(max.get)) else None
  }

  override def stop(): Unit =
    logWarning("Stop is not implemented!")

}

object JDBCStreamSource {

  lazy val df = (sqlContext: SQLContext,parameters: Map[String, String]) => {
    sqlContext.sparkSession.read
      .format("jdbc")
      .options(parameters)
      .load
  }
}

class JDBCStreamSourceProvider extends StreamSourceProvider with DataSourceV2 with DataSourceRegister {

  override def shortName(): String = "jdbc-streaming"

  override def sourceSchema(
                             sqlContext: SQLContext,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]
                           ): (String, StructType) = {

    (shortName(), JDBCStreamSource.df(sqlContext, parameters).schema)
  }

  override def createSource(
                             sqlContext: SQLContext,
                             metadataPath: String,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]
                           ): Source =
    new JDBCStreamSource(sqlContext, providerName, parameters)
}

