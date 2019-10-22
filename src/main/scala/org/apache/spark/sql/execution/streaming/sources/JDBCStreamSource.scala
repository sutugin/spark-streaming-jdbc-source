package org.apache.spark.sql.execution.streaming.sources

import java.sql.{Date, Timestamp}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.v2.DataSourceV2
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, SQLContext}

import scala.util.{Failure, Success, Try}

class JDBCStreamSource(
  sqlContext: SQLContext,
  providerName: String,
  parameters: Map[String, String],
  metadataPath: String,
  df: DataFrame
) extends Source
    with Logging {

  import sqlContext.implicits._
  import JDBCStreamSourceProvider._

  private val offsetColumn = parameters(OFFSET_COLUMN)

  private val maxOffsetsPerTrigger = None

  private val startingOffset = {
    val s = parameters.get(STARTING_OFFSETS_OPTION_KEY)
    if (s.isEmpty)
      EarliestOffsetRangeLimit
    else {
      val off = s.get
      off match {
        case JDBCOffsetRangeLimit.EARLIEST => EarliestOffsetRangeLimit
        case JDBCOffsetRangeLimit.LATEST   => LatestOffsetRangeLimit
        case v =>
          offsetColumnType match {
            case _: IntegerType | LongType => SpecificOffsetRangeLimit(v.toLong)
            case _: TimestampType          => SpecificOffsetRangeLimit(Timestamp.valueOf(v).getTime)
            case _: DataType               => SpecificOffsetRangeLimit(Date.valueOf(v).getTime)
          }
      }

    }
  }

  private lazy val initialOffsets = {
    val metadataLog = new JDBCStreamingSourceInitialOffsetWriter(sqlContext.sparkSession, metadataPath)
    metadataLog.get(0).getOrElse {
      val offsets = startingOffset match {
        case EarliestOffsetRangeLimit    => LongOffset(getOffsetValue(asc).get)
        case LatestOffsetRangeLimit      => LongOffset(getOffsetValue(desc).get)
        case SpecificOffsetRangeLimit(p) => LongOffset(p)
      }
      metadataLog.add(0, offsets)
      logInfo(s"Initial offsets: $offsets")
      offsets
    }
  }

  private var currentOffsets: Option[Long] = None

  override def schema: StructType = df.schema

  private lazy val offsetColumnType = {
    val t = df.col(offsetColumn).expr.dataType
    t match {
      case _: LongType | IntegerType | DateType | TimestampType => t
      case _ =>
        throw new AnalysisException(
          s"$offsetColumn column type should be ${LongType.simpleString}, ${IntegerType.simpleString}," +
            s"${DateType.catalogString}, or ${TimestampType.catalogString}, but " +
            s"${df.col(offsetColumn).expr.dataType.catalogString} found."
        )
    }
  }

  private def getOffsetValue(sortFunc: String => Column) = {
    val data = df.select(offsetColumn).orderBy(sortFunc(offsetColumn))
    Try {
      offsetColumnType match {
        case _: TimestampType          => data.as[Timestamp].first().getTime
        case _: IntegerType | LongType => data.as[Long].first()
        case _: DataType               => data.as[java.sql.Date].first().getTime
      }
    } match {
      case Success(value) => Some(value)
      case Failure(ex)    => logWarning("Not found offset", ex); None
    }
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    logInfo(s"GetBatch called with start = $start, end = $end")

    initialOffsets

    if (currentOffsets.isEmpty) {
      currentOffsets = Some(end.json().toLong)
    }
    if (start.isDefined && start.get == end) {
      return sqlContext.internalCreateDataFrame(
        sqlContext.sparkContext.emptyRDD[InternalRow].setName("empty"),
        schema,
        isStreaming = true
      )
    }
    val startOffsets = start match {
      case Some(prevBatchEndOffset) =>
        prevBatchEndOffset.json().toLong + 1
      case None =>
        initialOffsets.offset
    }

    val endOffset: Long = end.json().toLong

    val (s, e) = offsetColumnType match {
      case _: IntegerType | LongType => (startOffsets, endOffset)
      case _: TimestampType          => (new Timestamp(startOffsets).toString, new Timestamp(endOffset).toString)
      case _: DataType               => (new Date(startOffsets).toString, new Date(endOffset).toString)
    }

    val strFilter =
      s"$offsetColumn >= CAST('$s' AS ${offsetColumnType.sql}) and $offsetColumn <= CAST('$e' AS ${offsetColumnType.sql})"

    val filteredDf = df.where(strFilter)
    val rdd = filteredDf.queryExecution.toRdd
    val result = sqlContext.internalCreateDataFrame(rdd, schema, isStreaming = true)
    logInfo(s"Offset: ${start.toString} to ${endOffset.toString}")
    result
  }

  override def getOffset: Option[Offset] = {
    initialOffsets

    val latest = getOffsetValue(desc)
    val offsets = maxOffsetsPerTrigger match {
      case None =>
        latest.get
    }

    currentOffsets = Some(offsets)
    logDebug(s"GetOffset: $offsets")
    Some(LongOffset(offsets))
  }

  override def stop(): Unit =
    logWarning("Stop is not implemented!")

}

object JDBCStreamSourceProvider {
  val STARTING_OFFSETS_OPTION_KEY = "startingoffset"
  val OFFSET_COLUMN = "offsetcolumn"
}

class JDBCStreamSourceProvider extends StreamSourceProvider with DataSourceV2 with DataSourceRegister {

  private lazy val df = (sqlContext: SQLContext, parameters: Map[String, String]) => {
    sqlContext.sparkSession.read
      .format("jdbc")
      .options(parameters)
      .load
  }

  override def shortName(): String = "jdbc-streaming"

  override def sourceSchema(
    sqlContext: SQLContext,
    schema: Option[StructType],
    providerName: String,
    parameters: Map[String, String]
  ): (String, StructType) =
    (shortName(), df(sqlContext, parameters).schema)

  override def createSource(
    sqlContext: SQLContext,
    metadataPath: String,
    schema: Option[StructType],
    providerName: String,
    parameters: Map[String, String]
  ): Source = {
    val caseInsensitiveParameters = CaseInsensitiveMap(parameters)
    new JDBCStreamSource(sqlContext, providerName, caseInsensitiveParameters, metadataPath, df(sqlContext, parameters))
  }
}
