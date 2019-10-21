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

class JDBCStreamSource(sqlContext: SQLContext,
                       providerName: String,
                       parameters: Map[String, String],
                       metadataPath: String)
  extends Source
    with Logging {

  import sqlContext.implicits._
  import JDBCStreamSourceProvider._


  private val df = JDBCStreamSourceProvider.df(sqlContext, parameters)

  private val offsetColumn = parameters(OFFSET_COLUMN)

  private val maxOffsetsPerTrigger = None//parameters.get(MAX_OFFSET_PER_TRIGGER).map(_.toLong)

  private val startingOffsets = {
    val s = parameters.get(STARTING_OFFSETS_OPTION_KEY)
    if (s.isEmpty)
      EarliestOffsetRangeLimit
    else {
      val off = s.get
      off match {
        case JDBCOffsetRangeLimit.EARLIEST => EarliestOffsetRangeLimit
        case JDBCOffsetRangeLimit.LATEST => LatestOffsetRangeLimit
        case v =>
          offsetColumnType match {
            case _: IntegerType | LongType => SpecificOffsetRangeLimit(v.toLong)
            case _: TimestampType => SpecificOffsetRangeLimit(Timestamp.valueOf(v).getTime)
            case _: DataType => SpecificOffsetRangeLimit(Date.valueOf(v).getTime)
          }
      }

    }
  }


  lazy val initialOffsets = {
    val metadataLog = new JDBCStreamingSourceInitialOffsetWriter(sqlContext.sparkSession, metadataPath)
    metadataLog.get(0).getOrElse {
      val offsets = startingOffsets match {
        case EarliestOffsetRangeLimit => LongOffset(getOffsetValue(col(offsetColumn).asc).get)
        case LatestOffsetRangeLimit => LongOffset(getOffsetValue(col(offsetColumn).desc).get)
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

  private def getOffsetValue(col: Column) = {
    val data = df.select(offsetColumn).orderBy(col)
    Try {
      offsetColumnType match {
        case _: TimestampType => data.as[Timestamp].first().getTime
        case _: IntegerType | LongType => data.as[Long].first()
        case _: DataType => data.as[java.sql.Date].first().getTime
      }
    } match {
      case Success(value) => Some(value)
      case Failure(ex)    => logWarning("Not found offset", ex); None
    }
  }

//  private def rateLimit(
//                         limit: Long,
//                         from: Long,
//                         until: Long): Long = {
//    val fromNew = getOffsetValue(col(offsetColumn).asc)
//    val sizes = until.flatMap {
//      case (tp, end) =>
//        // If begin isn't defined, something's wrong, but let alert logic in getBatch handle it
//        from.get(tp).orElse(fromNew.get(tp)).flatMap { begin =>
//          val size = end - begin
//          logDebug(s"rateLimit $tp size is $size")
//          if (size > 0) Some(tp -> size) else None
//        }
//    }
//    val total = sizes.values.sum.toDouble
//    if (total < 1) {
//      until
//    } else {
//      until.map {
//        case (tp, end) =>
//          tp -> sizes.get(tp).map { size =>
//            val begin = from.getOrElse(tp, fromNew(tp))
//            val prorate = limit * (size / total)
//            logDebug(s"rateLimit $tp prorated amount is $prorate")
//            // Don't completely starve small topicpartitions
//            val prorateLong = (if (prorate < 1) Math.ceil(prorate) else Math.floor(prorate)).toLong
//            // need to be careful of integer overflow
//            // therefore added canary checks where to see if off variable could be overflowed
//            // refer to [https://issues.apache.org/jira/browse/SPARK-26718]
//            val off = if (prorateLong > Long.MaxValue - begin) {
//              Long.MaxValue
//            } else {
//              begin + prorateLong
//            }
//            logDebug(s"rateLimit $tp new offset is $off")
//            // Paranoia, make sure not to return an offset that's past end
//            Math.min(end, off)
//          }.getOrElse(end)
//      }
//    }
//  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {

    initialOffsets

    logInfo(s"GetBatch called with start = $start, end = $end")

    if (currentOffsets.isEmpty) {
      currentOffsets = Some(end.json().toLong)
    }
    if (start.isDefined && start.get == end) {
      return sqlContext.internalCreateDataFrame(
        sqlContext.sparkContext.emptyRDD[InternalRow].setName("empty"), schema, isStreaming = true)
    }
    val fromOffsets = start match {
      case Some(prevBatchEndOffset) =>
        prevBatchEndOffset.json().toLong + 1
      case None =>
        initialOffsets.offset
    }

    val endOffset: Long = end.json().toLong

    val (s, e) = offsetColumnType match {
      case _: IntegerType | LongType => (fromOffsets, endOffset)
      case _: TimestampType => (new Timestamp(fromOffsets).toString, new Timestamp(endOffset).toString)
      case _: DataType => (new Date(fromOffsets).toString, new Date(endOffset).toString)
    }

    val strFilter =  s"$offsetColumn >= CAST('$s' AS ${offsetColumnType.sql}) and $offsetColumn <= CAST('$e' AS ${offsetColumnType.sql})"

    val filteredDf = df.where(strFilter)

    // ToDo: clear after debug
    filteredDf.explain()

    val rdd = filteredDf.queryExecution.toRdd


    logInfo(s"Offset: ${start.toString} to ${endOffset.toString}")
    val result = sqlContext.internalCreateDataFrame(rdd, schema, isStreaming = true)
    result
  }

  override def getOffset: Option[Offset] = {
//    val max = getOffsetValue(col(offsetColumn).desc)
//    if (max.isDefined) Some(LongOffset(max.get)) else None

    initialOffsets

    val latest = getOffsetValue(col(offsetColumn).desc)
    val offsets = maxOffsetsPerTrigger match {
      case None =>
        latest.get
//      case Some(limit) if currentOffsets.isEmpty =>
//        rateLimit(limit, initialOffsets, latest.get)
//      case Some(limit) =>
//        rateLimit(limit, currentOffsets.get, latest)
    }

    currentOffsets = Some(offsets)
    logDebug(s"GetOffset: $offsets")
    Some(LongOffset(offsets))
  }

  override def stop(): Unit =
    logWarning("Stop is not implemented!")

}

object JDBCStreamSourceProvider {

  val STARTING_OFFSETS_OPTION_KEY = "startingoffsets"
  val MAX_OFFSET_PER_TRIGGER = "maxoffsetspertrigger"
  val OFFSET_COLUMN = "offsetcolumn"

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

    (shortName(), JDBCStreamSourceProvider.df(sqlContext, parameters).schema)
  }

  override def createSource(
                             sqlContext: SQLContext,
                             metadataPath: String,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]
                           ): Source = {
    val caseInsensitiveParameters = CaseInsensitiveMap(parameters)
    new JDBCStreamSource(sqlContext, providerName, caseInsensitiveParameters, metadataPath)
  }
}

