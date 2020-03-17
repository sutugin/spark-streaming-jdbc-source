package org.apache.spark.sql.execution.streaming.sources

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.sources.offset._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SQLContext}

import scala.util.{Failure, Success, Try}

class JDBCStreamSource(
  sqlContext: SQLContext,
  providerName: String,
  parameters: Map[String, String],
  metadataPath: String,
  df: DataFrame
) extends Source
    with Logging {

  import JDBCStreamSource._
  import sqlContext.implicits._

  // ToDo: implement
  // private val maxOffsetsPerTrigger = None

  private val offsetColumn =
    parameters.getOrElse(OFFSET_COLUMN, throw new IllegalArgumentException(s"Parameter not found: $OFFSET_COLUMN"))

  override def schema: StructType = df.schema

  private val offsetColumnType = getType(offsetColumn, schema)

  private var currentOffset: Option[JDBCOffset] = None

  private def getOffsetValue(sortFunc: String => Column) =
    Try { df.select(offsetColumn).orderBy(sortFunc(offsetColumn)).as[String].first } match {
      case Success(value) => Some(value)
      case Failure(ex)    => logWarning(s"Not found offset ${ex.getStackTrace.mkString("\n")}"); None
    }

  private def initFirstOffset(): Unit = {
    val start = startingOffset match {
      case EarliestOffsetRangeLimit    => SpecificOffsetRangeLimit(getOffsetValue(asc).get)
      case LatestOffsetRangeLimit      => SpecificOffsetRangeLimit(getOffsetValue(desc).get)
      case SpecificOffsetRangeLimit(p) => SpecificOffsetRangeLimit(p)
    }
    val end = SpecificOffsetRangeLimit(getOffsetValue(desc).get)

    val offsetRange = OffsetRange(Some(start.toString), Some(end.toString))
    currentOffset = Some(JDBCOffset(offsetColumn, offsetRange))
  }

  private val startingOffset = {
    val startingOffset = parameters.get(STARTING_OFFSETS_OPTION_KEY)
    val offset = startingOffset.getOrElse(EarliestOffsetRangeLimit.toString)
    offset match {
      case EarliestOffsetRangeLimit.toString => EarliestOffsetRangeLimit
      case LatestOffsetRangeLimit.toString   => LatestOffsetRangeLimit
      case v                                 => SpecificOffsetRangeLimit(v)
    }
  }

  private def toBatchRange(offset: Offset, startInclusionType: JDBCOffsetFilterType): BatchOffsetRange =
    offset match {
      case o: JDBCOffset       => toBatchRange(o, startInclusionType)
      case o: SerializedOffset => toBatchRange(JDBCOffset.fromJson(o.json), startInclusionType)
      case o                   => throw new IllegalArgumentException(s"Unknown offset type: '${o.getClass.getCanonicalName}'")
    }

  private def toBatchRange(offset: JDBCOffset, startInclusionType: JDBCOffsetFilterType): BatchOffsetRange = {
    val range = offset.range
    if (range.start.isEmpty || range.end.isEmpty) throw new IllegalArgumentException(s"Invalid range informed: $range")
    BatchOffsetRange(range.start.get, range.end.get, startInclusionType)
  }

  private def resolveBatchRange(start: Option[Offset], end: Offset): BatchOffsetRange =
    if (start.isEmpty) {
      toBatchRange(end, InclusiveJDBCOffsetFilterType)
    } else {
      val previousBatchRange = toBatchRange(start.get, InclusiveJDBCOffsetFilterType)
      val nextBatchRange = toBatchRange(end, InclusiveJDBCOffsetFilterType)
      BatchOffsetRange(previousBatchRange.end, nextBatchRange.end, ExclusiveJDBCOffsetFilterType)
    }

  private def getBatchData(range: BatchOffsetRange): DataFrame = {
    val strFilter =
      s"$offsetColumn ${JDBCOffsetFilterType.getStartOperator(range.startInclusion)} CAST('${range.start}' AS ${offsetColumnType.sql}) and $offsetColumn <= CAST('${range.end}' AS ${offsetColumnType.sql})"
    val filteredDf = df.where(strFilter)
    val rdd = filteredDf.queryExecution.toRdd
    val result = sqlContext.internalCreateDataFrame(rdd, schema, isStreaming = true)
    logInfo(s"Offset: '${range.start}' to '${range.end}'")
    result
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    logInfo(s"GetBatch  start = '$start', end = '$end'")
    end match {
      case offset: SerializedOffset =>
        logInfo(
          "Restoring state and returning empty as this offset " +
            "was processed by the last batch"
        )

        currentOffset = Some(JDBCOffset.fromJson(offset.json))
        logInfo(s"Offsets restored to '$currentOffset'")

        sqlContext.internalCreateDataFrame(
          sqlContext.sparkContext.emptyRDD[InternalRow].setName("empty"),
          schema,
          isStreaming = true
        )

      case _ =>
        val batchRange = resolveBatchRange(start, end)
        val batchData = getBatchData(batchRange)
        batchData

    }
  }

  override def stop(): Unit =
    logWarning("Stop is not implemented!")

  private def updateCurrentOffsets(newEndOffset: String): Unit = {
    val newStartOffset = currentOffset.get.range.end
    val newOffsetRange = OffsetRange(newStartOffset, Some(newEndOffset))
    logInfo(s"Updating offsets: FROM '${currentOffset.get.range}' TO '$newOffsetRange'")
    currentOffset = Some(JDBCOffset(offsetColumn, newOffsetRange))
  }

  override def getOffset: Option[Offset] =
    if (currentOffset.isEmpty) {
      logInfo("No offset, will try to get it from the source.")
      initFirstOffset()
      logInfo(s"Offsets retrieved from data: '$currentOffset'.")
      currentOffset
    } else {
      getOffsetValue(desc) match {
        case Some(candidateNewEndOffset) if candidateNewEndOffset != currentOffset.get.range.end.get =>
          updateCurrentOffsets(newEndOffset = candidateNewEndOffset)
          logInfo(s"New offset found: '$currentOffset'.")
          currentOffset
        case _ =>
          logDebug(s"No new offset found. Previous offset: $currentOffset")
          None
      }
    }

  private def getType(columnName: String, schema: StructType): DataType = {
    val sqlField = schema.fields
      .find(_.name.toLowerCase == columnName.toLowerCase)
      .getOrElse(throw new IllegalArgumentException(s"Column not found in schema: '$columnName'"))
    sqlField.dataType
  }
}

object JDBCStreamSource {
  val STARTING_OFFSETS_OPTION_KEY = "startingoffset"
  val OFFSET_COLUMN = "offsetcolumn"
}
