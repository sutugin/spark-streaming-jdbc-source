package org.apache.spark.sql.execution.streaming.sources

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.OffsetSeqLog
import org.apache.spark.sql.execution.streaming.sources.offset.JDBCOffset

import scala.util.{Failure, Success, Try}

object OffsetUtils {
  def getLastCommitOffset(spark: SparkSession, checkpointRoot: String): Option[JDBCOffset] =
    Try {
      val checkpointDir = new Path(new Path(checkpointRoot), "offsets").toUri.toString
      val offsetSeqLog = new OffsetSeqLog(spark, checkpointDir)
      JDBCOffset.fromJson(offsetSeqLog.getLatest().map(x => x._2).get.offsets.head.get.toString)
    } match {
      case Success(v) => Some(v)
      case Failure(_) => None
    }
}
