package org.apache.spark.sql.execution.streaming.sources

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.sources.v2.DataSourceV2
import org.apache.spark.sql.types.StructType

class JDBCStreamSourceProvider extends StreamSourceProvider with DataSourceV2 with DataSourceRegister {

  private lazy val df = (sqlContext: SQLContext, parameters: Map[String, String]) => {
    sqlContext.sparkSession.read
      .format("jdbc")
      .options(parameters)
      .load
  }

  /**
   * Alias name for this data source.
   */
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
