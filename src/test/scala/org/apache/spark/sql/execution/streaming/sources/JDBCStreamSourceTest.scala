package org.apache.spark.sql.execution.streaming.sources

import java.sql.{Date, Timestamp}

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.execution.streaming.sources.offset.{JDBCOffset, OffsetRange}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

class JDBCStreamSourceTest
    extends FlatSpec
    with Matchers
    with LocalFilesSupport
    with DataFrameSuiteBase
    with SharedSparkContext {

  private def jdbcDefaultParams(tableName: String, offsetColumn: String): Map[String, String] =
    Map(
      "user" -> "whatever_user",
      "password" -> "whatever_password",
      "database" -> "h2_db",
      "driver" -> "org.h2.Driver",
      "url" -> "jdbc:h2:mem:myDb;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false",
      "dbtable" -> tableName,
      "offsetColumn" -> offsetColumn
    )

  private def writeToJDBC(dbConfig: Map[String, String], data: DataFrame, saveMode: SaveMode): Unit =
    data.write
      .mode(saveMode)
      .format(source = "jdbc")
      .options(dbConfig)
      .save()

  private def saveStreamingDataToTempDir(
    options: Map[String, String],
    checkpointLocation: String,
    outPutDir: String,
    spark: SparkSession
  ): Unit = {
    val q = spark.readStream
      .format(fmt)
      .options(options)
      .load()
      .writeStream
      .outputMode("append")
      .format(source = "json")
      .option("checkpointLocation", checkpointLocation)
      .start(outPutDir)

    q.processAllAvailable()
  }

  private lazy val inputData = Seq(
    (Some(1), 1.11, "Bob", Timestamp.valueOf("2001-01-01 00:00:00"), Date.valueOf("2019-01-01")),
    (Some(2), 2.22, "Alice", Timestamp.valueOf("2017-02-20 03:04:00"), Date.valueOf("2019-01-02")),
    (Some(3), 3.33, "Mike", Timestamp.valueOf("2017-03-02 03:04:00"), Date.valueOf("2019-01-03")),
    (Some(4), 4.44, "Jon", Timestamp.valueOf("2017-03-15 03:04:00"), Date.valueOf("2019-01-04")),
    (Some(5), 5.55, "Kurt", Timestamp.valueOf("2017-03-15 03:04:00"), Date.valueOf("2019-01-05"))
  )
  private lazy val columns = Seq("id", "rate", "name", "ts", "dt")

  private val fmt = "jdbc-streaming"

  "JDBCStreamSource" should "load all data from table by jdbc with numeric offset column" in {
    import spark.implicits._
    val offsetColumn = "id"
    val jdbcTableName = s"tbl${java.util.UUID.randomUUID.toString.replace('-', 'n')}"
    val expected = inputData.toDF(columns: _*).orderBy(offsetColumn)
    val jdbc = jdbcDefaultParams(jdbcTableName, offsetColumn)
    writeToJDBC(jdbc, expected, SaveMode.Overwrite)
    val tmpCheckpoint = s"${createLocalTempDir("checkopoint")}"
    val tmpOutputDir = s"${createLocalTempDir("output")}"

    saveStreamingDataToTempDir(jdbc, tmpCheckpoint, tmpOutputDir, spark)

    val actual = spark.read.schema(expected.schema).json(tmpOutputDir).orderBy(offsetColumn)

    assertDataFrameEquals(expected, actual)
  }

  it should "load all data from table by jdbc with floating point offset column" in {
    import spark.implicits._
    val offsetColumn = "rate"
    val jdbcTableName = s"tbl${java.util.UUID.randomUUID.toString.replace('-', 'n')}"
    val expected = inputData.toDF(columns: _*).orderBy(offsetColumn)
    val jdbc = jdbcDefaultParams(jdbcTableName, offsetColumn)
    writeToJDBC(jdbc, expected, SaveMode.Overwrite)
    val tmpCheckpoint = s"${createLocalTempDir("checkopoint")}"
    val tmpOutputDir = s"${createLocalTempDir("output")}"

    saveStreamingDataToTempDir(jdbc, tmpCheckpoint, tmpOutputDir, spark)

    val actual = spark.read.schema(expected.schema).json(tmpOutputDir).orderBy(offsetColumn)
    assertDataFrameEquals(expected, actual)
  }

  it should "load all data from table by jdbc with timestamp offset column" in {
    import spark.implicits._
    val offsetColumn = "ts"
    val jdbcTableName = s"tbl${java.util.UUID.randomUUID.toString.replace('-', 'n')}"
    val expected = inputData.toDF(columns: _*).orderBy(offsetColumn)
    val jdbc = jdbcDefaultParams(jdbcTableName, offsetColumn)
    writeToJDBC(jdbc, expected, SaveMode.Overwrite)
    val tmpCheckpoint = s"${createLocalTempDir("checkopoint")}"
    val tmpOutputDir = s"${createLocalTempDir("output")}"

    saveStreamingDataToTempDir(jdbc, tmpCheckpoint, tmpOutputDir, spark)

    val actual = spark.read.schema(expected.schema).json(tmpOutputDir).orderBy(offsetColumn)
    assertDataFrameEquals(expected, actual)
  }

  it should "load all data from table by jdbc with date offset column" in {
    import spark.implicits._
    val offsetColumn = "dt"
    val jdbcTableName = s"tbl${java.util.UUID.randomUUID.toString.replace('-', 'n')}"
    val expected = inputData.toDF(columns: _*).orderBy(offsetColumn)
    val jdbc = jdbcDefaultParams(jdbcTableName, offsetColumn)
    writeToJDBC(jdbc, expected, SaveMode.Overwrite)
    val tmpCheckpoint = s"${createLocalTempDir("checkopoint")}"
    val tmpOutputDir = s"${createLocalTempDir("output")}"

    saveStreamingDataToTempDir(jdbc, tmpCheckpoint, tmpOutputDir, spark)

    val actual = spark.read.schema(expected.schema).json(tmpOutputDir).orderBy(offsetColumn)
    assertDataFrameEquals(expected, actual)
  }

  it should "load only new rows in each batch by jdbc with numeric offset column with specified offset value" in {
    import spark.implicits._
    val offsetColumn = "id"
    val jdbcTableName = s"tbl${java.util.UUID.randomUUID.toString.replace('-', 'n')}"
    val expectedFirstBatch = inputData.toDF(columns: _*).orderBy(offsetColumn)
    val startingOffset = "3"
    val jdbc = jdbcDefaultParams(jdbcTableName, offsetColumn) ++ Map("startingoffset" -> startingOffset)
    writeToJDBC(jdbc, expectedFirstBatch, SaveMode.Append)
    val tmpCheckpoint = s"${createLocalTempDir("checkopoint")}"
    val tmpOutputDir = s"${createLocalTempDir("output")}"
    saveStreamingDataToTempDir(jdbc, tmpCheckpoint, tmpOutputDir, spark)

    val actual = spark.read.schema(expectedFirstBatch.schema).json(tmpOutputDir).orderBy(offsetColumn)
    val expected = inputData.toDF(columns: _*).where(s"$offsetColumn >= $startingOffset").orderBy(offsetColumn)

    assertDataFrameEquals(expected, actual)

    val offsetFromCheckpoint = OffsetUtils.getLastCommitOffset(spark, tmpCheckpoint)
    val expectedOffset = Some(JDBCOffset(offsetColumn, OffsetRange(Some(startingOffset), Some("5"))))

    expectedOffset shouldBe offsetFromCheckpoint
  }

  it should "load only new rows in each batch by jdbc with numeric offset column with specified offset 'latest'" in {
    import spark.implicits._
    val offsetColumn = "id"
    val jdbcTableName = s"tbl${java.util.UUID.randomUUID.toString.replace('-', 'n')}"
    val expectedFirstBatch = inputData.toDF(columns: _*).orderBy(offsetColumn)
    val startingOffset = "latest"
    val jdbc = jdbcDefaultParams(jdbcTableName, offsetColumn) ++ Map("startingoffset" -> startingOffset)
    writeToJDBC(jdbc, expectedFirstBatch, SaveMode.Append)
    val tmpCheckpoint = s"${createLocalTempDir("checkopoint")}"
    val tmpOutputDir = s"${createLocalTempDir("output")}"
    saveStreamingDataToTempDir(jdbc, tmpCheckpoint, tmpOutputDir, spark)

    val actual = spark.read.schema(expectedFirstBatch.schema).json(tmpOutputDir).orderBy(offsetColumn)
    val expected = inputData.toDF(columns: _*).where(s"$offsetColumn >= 5").orderBy(offsetColumn)

    assertDataFrameEquals(expected, actual)

    val offsetFromCheckpoint = OffsetUtils.getLastCommitOffset(spark, tmpCheckpoint)
    val expectedOffset = Some(JDBCOffset(offsetColumn, OffsetRange(Some("5"), Some("5"))))

    expectedOffset shouldBe offsetFromCheckpoint
  }

  it should "load only new rows in each batch by jdbc with numeric offset column with specified offset 'earliest'" in {
    import spark.implicits._
    val offsetColumn = "id"
    val jdbcTableName = s"tbl${java.util.UUID.randomUUID.toString.replace('-', 'n')}"
    val expectedFirstBatch = inputData.toDF(columns: _*).orderBy(offsetColumn)
    val startingOffset = "earliest"
    val jdbc = jdbcDefaultParams(jdbcTableName, offsetColumn) ++ Map("startingoffset" -> startingOffset)
    writeToJDBC(jdbc, expectedFirstBatch, SaveMode.Append)
    val tmpCheckpoint = s"${createLocalTempDir("checkopoint")}"
    val tmpOutputDir = s"${createLocalTempDir("output")}"
    saveStreamingDataToTempDir(jdbc, tmpCheckpoint, tmpOutputDir, spark)

    val actual = spark.read.schema(expectedFirstBatch.schema).json(tmpOutputDir).orderBy(offsetColumn)
    val expected = inputData.toDF(columns: _*).orderBy(offsetColumn)

    assertDataFrameEquals(expected, actual)

    val offsetFromCheckpoint = OffsetUtils.getLastCommitOffset(spark, tmpCheckpoint)
    val expectedOffset = Some(JDBCOffset(offsetColumn, OffsetRange(Some("1"), Some("5"))))

    expectedOffset shouldBe offsetFromCheckpoint
  }
}
