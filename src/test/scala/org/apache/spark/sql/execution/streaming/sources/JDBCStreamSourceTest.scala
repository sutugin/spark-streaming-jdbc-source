package org.apache.spark.sql.execution.streaming.sources

import java.sql.{Date, Timestamp}
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

class JDBCStreamSourceTest
    extends FlatSpec
    with Matchers
    with LocalFilesSupport
    with SharedSparkContext
    with DataFrameSuiteBase {

  override implicit lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[1]")
    .appName("spark session")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  override implicit def reuseContextIfPossible: Boolean = true
  import spark.implicits._

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

  private def readStreamIntoMemoryTable(
    spark: SparkSession,
    streamFormat: String,
    tableName: String,
    options: Map[String, String],
    checkpointLocation: String
  ): DataFrame = {
    val stream = spark.readStream
      .format(streamFormat)
      .options(options)
      .load()

    val query = stream.writeStream
      .format(source = "memory")
      .queryName(tableName)
      .option("checkpointLocation", checkpointLocation)
      .start()

    query.processAllAvailable()
    spark.table(tableName)
  }

  private lazy val inputData = Seq(
    (Some(1), "Bob", Timestamp.valueOf("2001-01-01 00:00:00"), Date.valueOf("2019-01-01")),
    (Some(2), "Alice", Timestamp.valueOf("2017-02-20 03:04:00"), Date.valueOf("2019-01-02")),
    (Some(3), "Mike", Timestamp.valueOf("2017-03-02 03:04:00"), Date.valueOf("2019-01-03")),
    (Some(4), "Jon", Timestamp.valueOf("2017-03-15 03:04:00"), Date.valueOf("2019-01-04")),
    (Some(5), "Kurt", Timestamp.valueOf("2017-03-15 03:04:00"), Date.valueOf("2019-01-05"))
  )

  private lazy val columns = Seq("id", "name", "ts", "dt")

  private val fmt = "jdbc-streaming"

  "JDBCStreamSource" should "load all data from table by jdbc with numeric offset column" in {
    val offsetColumn = "id"
    val outputTableName = s"tbl${java.util.UUID.randomUUID.toString.replace('-', 'n')}"
    val expected = inputData.toDF(columns: _*).orderBy(offsetColumn)
    val jdbc = jdbcDefaultParams(outputTableName, offsetColumn)
    writeToJDBC(jdbc, expected, SaveMode.Overwrite)
    val tmpCheckpoint: String = s"${createLocalTempDir("checkopoint")}"
    val actual = readStreamIntoMemoryTable(spark, fmt, outputTableName, jdbc, tmpCheckpoint).orderBy(offsetColumn)
    assertDataFrameEquals(expected, actual)
  }

  it should "load all data from table by jdbc with timestamp offset column" in {
    val offsetColumn = "ts"
    val outputTableName = s"tbl${java.util.UUID.randomUUID.toString.replace('-', 'n')}"
    val expected = inputData.toDF(columns: _*).orderBy(offsetColumn)
    val jdbc = jdbcDefaultParams(outputTableName, offsetColumn)
    writeToJDBC(jdbc, expected, SaveMode.Overwrite)
    val tmpCheckpoint: String = s"${createLocalTempDir("checkopoint")}"
    val actual = readStreamIntoMemoryTable(spark, fmt, outputTableName, jdbc, tmpCheckpoint).orderBy(offsetColumn)

    assertDataFrameEquals(expected, actual)
  }

  it should "load all data from table by jdbc with date offset column" in {
    val offsetColumn = "dt"
    val outputTableName = s"tbl${java.util.UUID.randomUUID.toString.replace('-', 'n')}"
    val expected = inputData.toDF(columns: _*).orderBy(offsetColumn)
    val jdbc = jdbcDefaultParams(outputTableName, offsetColumn)
    writeToJDBC(jdbc, expected, SaveMode.Overwrite)
    val tmpCheckpoint: String = s"${createLocalTempDir("checkopoint")}"
    val actual = readStreamIntoMemoryTable(spark, fmt, outputTableName, jdbc, tmpCheckpoint).orderBy(offsetColumn)

    assertDataFrameEquals(expected, actual)
  }

  it should "load only new rows in each batch by jdbc with numeric offset column" in {
    val offsetColumn = "id"
    val outputTableName = s"tbl${java.util.UUID.randomUUID.toString.replace('-', 'n')}"
    val expectedBefore = inputData.toDF(columns: _*).orderBy(offsetColumn)
    val jdbc = jdbcDefaultParams(outputTableName, offsetColumn)
    writeToJDBC(jdbc, expectedBefore, SaveMode.Append)
    val tmpCheckpoint: String = s"${createLocalTempDir("checkopoint")}"
    val actualBefore = readStreamIntoMemoryTable(spark, fmt, outputTableName, jdbc, tmpCheckpoint).orderBy(offsetColumn)

    assertDataFrameEquals(expectedBefore, actualBefore)

    val updated =
      Seq((Some(6), "666", Timestamp.valueOf("2017-03-15 03:04:00"), Date.valueOf("2019-01-06"))).toDF(columns: _*)
    writeToJDBC(jdbc, updated, SaveMode.Append)

    val actualAfter = spark.sql(s"select * from $outputTableName").orderBy(offsetColumn)

    val expectedAfter = expectedBefore.union(updated).orderBy(offsetColumn)

    assertDataFrameEquals(expectedAfter, actualAfter)

  }

  it should "load only new rows in each batch by jdbc with numeric offset column with specified offset value" in {
    val offsetColumn = "id"
    val outputTableName = s"tbl${java.util.UUID.randomUUID.toString.replace('-', 'n')}"
    val firstBatch = inputData.toDF(columns: _*).orderBy(offsetColumn)
    val jdbc = jdbcDefaultParams(outputTableName, offsetColumn)
    writeToJDBC(jdbc, firstBatch, SaveMode.Append)
    val tmpCheckpoint: String = s"${createLocalTempDir("checkopoint")}"
    val startingOffset = 3
    val ops = jdbc ++ Map("startingoffset" -> startingOffset.toString)

    val actualBefore = readStreamIntoMemoryTable(spark, fmt, outputTableName, ops, tmpCheckpoint).orderBy(offsetColumn)

    val expectedFirstBatch = firstBatch.where(s"$offsetColumn >= $startingOffset")

    assertDataFrameEquals(expectedFirstBatch, actualBefore)

    val updated =
      Seq((Some(6), "666", Timestamp.valueOf("2017-03-15 03:04:00"), Date.valueOf("2019-01-06"))).toDF(columns: _*)
    writeToJDBC(jdbc, updated, SaveMode.Append)

    val actualAfter = spark.sql(s"select * from $outputTableName").orderBy(offsetColumn)
    val expectedAfter = expectedFirstBatch.union(updated).orderBy(offsetColumn)

    assertDataFrameEquals(expectedAfter, actualAfter)
  }

  it should "load only new rows in each batch by jdbc with numeric offset column with specified offset 'latest'" in {
    val offsetColumn = "id"
    val outputTableName = s"tbl${java.util.UUID.randomUUID.toString.replace('-', 'n')}"
    val firstBatch = inputData.toDF(columns: _*).orderBy(offsetColumn)
    val jdbc = jdbcDefaultParams(outputTableName, offsetColumn)
    writeToJDBC(jdbc, firstBatch, SaveMode.Append)
    val tmpCheckpoint: String = s"${createLocalTempDir("checkopoint")}"

    val ops = jdbc ++ Map("startingoffset" -> "latest")

    val actualBefore = readStreamIntoMemoryTable(spark, fmt, outputTableName, ops, tmpCheckpoint).orderBy(offsetColumn)

    firstBatch.orderBy(desc(offsetColumn)).createOrReplaceTempView("firstBatchView")
    val expectedFirstBatch = spark.sql("select * from firstBatchView limit 1")

    assertDataFrameEquals(expectedFirstBatch, actualBefore)

    val updated =
      Seq((Some(6), "666", Timestamp.valueOf("2017-03-15 03:04:00"), Date.valueOf("2019-01-06"))).toDF(columns: _*)
    writeToJDBC(jdbc, updated, SaveMode.Append)

    val actualAfter = spark.sql(s"select * from $outputTableName").orderBy(offsetColumn)
    val expectedAfter = expectedFirstBatch.union(updated).orderBy(offsetColumn)

    assertDataFrameEquals(expectedAfter, actualAfter)
  }

  it should "restoring from checkpoint and continue job" in {
    val offsetColumn = "id"
    val outputTableName = s"tbl${java.util.UUID.randomUUID.toString.replace('-', 'n')}"
    val firstBatch = inputData.toDF(columns: _*).orderBy(offsetColumn)
    val jdbc = jdbcDefaultParams(outputTableName, offsetColumn)
    writeToJDBC(jdbc, firstBatch, SaveMode.Append)
    val tmpOutput = s"${createLocalTempDir("outpute")}"
    val tmpCheckpoint: String = s"${createLocalTempDir("checkopoint")}"
    val q = spark.readStream
      .format(fmt)
      .options(jdbc)
      .load()
      .writeStream
      .format(source = "json")
      .queryName(outputTableName)
      .outputMode("append")
      .option("checkpointLocation", tmpCheckpoint)
      .start(tmpOutput)
    q.processAllAvailable()

    firstBatch.orderBy(desc(offsetColumn)).createOrReplaceTempView("firstBatchView")
    val expectedFirstBatch = spark.sql("select * from firstBatchView").orderBy(offsetColumn)
    val actualBefore = spark.read.schema(firstBatch.schema).json(tmpOutput)
    assertDataFrameEquals(expectedFirstBatch, actualBefore)

    spark.stop()

    val sp: SparkSession = SparkSession
      .builder()
      .master("local[1]")
      .appName("spark session")
      .getOrCreate()
    sp.sparkContext.setLogLevel("WARN")
    import sp.implicits._
    val updated =
      Seq((Some(6), "666", Timestamp.valueOf("2017-03-15 03:04:00"), Date.valueOf("2019-01-06"))).toDF(columns: _*)
    val withUpdated = inputData.toDF(columns: _*).union(updated)

    writeToJDBC(jdbc, withUpdated, SaveMode.Overwrite)
    val q2 = sp.readStream
      .format(fmt)
      .options(jdbc)
      .load()
      .writeStream
      .format(source = "json")
      .queryName(outputTableName)
      .outputMode("append")
      .option("checkpointLocation", tmpCheckpoint)
      .start(tmpOutput)
    q2.processAllAvailable()

    val actualAfter = sp.read.format("json").schema(updated.schema).load(tmpOutput).orderBy(offsetColumn)
    val expectedAfter = inputData.toDF(columns: _*).union(updated).orderBy(offsetColumn)

    assertDataFrameEquals(expectedAfter, actualAfter)

  }
}
