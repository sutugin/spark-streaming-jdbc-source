package org.apache.spark.sql.execution.streaming.sources

import java.sql.{Timestamp, Date}
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

class JDBCStreamSourceTest extends FlatSpec with Matchers with LocalFilesSupport with SharedSparkContext with DataFrameSuiteBase {

  override implicit lazy val spark = SparkSession
    .builder()
    .master("local")
    .appName("spark session")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  override implicit def reuseContextIfPossible: Boolean = true
  import spark.implicits._

  lazy val jdbcOptions = Map(
    "user"     -> "sa",
    "password" -> "dSzme8=/b*{:iqGI",
    "database" -> "h2_db",
    "driver" -> "org.h2.Driver",
    "url"    -> "jdbc:h2:mem:myDb;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false"
  )

  lazy val inputData = Seq(
    (Some(1), "Bob", Timestamp.valueOf("2001-01-01 00:00:00"), Date.valueOf("2019-01-01")),
    (Some(2), "Alice", Timestamp.valueOf("2017-02-20 03:04:00"), Date.valueOf("2019-01-02")),
    (Some(3), "Mike", Timestamp.valueOf("2017-03-02 03:04:00"), Date.valueOf("2019-01-03")),
    (Some(4), "Jon", Timestamp.valueOf("2017-03-15 03:04:00"), Date.valueOf("2019-01-04")),
    (Some(5), "Kurt",  Timestamp.valueOf("2017-03-15 03:04:00"), Date.valueOf("2019-01-05"))
  )

  lazy val columns = Seq("id", "name", "ts", "dt")

  "JDBCStreamSource" should "load all data from table by jdbc with numeric offset column" in {
    val offsetColumn = "id"
    val outputTableName = "outTable"
    val expected = inputData.toDF(columns: _*).orderBy(offsetColumn)
    expected.write.mode("overwrite").format("jdbc").options(jdbcOptions + ("dbtable" -> "source")).save()
    val fmt = "jdbc-streaming"


    val tmpCheckpoint: String = s"${createLocalTempDir("checkopoint")}"

    val stream = spark.readStream
      .format(fmt)
      .options(jdbcOptions + ("dbtable" -> "source") + ("offsetColumn" -> offsetColumn))
      .load

    val out = stream.writeStream
      .option("checkpointLocation", tmpCheckpoint)
      .outputMode("append")
      .format("memory")
      .queryName(outputTableName)
      .start()

    out.processAllAvailable()

    val actual = spark.sql(s"select * from $outputTableName").orderBy(offsetColumn)

    assertDataFrameEquals(expected, actual)

    out.stop()
  }

  it should "load all data from table by jdbc with timestamp offset column" in {
    val offsetColumn = "ts"
    val outputTableName = "outTable"
    val expected = inputData.toDF(columns: _*).orderBy(offsetColumn)
    expected.write.mode("overwrite").format("jdbc").options(jdbcOptions + ("dbtable" -> "source")).save()
    val fmt = "jdbc-streaming"


    val tmpCheckpoint: String = s"${createLocalTempDir("checkopoint")}"

    val stream = spark.readStream
      .format(fmt)
      .options(jdbcOptions + ("dbtable" -> "source") + ("offsetColumn" -> offsetColumn))
      .load

    val out = stream.writeStream
      .option("checkpointLocation", tmpCheckpoint)
      .outputMode("append")
      .format("memory")
      .queryName(outputTableName)
      .start()

    out.processAllAvailable()

    val actual = spark.sql(s"select * from $outputTableName").orderBy(offsetColumn)

    assertDataFrameEquals(expected, actual)

    out.stop()
  }

  it should "load all data from table by jdbc with date offset column" in {
    val offsetColumn = "dt"
    val outputTableName = "outTable"
    val expected = inputData.toDF(columns: _*).orderBy(offsetColumn)
    expected.write.mode("overwrite").format("jdbc").options(jdbcOptions + ("dbtable" -> "source")).save()
    val fmt = "jdbc-streaming"


    val tmpCheckpoint: String = s"${createLocalTempDir("checkopoint")}"

    val stream = spark.readStream
      .format(fmt)
      .options(jdbcOptions + ("dbtable" -> "source") + ("offsetColumn" -> offsetColumn))
      .load

    val out = stream.writeStream
      .option("checkpointLocation", tmpCheckpoint)
      .outputMode("append")
      .format("memory")
      .queryName(outputTableName)
      .start()

    out.processAllAvailable()

    val actual = spark.sql(s"select * from $outputTableName").orderBy(offsetColumn)

    assertDataFrameEquals(expected, actual)

    out.stop()
  }

  it should "load only new rows in each batch by jdbc with numeric offset column" in {
    val offsetColumn = "id"
    val outputTableName = "outTable"
    val expectedBefore = inputData.toDF(columns: _*).orderBy(offsetColumn)
    expectedBefore.write.mode("overwrite").format("jdbc").options(jdbcOptions + ("dbtable" -> "source")).save()
    val fmt = "jdbc-streaming"


    val tmpCheckpoint: String = s"${createLocalTempDir("checkopoint")}"

    val stream = spark.readStream
      .format(fmt)
      .options(jdbcOptions + ("dbtable" -> "source") + ("offsetColumn" -> offsetColumn))
      .load

    val out = stream.writeStream
      .option("checkpointLocation", tmpCheckpoint)
      .outputMode("append")
      .format("memory")
      .queryName(outputTableName)
      .start()

    out.processAllAvailable()

    val actualBefore = spark.sql(s"select * from $outputTableName").orderBy(offsetColumn)

    assertDataFrameEquals(expectedBefore, actualBefore)

    val updated = Seq((Some(6), "666",  Timestamp.valueOf("2017-03-15 03:04:00"), Date.valueOf("2019-01-06"))).toDF(columns: _*)
    updated.write.mode("append").format("jdbc").options(jdbcOptions + ("dbtable" -> "source")).save()

    out.processAllAvailable()
    val actualAfter = spark.sql(s"select * from $outputTableName").orderBy(offsetColumn)



    val expectedAfter = expectedBefore.union(updated).orderBy(offsetColumn)

    assertDataFrameEquals(expectedAfter, actualAfter)

    out.stop()
  }

  it should "load only new rows in each batch by jdbc with numeric offset column with specified offset value" in {
    val offsetColumn = "id"
    val outputTableName = "outTable"
    val expectedBefore = inputData.toDF(columns: _*).orderBy(offsetColumn)
    expectedBefore.write.mode("overwrite").format("jdbc").options(jdbcOptions + ("dbtable" -> "source")).save()
    val fmt = "jdbc-streaming"

    val tmpCheckpoint: String = s"${createLocalTempDir("checkopoint")}" //"/tmp/checkopoint1"//

    val stream = spark.readStream
      .format(fmt)
      .options(jdbcOptions + ("dbtable" -> "source") + ("offsetColumn" -> offsetColumn) + ("startingoffsets" -> "3"))
      .load

    val out = stream.writeStream
      .option("checkpointLocation", tmpCheckpoint)
      .outputMode("append")
      .format("console")
      .queryName(outputTableName)
      .start()

    out.processAllAvailable()

    //    val actualBefore = spark.sql(s"select * from $outputTableName").orderBy(offsetColumn)
    //
    //    assertDataFrameEquals(expectedBefore, actualBefore)

    val updated = Seq((Some(6), "666",  Timestamp.valueOf("2017-03-15 03:04:00"), Date.valueOf("2019-01-06"))).toDF(columns: _*)
    updated.write.mode("append").format("jdbc").options(jdbcOptions + ("dbtable" -> "source")).save()

    out.processAllAvailable()
    //    val actualAfter = spark.sql(s"select * from $outputTableName").orderBy(offsetColumn)
    //
    //
    //    val expectedAfter = expectedBefore.union(updated).orderBy(offsetColumn)
    //
    //    assertDataFrameEquals(expectedAfter, actualAfter)

    out.stop()
  }

  it should "load only new rows in each batch by jdbc with numeric offset column with specified offset 'latest'" in {
    val offsetColumn = "id"
    val outputTableName = "outTable"
    val expectedBefore = inputData.toDF(columns: _*).orderBy(offsetColumn)
    expectedBefore.write.mode("overwrite").format("jdbc").options(jdbcOptions + ("dbtable" -> "source")).save()
    val fmt = "jdbc-streaming"

    val tmpCheckpoint: String = s"${createLocalTempDir("checkopoint")}" //

    val stream = spark.readStream
      .format(fmt)
      .options(jdbcOptions + ("dbtable" -> "source") + ("offsetColumn" -> offsetColumn) + ("startingoffsets" -> "latest"))
      .load

    val out = stream.writeStream
      .option("checkpointLocation", tmpCheckpoint)
      .outputMode("append")
      .format("console")
      .queryName(outputTableName)
      .start()

    out.processAllAvailable()

    //    val actualBefore = spark.sql(s"select * from $outputTableName").orderBy(offsetColumn)
    //
    //    assertDataFrameEquals(expectedBefore, actualBefore)

    val updated = Seq((Some(6), "666",  Timestamp.valueOf("2017-03-15 03:04:00"), Date.valueOf("2019-01-06"))).toDF(columns: _*)
    updated.write.mode("append").format("jdbc").options(jdbcOptions + ("dbtable" -> "source")).save()

    out.processAllAvailable()
    //    val actualAfter = spark.sql(s"select * from $outputTableName").orderBy(offsetColumn)
    //
    //
    //    val expectedAfter = expectedBefore.union(updated).orderBy(offsetColumn)
    //
    //    assertDataFrameEquals(expectedAfter, actualAfter)

    out.stop()
  }
}
