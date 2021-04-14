## Spark structured streaming JDBC source 

- ### Overview:

A library for querying JDBC data with Apache Spark Structured Streaming, for Spark SQL and DataFrames.

- ### Build from Source
In the build.sbt file specify the desired version of spark(2.3 and older) 

``
sbt "set test in assembly := {}" clean assembly
``

- ### Quick Example

```
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder
  .appName("StructuredJDBC")
  .getOrCreate()
  
import spark.implicits._

val jdbcOptions = Map(
    "user" -> "user",
    "password" -> "password",
    "database" -> "db name",
    "driver" -> "org.h2.Driver",
    "url" -> "jdbc:h2:mem:myDb;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false"
  )

// Create DataFrame representing the stream of input lines from jdbc
val stream = spark.readStream
      .format("jdbc-streaming")
      .options(jdbcOptions + ("dbtable" -> "source") + ("offsetColumn" -> "offsetColumn"))
      .load

// Start running the query that prints 'select result' to the console
val query = stream.writeStream
  .outputMode("append")
  .format("console")
  .start()

query.awaitTermination()

```

- ### Features


All JDBC connection parameters are set as in non-streaming reading (https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html),
except for the following:

#### ``offsetColumn`` : Required field, name of the column by which changes will be tracked. 

#### ``startingoffset`` : The start point when a query is started, either ``"earliest"`` (default value) which is from the min offsetColumn value, ``"latest"`` which is just from the max offsetColumn value, or a string specifying a starting offset:
- Numeric  ``"0"`` or ``"1.4"``
- TimestampType ``"2019-01-30 00:10:00"``
- DataType ``"2019-03-20""``

### ToDo:
- Validate input options.
- make 'maxoffsetspertrigger' property. 
