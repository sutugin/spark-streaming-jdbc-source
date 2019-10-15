import sbt._

object Dependencies {
  // spark
  private lazy val sparkVersion = "2.0.0"
  lazy val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion
  lazy val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion

  // test
  lazy val scalatest = "org.scalatest" %% "scalatest" % "3.0.8"
  lazy val sparkTestingBase =  "com.holdenkarau" %% "spark-testing-base" % "2.3.2_0.12.0"

  // jdbc
  lazy val h2database = "com.h2database" % "h2" % "1.4.196"
 }