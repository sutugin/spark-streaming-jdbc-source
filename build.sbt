lazy val defaultScalaVersion = "2.11.12"
lazy val defaultSparkVersion = "2.3.2"
lazy val releaseVersion = settingKey[String]("Global assembly version")
lazy val sparkVersion = settingKey[String]("Spark version")
lazy val jdbcStreamingSourceVersion =
  settingKey[String]("Spark streaming jdbc source version without Spark version part")

lazy val root = (project in file("."))
  .settings(
    name := "spark-streaming-jdbc-source",
    commonSettings,
    publishSettings,
    crossScalaVersions := {
      if (sparkVersion.value >= "2.4.0") {
        Seq("2.12.8")
      } else if (sparkVersion.value >= "2.3.0") {
        Seq("2.11.12")
      } else {
        Seq("2.10.6", "2.11.11")
      }
    },
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion.value % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion.value % Provided,
      "org.scalatest" %% "scalatest" % "3.0.8" % Test,
      "com.h2database" % "h2" % "1.4.196" % Test,
      "com.holdenkarau" %% "spark-testing-base" % "2.3.2_0.12.0" % Test
    )
  )

lazy val scalacOptionsSettings = Seq(
  "-deprecation", // Emit warning and location for usages of deprecated APIs.
  "-encoding",
  "utf-8", // Specify character encoding used by source files.
  "-explaintypes", // Explain type errors in more detail.
  "-feature", // Emit warning and location for usages of features that should be imported explicitly.
  "-language:existentials", // Existential types (besides wildcard types) can be written and inferred
  "-language:experimental.macros", // Allow macro definition (besides implementation and application)
  "-language:higherKinds", // Allow higher-kinded types
  "-language:implicitConversions", // Allow definition of implicit functions called views
  "-unchecked", // Enable additional warnings where generated code depends on assumptions.
  "-Xcheckinit", // Wrap field accessors to throw an exception on uninitialized access.
  "-Xfatal-warnings", // Fail the compilation if there are any warnings.
  "-Xfuture", // Turn on future language features.
  "-Yno-adapted-args", // Do not adapt an argument list (either by inserting () or creating a tuple) to match the receiver.
  "-Ywarn-dead-code", // Warn when dead code is identified.
  "-Ywarn-inaccessible", // Warn about inaccessible types in method signatures.
  "-Ywarn-infer-any", // Warn when a type argument is inferred to be `Any`.
  "-Ywarn-nullary-override", // Warn when non-nullary `def f()' overrides nullary `def f'.
  "-Ywarn-nullary-unit", // Warn when nullary methods return Unit.
  "-Ywarn-numeric-widen", // Warn when numerics are widened.
  "-Ywarn-unused", // Warn is unused.
  "-Ywarn-value-discard" // Warn when non-Unit expression results are unused.
)

lazy val commonSettings = Seq(
  organization := "sutugin",
  publishMavenStyle := true,
  sparkVersion := System.getProperty("sparkVersion", defaultSparkVersion),
  releaseVersion := "0.0.1",
  version := sparkVersion.value + "_" + releaseVersion.value,
  scalaVersion := {
    if (sparkVersion.value >= "2.4.0") {
      "2.12.8"
    } else if (sparkVersion.value >= "2.0.0") {
      defaultScalaVersion
    } else {
      throw new IllegalArgumentException(s"spark version must be more than 2.0.0")
    }
  },
  scalacOptions ++= scalacOptionsSettings,
  javacOptions ++= {
    if (sparkVersion.value >= "2.1.1") {
      Seq("-source", "1.8", "-target", "1.8")
    } else {
      Seq("-source", "1.7", "-target", "1.7")
    }
  },
  parallelExecution in Test := false,
  fork := true,
  resolvers ++= Seq(
    ("sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/").withAllowInsecureProtocol(true),
    ("Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/").withAllowInsecureProtocol(true),
    ("Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/").withAllowInsecureProtocol(true),
    ("Artima Maven Repository" at "http://repo.artima.com/releases").withAllowInsecureProtocol(true),
    ("datanucleus" at "http://www.datanucleus.org/downloads/maven2/").withAllowInsecureProtocol(true),
    Resolver.sonatypeRepo("public")
  ),
  artifact in (Compile, assembly) := {
    val art = (artifact in (Compile, assembly)).value
    art.withClassifier(None)
  },
  logBuffered in Test := false,
  assemblyJarName in assembly := s"${name.value}-${version.value}.jar"
)

// publish settings
lazy val publishSettings = Seq(
  pomIncludeRepository := { _ =>
    false
  },
  licenses := Seq("Apache License 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")),
  homepage := Some(url("https://github.com/sutugin/spark-streaming-jdbc-source")),
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/sutugin/spark-streaming-jdbc-source.git"),
      "scm:git@github.com:sutugin/spark-streaming-jdbc-source.git"
    )
  ),
  developers := List(
    Developer("sutugin", "Andrey Sutugin", "sutuginandrey@gmail.com", url("https://www.linkedin.com/in/sutugin"))
  )
)

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")     => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
  case "log4j.properties"                             => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") =>
    MergeStrategy.concat
  case _ => MergeStrategy.first
}

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")
