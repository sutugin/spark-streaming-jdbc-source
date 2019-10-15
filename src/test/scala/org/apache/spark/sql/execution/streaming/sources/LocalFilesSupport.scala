package org.apache.spark.sql.execution.streaming.sources

import java.io.IOException
import java.nio.file.{FileVisitResult, Files, Path, Paths, SimpleFileVisitor}
import java.nio.file.attribute.BasicFileAttributes
import org.scalatest.{BeforeAndAfterAll, Suite}

trait LocalFilesSupport extends BeforeAndAfterAll {
  this: Suite =>

  private var tempDirs = List.empty[Path]

  def createLocalTempDir(prefix: String) = {
    val newTempDir = Files.createTempDirectory(Paths.get(System.getProperty("java.io.tmpdir")), prefix)
    tempDirs = tempDirs :+ newTempDir

    newTempDir.toAbsolutePath.toString
  }

  private def deleteTempDirs(): Unit = {
    def delete(path: Path) =
      Files.walkFileTree(
        path,
        new SimpleFileVisitor[Path]() {
          override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
            Files.delete(file)
            FileVisitResult.CONTINUE
          }

          override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
            Files.delete(dir)
            FileVisitResult.CONTINUE
          }
        }
      )

    tempDirs.foreach(delete)
  }

  override def afterAll(): Unit = {
    super.afterAll()

    deleteTempDirs()
  }
}
