package org.apache.spark.util

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.commons.io.{FileUtils, IOUtils}
import scala.collection.mutable.HashSet
import java.io._
import org.apache.spark.{SparkConf, Logging}
import org.apache.spark.util.{RollingFileAppender, FileAppender}

class FileAppenderSuite extends FunSuite with BeforeAndAfter with Logging {

  val testFile = new File("FileAppenderSuite-test-" + System.currentTimeMillis).getAbsoluteFile

  before {
    cleanup()
  }

  after {
    cleanup()
  }

  test("basic file appender") {
    val testString = (1 to 1000).mkString(", ")
    val inputStream = IOUtils.toInputStream(testString)
    val appender = new FileAppender(inputStream, testFile)
    inputStream.close()
    appender.awaitTermination()
    assert(FileUtils.readFileToString(testFile) === testString)
  }

  test("rolling file appender - rolling") {
    // setup input stream and appender
    val testOutputStream = new PipedOutputStream()
    val testInputStream = new PipedInputStream(testOutputStream, 100 * 1000)
    val rolloverIntervalMillis = 100
    val durationMillis = 1000
    val numRollovers = durationMillis / rolloverIntervalMillis
    val appender = new RollingFileAppender(testInputStream,
      testFile, rolloverIntervalMillis, s"'${testFile.getName}'-HH-mm-ss-SSSS", new SparkConf, 10)

    // send data to appender through the input stream, and wait for the data to be written
    val items = (1 to numRollovers).map { _.toString * 10 }
    val expectedText = items.mkString("")
    for (i <- 0 until numRollovers) {
      testOutputStream.write(items(i).getBytes("UTF8"))
      testOutputStream.flush()
      Thread.sleep(rolloverIntervalMillis)
    }
    logInfo("Data sent to appender")
    testOutputStream.close()
    appender.awaitTermination()
    logInfo("Appender closed")

    // verify whether all the data written to rolled over files is same as expected
    val generatedFiles = getGeneratedFiles()
    logInfo("Filtered files: \n" + generatedFiles.mkString("\n"))
    assert(generatedFiles.size > 1)
    val allText = generatedFiles.map{ file =>
      FileUtils.readFileToString(file)
    }.mkString("")
    assert(allText === expectedText)
  }

  test("rolling file appender - cleaning") {
    // setup input stream and appender
    val testOutputStream = new PipedOutputStream()
    val testInputStream = new PipedInputStream(testOutputStream, 100 * 1000)
    val rolloverIntervalMillis = 100
    val durationMillis = 400
    val numRollovers = durationMillis / rolloverIntervalMillis
    val conf = new SparkConf().set("spark.executor.rolloverLogs.cleanupTtl", "200")
    val appender = new RollingFileAppender(testInputStream,
      testFile, rolloverIntervalMillis, s"'${testFile.getName}'-HH-mm-ss-SSSS", conf, 10)

    // send data to appender through the input stream, and wait for the data to be written
    val allGeneratedFiles = new HashSet[String]()
    val items = (1 to numRollovers).map { _.toString * 10 }
    for (i <- 0 until numRollovers) {
      testOutputStream.write(items(i).getBytes("UTF8"))
      testOutputStream.flush()
      Thread.sleep(rolloverIntervalMillis)
      allGeneratedFiles ++= getGeneratedFiles().map { _.toString }
    }
    testOutputStream.close()
    appender.awaitTermination()
    appender.cleanup()
    logInfo("Appender closed")

    // verify whether the earliest file has been deleted
    val rolledOverFiles = allGeneratedFiles.filter { _ == testFile.toString }.toArray.sorted
    logInfo("All rolled over files generated:\n" + rolledOverFiles.mkString("\n"))
    assert(allGeneratedFiles.size > 2)
    val earliestRolledOverFile = rolledOverFiles.head
    val existingRolledOverFiles = getGeneratedFiles().map { _.toString }.toSet
    logInfo("Existing rolled over files:\n" + existingRolledOverFiles.mkString("\n"))
    assert(existingRolledOverFiles.contains(earliestRolledOverFile))
  }

  def getGeneratedFiles() = {
    val testFileName = testFile.getName
    testFile.getParentFile.listFiles.filter { file =>
      val fileName = file.getName
      fileName.startsWith(testFileName) && fileName != testFileName
    }.sorted ++ Seq(testFile)
  }

  def cleanup() {
    testFile.getParentFile.listFiles.filter { file =>
      file.getName.startsWith(testFile.getName)
    }.foreach { _.delete() }
  }
}
