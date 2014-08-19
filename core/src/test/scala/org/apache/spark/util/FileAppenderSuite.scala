/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util

import java.io._
import java.nio.charset.Charset

import scala.collection.mutable.HashSet
import scala.reflect._

import org.scalatest.{BeforeAndAfter, FunSuite}

import com.google.common.io.Files

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.util.logging.{RollingFileAppender, SizeBasedRollingPolicy, TimeBasedRollingPolicy, FileAppender}

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
    val inputStream = new ByteArrayInputStream(testString.getBytes(Charset.forName("UTF-8")))
    val appender = new FileAppender(inputStream, testFile)
    inputStream.close()
    appender.awaitTermination()
    assert(Files.toString(testFile, Charset.forName("UTF-8")) === testString)
  }

  test("rolling file appender - time-based rolling") {
    // setup input stream and appender
    val testOutputStream = new PipedOutputStream()
    val testInputStream = new PipedInputStream(testOutputStream, 100 * 1000)
    val rolloverIntervalMillis = 100
    val durationMillis = 1000
    val numRollovers = durationMillis / rolloverIntervalMillis
    val textToAppend = (1 to numRollovers).map( _.toString * 10 )

    val appender = new RollingFileAppender(testInputStream, testFile,
      new TimeBasedRollingPolicy(rolloverIntervalMillis, s"--HH-mm-ss-SSSS", false),
      new SparkConf(), 10)

    testRolling(appender, testOutputStream, textToAppend, rolloverIntervalMillis)
  }

  test("rolling file appender - size-based rolling") {
    // setup input stream and appender
    val testOutputStream = new PipedOutputStream()
    val testInputStream = new PipedInputStream(testOutputStream, 100 * 1000)
    val rolloverSize = 1000
    val textToAppend = (1 to 3).map( _.toString * 1000 )

    val appender = new RollingFileAppender(testInputStream, testFile,
      new SizeBasedRollingPolicy(rolloverSize, false), new SparkConf(), 99)

    val files = testRolling(appender, testOutputStream, textToAppend, 0)
    files.foreach { file =>
      logInfo(file.toString + ": " + file.length + " bytes")
      assert(file.length <= rolloverSize)
    }
  }

  test("rolling file appender - cleaning") {
    // setup input stream and appender
    val testOutputStream = new PipedOutputStream()
    val testInputStream = new PipedInputStream(testOutputStream, 100 * 1000)
    val conf = new SparkConf().set(RollingFileAppender.RETAINED_FILES_PROPERTY, "10")
    val appender = new RollingFileAppender(testInputStream, testFile,
      new SizeBasedRollingPolicy(1000, false), conf, 10)

    // send data to appender through the input stream, and wait for the data to be written
    val allGeneratedFiles = new HashSet[String]()
    val items = (1 to 10).map { _.toString * 10000 }
    for (i <- 0 until items.size) {
      testOutputStream.write(items(i).getBytes(Charset.forName("UTF-8")))
      testOutputStream.flush()
      allGeneratedFiles ++= RollingFileAppender.getSortedRolledOverFiles(
        testFile.getParentFile.toString, testFile.getName).map(_.toString)

      Thread.sleep(10)
    }
    testOutputStream.close()
    appender.awaitTermination()
    logInfo("Appender closed")

    // verify whether the earliest file has been deleted
    val rolledOverFiles = allGeneratedFiles.filter { _ != testFile.toString }.toArray.sorted
    logInfo(s"All rolled over files generated:${rolledOverFiles.size}\n" + rolledOverFiles.mkString("\n"))
    assert(rolledOverFiles.size > 2)
    val earliestRolledOverFile = rolledOverFiles.head
    val existingRolledOverFiles = RollingFileAppender.getSortedRolledOverFiles(
      testFile.getParentFile.toString, testFile.getName).map(_.toString)
    logInfo("Existing rolled over files:\n" + existingRolledOverFiles.mkString("\n"))
    assert(!existingRolledOverFiles.toSet.contains(earliestRolledOverFile))
  }

  test("file appender selection") {
    // Test whether FileAppender.apply() returns the right type of the FileAppender based
    // on SparkConf settings.

    def testAppenderSelection[ExpectedAppender: ClassTag, ExpectedRollingPolicy](
        properties: Seq[(String, String)], expectedRollingPolicyParam: Long = -1): Unit = {

      // Set spark conf properties
      val conf = new SparkConf
      properties.foreach { p =>
        conf.set(p._1, p._2)
      }

      // Create and test file appender
      val testOutputStream = new PipedOutputStream()
      val testInputStream = new PipedInputStream(testOutputStream)
      val appender = FileAppender(testInputStream, testFile, conf)
      //assert(appender.getClass === classTag[ExpectedAppender].getClass)
      assert(appender.getClass.getSimpleName ===
        classTag[ExpectedAppender].runtimeClass.getSimpleName)
      if (appender.isInstanceOf[RollingFileAppender]) {
        val rollingPolicy = appender.asInstanceOf[RollingFileAppender].rollingPolicy
        val policyParam = if (rollingPolicy.isInstanceOf[TimeBasedRollingPolicy]) {
          rollingPolicy.asInstanceOf[TimeBasedRollingPolicy].rolloverIntervalMillis
        } else {
          rollingPolicy.asInstanceOf[SizeBasedRollingPolicy].rolloverSizeBytes
        }
        assert(policyParam === expectedRollingPolicyParam)
      }
      testOutputStream.close()
      appender.awaitTermination()
    }

    import RollingFileAppender._

    def rollingStrategy(strategy: String) = Seq(STRATEGY_PROPERTY -> strategy)
    def rollingSize(size: String) = Seq(SIZE_PROPERTY -> size)
    def rollingInterval(interval: String) = Seq(INTERVAL_PROPERTY -> interval)

    val msInDay = 24 * 60 * 60 * 1000L
    val msInHour = 60 * 60 * 1000L
    val msInMinute = 60 * 1000L

    // test no strategy -> no rolling
    testAppenderSelection[FileAppender, Any](Seq.empty)

    // test time based rolling strategy
    testAppenderSelection[RollingFileAppender, Any](rollingStrategy("time"), msInDay)
    testAppenderSelection[RollingFileAppender, TimeBasedRollingPolicy](
      rollingStrategy("time") ++ rollingInterval("daily"), msInDay)
    testAppenderSelection[RollingFileAppender, TimeBasedRollingPolicy](
      rollingStrategy("time") ++ rollingInterval("hourly"), msInHour)
    testAppenderSelection[RollingFileAppender, TimeBasedRollingPolicy](
      rollingStrategy("time") ++ rollingInterval("minutely"), msInMinute)
    testAppenderSelection[RollingFileAppender, TimeBasedRollingPolicy](
      rollingStrategy("time") ++ rollingInterval("123456789"), 123456789 * 1000L)
    testAppenderSelection[FileAppender, Any](
      rollingStrategy("time") ++ rollingInterval("xyz"))

    // test size based rolling strategy
    testAppenderSelection[RollingFileAppender, SizeBasedRollingPolicy](
      rollingStrategy("size") ++ rollingSize("123456789"), 123456789)
    testAppenderSelection[FileAppender, Any](rollingSize("xyz"))

    // test illegal strategy
    testAppenderSelection[FileAppender, Any](rollingStrategy("xyz"))
  }

  /**
   * Run the rolling file appender with data and see whether all the data was written correctly
   * across rolled over files.
   */
  def testRolling(
      appender: FileAppender,
      outputStream: OutputStream,
      textToAppend: Seq[String],
      sleepTimeBetweenTexts: Long
    ): Seq[File] = {
    // send data to appender through the input stream, and wait for the data to be written
    val expectedText = textToAppend.mkString("")
    for (i <- 0 until textToAppend.size) {
      outputStream.write(textToAppend(i).getBytes(Charset.forName("UTF-8")))
      outputStream.flush()
      Thread.sleep(sleepTimeBetweenTexts)
    }
    logInfo("Data sent to appender")
    outputStream.close()
    appender.awaitTermination()
    logInfo("Appender closed")

    // verify whether all the data written to rolled over files is same as expected
    val generatedFiles = RollingFileAppender.getSortedRolledOverFiles(
      testFile.getParentFile.toString, testFile.getName)
    logInfo("Filtered files: \n" + generatedFiles.mkString("\n"))
    assert(generatedFiles.size > 1)
    val allText = generatedFiles.map { file =>
      Files.toString(file, Charset.forName("UTF-8"))
    }.mkString("")
    assert(allText === expectedText)
    generatedFiles
  }

  /** Delete all the generated rolledover files */
  def cleanup() {
    testFile.getParentFile.listFiles.filter { file =>
      file.getName.startsWith(testFile.getName)
    }.foreach { _.delete() }
  }
}
