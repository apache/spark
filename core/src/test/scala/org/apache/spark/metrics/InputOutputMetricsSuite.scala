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

package org.apache.spark.metrics

import java.io.{File, PrintWriter}
import java.util.concurrent.ThreadLocalRandom

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileSplit => OldFileSplit, InputSplit => OldInputSplit, JobConf, LineRecordReader => OldLineRecordReader, RecordReader => OldRecordReader, Reporter, TextInputFormat => OldTextInputFormat}
import org.apache.hadoop.mapred.lib.{CombineFileInputFormat => OldCombineFileInputFormat, CombineFileRecordReader => OldCombineFileRecordReader, CombineFileSplit => OldCombineFileSplit}
import org.apache.hadoop.mapreduce.{InputSplit => NewInputSplit, RecordReader => NewRecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.{CombineFileInputFormat => NewCombineFileInputFormat, CombineFileRecordReader => NewCombineFileRecordReader, CombineFileSplit => NewCombineFileSplit, FileSplit => NewFileSplit, TextInputFormat => NewTextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{TextOutputFormat => NewTextOutputFormat}
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SharedSparkContext, SparkFunSuite}
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.util.{ThreadUtils, Utils}

class InputOutputMetricsSuite extends SparkFunSuite with SharedSparkContext
  with BeforeAndAfter {

  @transient var tmpDir: File = _
  @transient var tmpFile: File = _
  @transient var tmpFilePath: String = _
  @transient val numRecords: Int = 100000
  @transient val numBuckets: Int = 10

  before {
    tmpDir = Utils.createTempDir()
    val testTempDir = new File(tmpDir, "test")
    testTempDir.mkdir()

    tmpFile = new File(testTempDir, getClass.getSimpleName + ".txt")
    Utils.tryWithResource(new PrintWriter(tmpFile)) { pw =>
      for (x <- 1 to numRecords) {
        // scalastyle:off println
        pw.println(ThreadLocalRandom.current().nextInt(0, numBuckets))
        // scalastyle:on println
      }
    }

    // Path to tmpFile
    tmpFilePath = tmpFile.toURI.toString
  }

  after {
    Utils.deleteRecursively(tmpDir)
  }

  test("input metrics for old hadoop with coalesce") {
    val bytesRead = runAndReturnBytesRead {
      sc.textFile(tmpFilePath, 4).count()
    }
    val bytesRead2 = runAndReturnBytesRead {
      sc.textFile(tmpFilePath, 4).coalesce(2).count()
    }
    assert(bytesRead != 0)
    assert(bytesRead == bytesRead2)
    assert(bytesRead2 >= tmpFile.length())
  }

  test("input metrics with cache and coalesce") {
    // prime the cache manager
    val rdd = sc.textFile(tmpFilePath, 4).cache()
    rdd.collect()

    val bytesRead = runAndReturnBytesRead {
      rdd.count()
    }
    val bytesRead2 = runAndReturnBytesRead {
      rdd.coalesce(4).count()
    }

    // for count and coalesce, the same bytes should be read.
    assert(bytesRead != 0)
    assert(bytesRead2 == bytesRead)
  }

  test("input metrics for new Hadoop API with coalesce") {
    val bytesRead = runAndReturnBytesRead {
      sc.newAPIHadoopFile(tmpFilePath, classOf[NewTextInputFormat], classOf[LongWritable],
        classOf[Text]).count()
    }
    val bytesRead2 = runAndReturnBytesRead {
      sc.newAPIHadoopFile(tmpFilePath, classOf[NewTextInputFormat], classOf[LongWritable],
        classOf[Text]).coalesce(5).count()
    }
    assert(bytesRead != 0)
    assert(bytesRead2 == bytesRead)
    assert(bytesRead >= tmpFile.length())
  }

  test("input metrics when reading text file") {
    val bytesRead = runAndReturnBytesRead {
      sc.textFile(tmpFilePath, 2).count()
    }
    assert(bytesRead >= tmpFile.length())
  }

  test("input metrics on records read - simple") {
    val records = runAndReturnRecordsRead {
      sc.textFile(tmpFilePath, 4).count()
    }
    assert(records == numRecords)
  }

  test("input metrics on records read - more stages") {
    val records = runAndReturnRecordsRead {
      sc.textFile(tmpFilePath, 4)
        .map(key => (key.length, 1))
        .reduceByKey(_ + _)
        .count()
    }
    assert(records == numRecords)
  }

  test("input metrics on records - New Hadoop API") {
    val records = runAndReturnRecordsRead {
      sc.newAPIHadoopFile(tmpFilePath, classOf[NewTextInputFormat], classOf[LongWritable],
        classOf[Text]).count()
    }
    assert(records == numRecords)
  }

  test("input metrics on records read with cache") {
    // prime the cache manager
    val rdd = sc.textFile(tmpFilePath, 4).cache()
    rdd.collect()

    val records = runAndReturnRecordsRead {
      rdd.count()
    }

    assert(records == numRecords)
  }

  /**
   * Tests the metrics from end to end.
   * 1) reading a hadoop file
   * 2) shuffle and writing to a hadoop file.
   * 3) writing to hadoop file.
   */
  test("input read/write and shuffle read/write metrics all line up") {
    var inputRead = 0L
    var outputWritten = 0L
    var shuffleRead = 0L
    var shuffleWritten = 0L
    sc.addSparkListener(new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        val metrics = taskEnd.taskMetrics
        inputRead += metrics.inputMetrics.recordsRead
        outputWritten += metrics.outputMetrics.recordsWritten
        shuffleRead += metrics.shuffleReadMetrics.recordsRead
        shuffleWritten += metrics.shuffleWriteMetrics.recordsWritten
      }
    })

    val tmpFile = new File(tmpDir, getClass.getSimpleName)

    sc.textFile(tmpFilePath, 4)
      .map(key => (key, 1))
      .reduceByKey(_ + _)
      .saveAsTextFile(tmpFile.toURI.toString)

    sc.listenerBus.waitUntilEmpty()
    assert(inputRead == numRecords)

    assert(outputWritten == numBuckets)
    assert(shuffleRead == shuffleWritten)
  }

  test("input metrics with interleaved reads") {
    val numPartitions = 2
    val cartVector = 0 to 9
    val cartFile = new File(tmpDir, getClass.getSimpleName + "_cart.txt")
    val cartFilePath = cartFile.toURI.toString

    // write files to disk so we can read them later.
    sc.parallelize(cartVector).saveAsTextFile(cartFilePath)
    val aRdd = sc.textFile(cartFilePath, numPartitions)

    val tmpRdd = sc.textFile(tmpFilePath, numPartitions)

    val firstSize = runAndReturnBytesRead {
      aRdd.count()
    }
    val secondSize = runAndReturnBytesRead {
      tmpRdd.count()
    }

    val cartesianBytes = runAndReturnBytesRead {
      aRdd.cartesian(tmpRdd).count()
    }

    // Computing the amount of bytes read for a cartesian operation is a little involved.
    // Cartesian interleaves reads between two partitions e.g. p1 and p2.
    // Here are the steps:
    //  1) First it creates an iterator for p1
    //  2) Creates an iterator for p2
    //  3) Reads the first element of p1 and then all the elements of p2
    //  4) proceeds to the next element of p1
    //  5) Creates a new iterator for p2
    //  6) rinse and repeat.
    // As a result we read from the second partition n times where n is the number of keys in
    // p1. Thus the math below for the test.
    assert(cartesianBytes != 0)
    assert(cartesianBytes == firstSize * numPartitions + (cartVector.length  * secondSize))
  }

  private def runAndReturnBytesRead(job: => Unit): Long = {
    runAndReturnMetrics(job, _.taskMetrics.inputMetrics.bytesRead)
  }

  private def runAndReturnRecordsRead(job: => Unit): Long = {
    runAndReturnMetrics(job, _.taskMetrics.inputMetrics.recordsRead)
  }

  private def runAndReturnRecordsWritten(job: => Unit): Long = {
    runAndReturnMetrics(job, _.taskMetrics.outputMetrics.recordsWritten)
  }

  private def runAndReturnMetrics(job: => Unit, collector: (SparkListenerTaskEnd) => Long): Long = {
    val taskMetrics = new ArrayBuffer[Long]()

    // Avoid receiving earlier taskEnd events
    sc.listenerBus.waitUntilEmpty()

    sc.addSparkListener(new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        taskMetrics += collector(taskEnd)
      }
    })

    job

    sc.listenerBus.waitUntilEmpty()
    taskMetrics.sum
  }

  test("output metrics on records written") {
    val file = new File(tmpDir, getClass.getSimpleName)
    val filePath = file.toURI.toURL.toString

    val records = runAndReturnRecordsWritten {
      sc.parallelize(1 to numRecords).saveAsTextFile(filePath)
    }
    assert(records == numRecords)
  }

  test("output metrics on records written - new Hadoop API") {
    val file = new File(tmpDir, getClass.getSimpleName)
    val filePath = file.toURI.toURL.toString

    val records = runAndReturnRecordsWritten {
      sc.parallelize(1 to numRecords).map(key => (key.toString, key.toString))
        .saveAsNewAPIHadoopFile[NewTextOutputFormat[String, String]](filePath)
    }
    assert(records == numRecords)
  }

  test("output metrics when writing text file") {
    val fs = FileSystem.getLocal(new Configuration())
    val outPath = new Path(fs.getWorkingDirectory, "outdir")

    val taskBytesWritten = new ArrayBuffer[Long]()
    sc.addSparkListener(new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        taskBytesWritten += taskEnd.taskMetrics.outputMetrics.bytesWritten
      }
    })

    val rdd = sc.parallelize(Seq("a", "b", "c", "d"), 2)

    try {
      rdd.saveAsTextFile(outPath.toString)
      sc.listenerBus.waitUntilEmpty()
      assert(taskBytesWritten.length == 2)
      val outFiles = fs.listStatus(outPath).filter(_.getPath.getName != "_SUCCESS")
      taskBytesWritten.zip(outFiles).foreach { case (bytes, fileStatus) =>
        assert(bytes >= fileStatus.getLen)
      }
    } finally {
      fs.delete(outPath, true)
    }
  }

  test("input metrics with old CombineFileInputFormat") {
    val bytesRead = runAndReturnBytesRead {
      sc.hadoopFile(tmpFilePath, classOf[OldCombineTextInputFormat], classOf[LongWritable],
        classOf[Text], 2).count()
    }
    assert(bytesRead >= tmpFile.length())
  }

  test("input metrics with new CombineFileInputFormat") {
    val bytesRead = runAndReturnBytesRead {
      sc.newAPIHadoopFile(tmpFilePath, classOf[NewCombineTextInputFormat], classOf[LongWritable],
        classOf[Text], new Configuration()).count()
    }
    assert(bytesRead >= tmpFile.length())
  }

  test("input metrics with old Hadoop API in different thread") {
    val bytesRead = runAndReturnBytesRead {
      sc.textFile(tmpFilePath, 4).mapPartitions { iter =>
        val buf = new ArrayBuffer[String]()
        ThreadUtils.runInNewThread("testThread", false) {
          iter.flatMap(_.split(" ")).foreach(buf.append(_))
        }

        buf.iterator
      }.count()
    }
    assert(bytesRead >= tmpFile.length())
  }

  test("input metrics with new Hadoop API in different thread") {
    val bytesRead = runAndReturnBytesRead {
      sc.newAPIHadoopFile(tmpFilePath, classOf[NewTextInputFormat], classOf[LongWritable],
        classOf[Text]).mapPartitions { iter =>
        val buf = new ArrayBuffer[String]()
        ThreadUtils.runInNewThread("testThread", false) {
          iter.map(_._2.toString).flatMap(_.split(" ")).foreach(buf.append(_))
        }

        buf.iterator
      }.count()
    }
    assert(bytesRead >= tmpFile.length())
  }
}

/**
 * Hadoop 2 has a version of this, but we can't use it for backwards compatibility
 */
class OldCombineTextInputFormat extends OldCombineFileInputFormat[LongWritable, Text] {
  override def getRecordReader(split: OldInputSplit, conf: JobConf, reporter: Reporter)
  : OldRecordReader[LongWritable, Text] = {
    new OldCombineFileRecordReader[LongWritable, Text](conf,
      split.asInstanceOf[OldCombineFileSplit], reporter, classOf[OldCombineTextRecordReaderWrapper]
        .asInstanceOf[Class[OldRecordReader[LongWritable, Text]]])
  }
}

class OldCombineTextRecordReaderWrapper(
    split: OldCombineFileSplit,
    conf: Configuration,
    reporter: Reporter,
    idx: Integer) extends OldRecordReader[LongWritable, Text] {

  val fileSplit = new OldFileSplit(split.getPath(idx),
    split.getOffset(idx),
    split.getLength(idx),
    split.getLocations())

  val delegate: OldLineRecordReader = new OldTextInputFormat().getRecordReader(fileSplit,
    conf.asInstanceOf[JobConf], reporter).asInstanceOf[OldLineRecordReader]

  override def next(key: LongWritable, value: Text): Boolean = delegate.next(key, value)
  override def createKey(): LongWritable = delegate.createKey()
  override def createValue(): Text = delegate.createValue()
  override def getPos(): Long = delegate.getPos
  override def close(): Unit = delegate.close()
  override def getProgress(): Float = delegate.getProgress
}

/**
 * Hadoop 2 has a version of this, but we can't use it for backwards compatibility
 */
class NewCombineTextInputFormat extends NewCombineFileInputFormat[LongWritable, Text] {
  def createRecordReader(split: NewInputSplit, context: TaskAttemptContext)
  : NewRecordReader[LongWritable, Text] = {
    new NewCombineFileRecordReader[LongWritable, Text](split.asInstanceOf[NewCombineFileSplit],
      context, classOf[NewCombineTextRecordReaderWrapper])
  }
}

class NewCombineTextRecordReaderWrapper(
    split: NewCombineFileSplit,
    context: TaskAttemptContext,
    idx: Integer) extends NewRecordReader[LongWritable, Text] {

  val fileSplit = new NewFileSplit(split.getPath(idx),
    split.getOffset(idx),
    split.getLength(idx),
    split.getLocations())

  val delegate = new NewTextInputFormat().createRecordReader(fileSplit, context)

  override def initialize(split: NewInputSplit, context: TaskAttemptContext): Unit = {
    delegate.initialize(fileSplit, context)
  }

  override def nextKeyValue(): Boolean = delegate.nextKeyValue()
  override def getCurrentKey(): LongWritable = delegate.getCurrentKey
  override def getCurrentValue(): Text = delegate.getCurrentValue
  override def getProgress(): Float = delegate.getProgress
  override def close(): Unit = delegate.close()
}
