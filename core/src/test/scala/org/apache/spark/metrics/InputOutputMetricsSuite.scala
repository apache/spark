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

import java.io.{File, FileWriter, PrintWriter}

import scala.collection.mutable.ArrayBuffer

import org.scalatest.FunSuite

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{TextInputFormat => NewTextInputFormat}

import org.apache.spark.SharedSparkContext
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.util.Utils

class InputOutputMetricsSuite extends FunSuite with SharedSparkContext {

  @transient var tmpDir: File = _
  @transient var tmpFile: File = _
  @transient var tmpFilePath: String = _

  override def beforeAll() {
    super.beforeAll()

    tmpDir = Utils.createTempDir()
    val testTempDir = new File(tmpDir, "test")
    testTempDir.mkdir()

    tmpFile = new File(testTempDir, getClass.getSimpleName + ".txt")
    val pw = new PrintWriter(new FileWriter(tmpFile))
    for (x <- 1 to 1000000) {
      pw.println("s")
    }
    pw.close()

    // Path to tmpFile
    tmpFilePath = "file://" + tmpFile.getAbsolutePath
  }

  override def afterAll() {
    super.afterAll()
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

    // for count and coelesce, the same bytes should be read.
    assert(bytesRead != 0)
    assert(bytesRead2 == bytesRead)
  }

  /**
   * This checks the situation where we have interleaved reads from
   * different sources. Currently, we only accumulate fron the first
   * read method we find in the task. This test uses cartesian to create
   * the interleaved reads.
   *
   * Once https://issues.apache.org/jira/browse/SPARK-5225 is fixed
   * this test should break.
   */
  test("input metrics with mixed read method") {
    // prime the cache manager
    val numPartitions = 2
    val rdd = sc.parallelize(1 to 100, numPartitions).cache()
    rdd.collect()

    val rdd2 = sc.textFile(tmpFilePath, numPartitions)

    val bytesRead = runAndReturnBytesRead {
      rdd.count()
    }
    val bytesRead2 = runAndReturnBytesRead {
      rdd2.count()
    }

    val cartRead = runAndReturnBytesRead {
      rdd.cartesian(rdd2).count()
    }

    assert(cartRead != 0)
    assert(bytesRead != 0)
    // We read from the first rdd of the cartesian once per partition.
    assert(cartRead == bytesRead * numPartitions)
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

  test("input metrics with interleaved reads") {
    val numPartitions = 2
    val cartVector = 0 to 9
    val cartFile = new File(tmpDir, getClass.getSimpleName + "_cart.txt")
    val cartFilePath = "file://" + cartFile.getAbsolutePath

    // write files to disk so we can read them later.
    sc.parallelize(cartVector).saveAsTextFile(cartFilePath)
    val aRdd = sc.textFile(cartFilePath, numPartitions)

    val tmpRdd = sc.textFile(tmpFilePath, numPartitions)

    val firstSize= runAndReturnBytesRead {
      aRdd.count()
    }
    val secondSize = runAndReturnBytesRead {
      tmpRdd.count()
    }

    val cartesianBytes = runAndReturnBytesRead {
      aRdd.cartesian(tmpRdd).count()
    }

    // Computing the amount of bytes read for a cartesian operation is a little involved.
    // Cartesian interleaves reads between two partitions eg. p1 and p2.
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

  private def runAndReturnBytesRead(job : => Unit): Long = {
    val taskBytesRead = new ArrayBuffer[Long]()
    sc.addSparkListener(new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
        taskBytesRead += taskEnd.taskMetrics.inputMetrics.get.bytesRead
      }
    })

    job

    sc.listenerBus.waitUntilEmpty(500)
    taskBytesRead.sum
  }

  test("output metrics when writing text file") {
    val fs = FileSystem.getLocal(new Configuration())
    val outPath = new Path(fs.getWorkingDirectory, "outdir")

    if (SparkHadoopUtil.get.getFSBytesWrittenOnThreadCallback(outPath, fs.getConf).isDefined) {
      val taskBytesWritten = new ArrayBuffer[Long]()
      sc.addSparkListener(new SparkListener() {
        override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
          taskBytesWritten += taskEnd.taskMetrics.outputMetrics.get.bytesWritten
        }
      })

      val rdd = sc.parallelize(Array("a", "b", "c", "d"), 2)

      try {
        rdd.saveAsTextFile(outPath.toString)
        sc.listenerBus.waitUntilEmpty(500)
        assert(taskBytesWritten.length == 2)
        val outFiles = fs.listStatus(outPath).filter(_.getPath.getName != "_SUCCESS")
        taskBytesWritten.zip(outFiles).foreach { case (bytes, fileStatus) =>
          assert(bytes >= fileStatus.getLen)
        }
      } finally {
        fs.delete(outPath, true)
      }
    }
  }
}
