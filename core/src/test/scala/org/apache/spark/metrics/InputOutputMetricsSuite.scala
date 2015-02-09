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

import java.io.{FileWriter, PrintWriter, File}

import org.apache.spark.SharedSparkContext
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.scheduler.{SparkListenerTaskEnd, SparkListener}

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

import scala.collection.mutable.ArrayBuffer

class InputOutputMetricsSuite extends FunSuite with SharedSparkContext with ShouldMatchers {
  test("input metrics when reading text file with single split") {
    val file = new File(getClass.getSimpleName + ".txt")
    val pw = new PrintWriter(new FileWriter(file))
    pw.println("some stuff")
    pw.println("some other stuff")
    pw.println("yet more stuff")
    pw.println("too much stuff")
    pw.close()
    file.deleteOnExit()

    val taskBytesRead = new ArrayBuffer[Long]()
    sc.addSparkListener(new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
        taskBytesRead += taskEnd.taskMetrics.inputMetrics.get.bytesRead
      }
    })
    sc.textFile("file://" + file.getAbsolutePath, 2).count()

    // Wait for task end events to come in
    sc.listenerBus.waitUntilEmpty(500)
    assert(taskBytesRead.length == 2)
    assert(taskBytesRead.sum >= file.length())
  }

  test("input metrics when reading text file with multiple splits") {
    val file = new File(getClass.getSimpleName + ".txt")
    val pw = new PrintWriter(new FileWriter(file))
    for (i <- 0 until 10000) {
      pw.println("some stuff")
    }
    pw.close()
    file.deleteOnExit()

    val taskBytesRead = new ArrayBuffer[Long]()
    sc.addSparkListener(new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
        taskBytesRead += taskEnd.taskMetrics.inputMetrics.get.bytesRead
      }
    })
    sc.textFile("file://" + file.getAbsolutePath, 2).count()

    // Wait for task end events to come in
    sc.listenerBus.waitUntilEmpty(500)
    assert(taskBytesRead.length == 2)
    assert(taskBytesRead.sum >= file.length())
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
