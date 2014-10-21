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

import org.scalatest.FunSuite

import org.apache.spark.SharedSparkContext
import org.apache.spark.scheduler.{SparkListenerTaskEnd, SparkListener}

import scala.collection.mutable.ArrayBuffer

import java.io.{FileWriter, PrintWriter, File}

class InputMetricsSuite extends FunSuite with SharedSparkContext {
  test("input metrics when reading text file") {
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
    assert(taskBytesRead.sum == file.length())
  }
}
