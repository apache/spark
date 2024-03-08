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

package org.apache.spark.executor

import org.mockito.Mockito.{mock, spy, when}
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.{interval, timeout}
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.spark.{SparkConf, SparkEnv, SparkFunSuite}
import org.apache.spark.internal.config.EXECUTOR_PROCESS_TREE_METRICS_ENABLED
import org.apache.spark.util.Utils


class ProcfsMetricsGetterSuite extends SparkFunSuite {
  private val sparkHome =
    sys.props.getOrElse("spark.test.home", fail("spark.test.home is not set!"))

  test("testGetProcessInfo") {
    val p = new ProcfsMetricsGetter(getTestResourcePath("ProcfsMetrics"))
    var r = ProcfsMetrics(0, 0, 0, 0, 0, 0)
    r = p.addProcfsMetricsFromOneProcess(r, 26109)
    assert(r.jvmVmemTotal == 4769947648L)
    assert(r.jvmRSSTotal == 262610944)
    assert(r.pythonVmemTotal == 0)
    assert(r.pythonRSSTotal == 0)

    r = p.addProcfsMetricsFromOneProcess(r, 22763)
    assert(r.pythonVmemTotal == 360595456)
    assert(r.pythonRSSTotal == 7831552)
    assert(r.jvmVmemTotal == 4769947648L)
    assert(r.jvmRSSTotal == 262610944)
  }

  test("SPARK-34845: partial metrics shouldn't be returned") {
    val p = new ProcfsMetricsGetter(getTestResourcePath("ProcfsMetrics"))
    val mockedP = spy[ProcfsMetricsGetter](p)

    var ptree: Set[Long] = Set(26109, 22763)
    when(mockedP.computeProcessTree()).thenReturn(ptree)
    var r = mockedP.computeAllMetrics()
    assert(r.jvmVmemTotal == 4769947648L)
    assert(r.jvmRSSTotal == 262610944)
    assert(r.pythonVmemTotal == 360595456)
    assert(r.pythonRSSTotal == 7831552)

    // proc file of pid 22764 doesn't exist, so partial metrics shouldn't be returned
    ptree = Set(26109, 22764, 22763)
    when(mockedP.computeProcessTree()).thenReturn(ptree)
    r = mockedP.computeAllMetrics()
    assert(r.jvmVmemTotal == 0)
    assert(r.jvmRSSTotal == 0)
    assert(r.pythonVmemTotal == 0)
    assert(r.pythonRSSTotal == 0)
  }

  test("SPARK-45907: Use ProcessHandle APIs to computeProcessTree in ProcfsMetricsGetter") {
    val originalSparkEnv = SparkEnv.get
    val sparkEnv = mock(classOf[SparkEnv])
    val conf = new SparkConf(false)
      .set(EXECUTOR_PROCESS_TREE_METRICS_ENABLED, true)
    when(sparkEnv.conf).thenReturn(conf)
    try {
      SparkEnv.set(sparkEnv)
      val p = new ProcfsMetricsGetter()
      val currentPid = ProcessHandle.current().pid()
      val process = Utils.executeCommand(Seq(
        s"$sparkHome/bin/spark-class",
        this.getClass.getCanonicalName.stripSuffix("$"),
        currentPid.toString))
      val child = process.toHandle.pid()
      eventually(timeout(10.seconds), interval(100.milliseconds)) {
        val pids = p.computeProcessTree()
        assert(pids.size === 3)
        assert(pids.contains(currentPid))
        assert(pids.contains(child))
      }
    } finally {
      SparkEnv.set(originalSparkEnv)
    }
  }
}

object ProcfsMetricsGetterSuite {
  def main(args: Array[String]): Unit = {
    Utils.executeCommand(Seq("jstat", "-gcutil", args(0), "50", "100"))
  }
}
