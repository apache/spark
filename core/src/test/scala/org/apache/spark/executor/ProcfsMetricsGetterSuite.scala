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

import org.mockito.Mockito.{spy, when}

import org.apache.spark.SparkFunSuite


class ProcfsMetricsGetterSuite extends SparkFunSuite {


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
    val mockedP = spy(p)

    var ptree: Set[Int] = Set(26109, 22763)
    when(mockedP.computeProcessTree).thenReturn(ptree)
    var r = mockedP.computeAllMetrics
    assert(r.jvmVmemTotal == 4769947648L)
    assert(r.jvmRSSTotal == 262610944)
    assert(r.pythonVmemTotal == 360595456)
    assert(r.pythonRSSTotal == 7831552)

    // proc file of pid 22764 doesn't exist, so partial metrics shouldn't be returned
    ptree = Set(26109, 22764, 22763)
    when(mockedP.computeProcessTree).thenReturn(ptree)
    r = mockedP.computeAllMetrics
    assert(r.jvmVmemTotal == 0)
    assert(r.jvmRSSTotal == 0)
    assert(r.pythonVmemTotal == 0)
    assert(r.pythonRSSTotal == 0)
  }
}
