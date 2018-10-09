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

import org.apache.spark.SparkFunSuite


class ProcfsBasedSystemsSuite extends SparkFunSuite {

  val p = new ProcfsBasedSystems(getTestResourcePath("ProcessTree"))
  p.pageSize = 4096L

  test("testGetProcessInfo") {
    p.computeProcessInfo(26109)
    assert(p.allMetrics.jvmVmemTotal == 4769947648L)
    assert(p.allMetrics.jvmRSSTotal == 262610944)
    assert(p.allMetrics.pythonVmemTotal == 0)
    assert(p.allMetrics.pythonRSSTotal == 0)

    p.computeProcessInfo(22763)
    assert(p.allMetrics.pythonVmemTotal == 360595456)
    assert(p.allMetrics.pythonRSSTotal == 7831552)
    assert(p.allMetrics.jvmVmemTotal == 4769947648L)
    assert(p.allMetrics.jvmRSSTotal == 262610944)
  }
}
