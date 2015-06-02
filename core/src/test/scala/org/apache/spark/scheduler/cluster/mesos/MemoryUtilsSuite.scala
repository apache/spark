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

package org.apache.spark.scheduler.cluster.mesos

import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}

class MemoryUtilsSuite extends SparkFunSuite with MockitoSugar {
  test("MesosMemoryUtils should always override memoryOverhead when it's set") {
    val sparkConf = new SparkConf

    val sc = mock[SparkContext]
    when(sc.conf).thenReturn(sparkConf)

    // 384 > sc.executorMemory * 0.1 => 512 + 384 = 896
    when(sc.executorMemory).thenReturn(512)
    assert(MemoryUtils.calculateTotalMemory(sc) === 896)

    // 384 < sc.executorMemory * 0.1 => 4096 + (4096 * 0.1) = 4505.6
    when(sc.executorMemory).thenReturn(4096)
    assert(MemoryUtils.calculateTotalMemory(sc) === 4505)

    // set memoryOverhead
    sparkConf.set("spark.mesos.executor.memoryOverhead", "100")
    assert(MemoryUtils.calculateTotalMemory(sc) === 4196)
    sparkConf.set("spark.mesos.executor.memoryOverhead", "400")
    assert(MemoryUtils.calculateTotalMemory(sc) === 4496)
  }
}
