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
package org.apache.spark.sql.util

import org.apache.spark.SparkFunSuite

class MapperRowCounterSuite extends SparkFunSuite {

  test("Test MapperRowCounter") {
    val counter = new MapperRowCounter()
    assert(counter.isZero)

    counter.setPartitionId(0)
    counter.add(1L)
    counter.add(1L)
    assert(counter.value.get(0)._1 == 0L)
    assert(counter.value.get(0)._2 == 2L)

    counter.reset()
    assert(counter.isZero)
    counter.setPartitionId(100)
    counter.add(1L)
    assert(counter.value.get(0)._1 == 100L)
    assert(counter.value.get(0)._2 == 1L)

    val counter2 = new MapperRowCounter()
    counter2.setPartitionId(40)
    counter2.add(1L)
    counter2.add(1L)
    counter2.add(1L)

    counter.merge(counter2)
    assert(counter.value.size() == 2)
    assert(counter.value.get(0)._1 == 100L)
    assert(counter.value.get(0)._2 == 1L)
    assert(counter.value.get(1)._1 == 40L)
    assert(counter.value.get(1)._2 == 3L)
  }
}

