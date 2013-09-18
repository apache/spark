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

package org.apache.spark

import org.scalatest.FunSuite

class BroadcastSuite extends FunSuite with LocalSparkContext {
  
  test("basic broadcast") {
    sc = new SparkContext("local", "test")
    val list = List(1, 2, 3, 4)
    val listBroadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to 2).map(x => (x, listBroadcast.value.sum))
    assert(results.collect.toSet === Set((1, 10), (2, 10)))
  }

  test("broadcast variables accessed in multiple threads") {
    sc = new SparkContext("local[10]", "test")
    val list = List(1, 2, 3, 4)
    val listBroadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to 10).map(x => (x, listBroadcast.value.sum))
    assert(results.collect.toSet === (1 to 10).map(x => (x, 10)).toSet)
  }
}
