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

package org.apache.spark.graphx.impl

import scala.reflect.ClassTag
import scala.util.Random

import org.scalatest.FunSuite

import org.apache.spark.graphx._

class EdgeTripletIteratorSuite extends FunSuite {
  test("iterator.toList") {
    val builder = new EdgePartitionBuilder[Int, Int]
    builder.add(1, 2, 0)
    builder.add(1, 3, 0)
    builder.add(1, 4, 0)
    val iter = new EdgeTripletIterator[Int, Int](builder.toEdgePartition, true, true)
    val result = iter.toList.map(et => (et.srcId, et.dstId))
    assert(result === Seq((1, 2), (1, 3), (1, 4)))
  }
}
