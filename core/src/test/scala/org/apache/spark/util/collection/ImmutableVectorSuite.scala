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

package org.apache.spark.util.collection

import scala.util.Random

import org.scalatest.FunSuite

import org.apache.spark.util.SizeEstimator

class ImmutableVectorSuite extends FunSuite {

  test("fromArray") {
    val sizes = for {
      shift <- 0 to 20
      offset <- Array(-1, 0, 1)
    } yield (1 << shift) + offset
    for (size <- sizes) {
      val v = ImmutableVector.fromArray((0 until size).toArray)
      assert(v.size === size)
      for (i <- 0 until size) {
        assert(v(i) == i)
      }
    }
  }

  test("iterator") {
    val sizes = for {
      shift <- 0 to 25
      offset <- Array(-1, 0, 1)
    } yield (1 << shift) + offset
    for (size <- sizes) {
      val v = ImmutableVector.fromArray((0 until size).toArray)
      val iter = v.iterator
      var i = 0
      while (iter.hasNext) {
        assert(iter.next() == i)
        i += 1
      }
      assert(i == size)
    }
  }

  test("updated") {
    val sizes = for {
      shift <- 1 to 15
      offset <- Array(-1, 0, 1)
    } yield (1 << shift) + offset
    for (size <- sizes) {
      var v = ImmutableVector.fromArray((0 until size).toArray)
      for (i <- 0 until size) {
        v = v.updated(i, 0)
        assert(v(i) == 0)
      }
    }
  }
}
