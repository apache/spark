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

package org.apache.spark.graphx

import scala.reflect.ClassTag

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.Kryo._
import org.apache.spark.serializer.KryoSerializer

class EdgeSuite extends SparkFunSuite {
  test("Kryo class register") {
    val conf = new SparkConf(false)
    conf.set(KRYO_REGISTRATION_REQUIRED, true)

    val ser = new KryoSerializer(conf).newInstance()
    def check[T: ClassTag](t: T) {
      assert(ser.deserialize[T](ser.serialize(t)) === t)
    }

    check(Edge(0L, 1L, true))
    check(Edge(0L, 1L, 1.toChar))
    check(Edge(0L, 1L, 1.toByte))
    check(Edge(0L, 1L, 1))
    check(Edge(0L, 1L, 1L))
    check(Edge(0L, 1L, 1F))
    check(Edge(0L, 1L, 1D))
    check(Edge(0L, 1L, "1"))
    check(Edge(0L, 1L, null))
  }

  test ("compare") {
    // descending order
    val testEdges: Array[Edge[Int]] = Array(
      Edge(0x7FEDCBA987654321L, -0x7FEDCBA987654321L, 1),
      Edge(0x2345L, 0x1234L, 1),
      Edge(0x1234L, 0x5678L, 1),
      Edge(0x1234L, 0x2345L, 1),
      Edge(-0x7FEDCBA987654321L, 0x7FEDCBA987654321L, 1)
    )
    // to ascending order
    val sortedEdges = testEdges.sorted(Edge.lexicographicOrdering[Int])

    for (i <- 0 until testEdges.length) {
      assert(sortedEdges(i) == testEdges(testEdges.length - i - 1))
    }
  }
}
