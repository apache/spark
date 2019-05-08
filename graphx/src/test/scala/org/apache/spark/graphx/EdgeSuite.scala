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

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.Kryo._
import org.apache.spark.serializer.KryoSerializer

class EdgeSuite extends SparkFunSuite {
  test("Kryo class register") {
    val conf = new SparkConf(false)
    conf.set(KRYO_REGISTRATION_REQUIRED, true)

    val ser = new KryoSerializer(conf).newInstance()

    val edge1 = Edge(0L, 1L, true)
    assert(edge1 == ser.deserialize[Edge[Boolean]](ser.serialize(edge1)))

    val edge2 = Edge(0L, 1L, 1.toChar)
    assert(edge2 == ser.deserialize[Edge[Char]](ser.serialize(edge2)))

    val edge3 = Edge(0L, 1L, 1.toByte)
    assert(edge3 == ser.deserialize[Edge[Byte]](ser.serialize(edge3)))

    val edge4 = Edge(0L, 1L, 1)
    assert(edge4 == ser.deserialize[Edge[Int]](ser.serialize(edge4)))

    val edge5 = Edge(0L, 1L, 1L)
    assert(edge5 == ser.deserialize[Edge[Long]](ser.serialize(edge5)))

    val edge6 = Edge(0L, 1L, 1F)
    assert(edge6 == ser.deserialize[Edge[Float]](ser.serialize(edge6)))

    val edge7 = Edge(0L, 1L, 1D)
    assert(edge7 == ser.deserialize[Edge[Double]](ser.serialize(edge7)))

    val edge8 = Edge(0L, 1L, "1")
    assert(edge8 == ser.deserialize[Edge[String]](ser.serialize(edge8)))

    val edge9 = Edge(0L, 1L, null)
    assert(edge9 == ser.deserialize[Edge[Null]](ser.serialize(edge9)))
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
