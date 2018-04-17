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
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}

class ArrayBasedMapDataSuite extends SparkFunSuite {
  test("compare simple maps") {
    val map1 = new ArrayBasedMapData(
      keyArray = new GenericArrayData(Array[String]("a")),
      valueArray = new GenericArrayData(Array[Int](1)))
    val map2 = new ArrayBasedMapData(
      keyArray = new GenericArrayData(Array[String]("a")),
      valueArray = new GenericArrayData(Array[Int](1)))

    assert(map1 == map2)
    assert(map1.hashCode() == map2.hashCode())
  }

  test("compare not equal simple maps - values different") {
    val map1 = new ArrayBasedMapData(
      keyArray = new GenericArrayData(Array[String]("a")),
      valueArray = new GenericArrayData(Array[Int](1)))
    val map2 = new ArrayBasedMapData(
      keyArray = new GenericArrayData(Array[String]("a")),
      valueArray = new GenericArrayData(Array[Int](2)))

    assert(map1 != map2)
    assert(map1.hashCode() != map2.hashCode())
  }

  test("compare not equal simple maps - keys different") {
    val map1 = new ArrayBasedMapData(
      keyArray = new GenericArrayData(Array[String]("a")),
      valueArray = new GenericArrayData(Array[Int](1)))
    val map2 = new ArrayBasedMapData(
      keyArray = new GenericArrayData(Array[String]("b")),
      valueArray = new GenericArrayData(Array[Int](1)))

    assert(map1 != map2)
    assert(map1.hashCode() != map2.hashCode())
  }

  test("compare maps of maps") {
    val baseMapA = new ArrayBasedMapData(
      keyArray = new GenericArrayData(Array[String]("A")),
      valueArray = new GenericArrayData(Array[Int](1)))
    val baseMapB = new ArrayBasedMapData(
      keyArray = new GenericArrayData(Array[String]("B")),
      valueArray = new GenericArrayData(Array[Int](1)))

    val map1 = new ArrayBasedMapData(
      keyArray = new GenericArrayData(Array(baseMapA, baseMapB)),
      valueArray = new GenericArrayData(Array[Int](1, 2)))
    val map2 = new ArrayBasedMapData(
      keyArray = new GenericArrayData(Array(baseMapA, baseMapB)),
      valueArray = new GenericArrayData(Array[Int](1, 2)))

    assert(map1 == map2)
    assert(map1.hashCode() == map2.hashCode())

    val map3 = new ArrayBasedMapData(
      keyArray = new GenericArrayData(Array(baseMapA, baseMapB)),
      valueArray = new GenericArrayData(Array[Int](1, 0)))

    assert(map1 != map3)
    assert(map1.hashCode() != map3.hashCode())
  }
}