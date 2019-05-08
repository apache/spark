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

package org.apache.spark.graphx.util.collection

import scala.reflect.ClassTag

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.Kryo._
import org.apache.spark.serializer.KryoSerializer

class GraphXPrimitiveKeyOpenHashMapSuite extends SparkFunSuite {
  test("Kryo class register") {
    val conf = new SparkConf(false)
    conf.set(KRYO_REGISTRATION_REQUIRED, true)

    val ser = new KryoSerializer(conf).newInstance()
    def check[K: ClassTag, V: ClassTag](t: GraphXPrimitiveKeyOpenHashMap[K, V]) {
      assert(t.toMap ===
        ser.deserialize[GraphXPrimitiveKeyOpenHashMap[K, V]](ser.serialize(t)).toMap)
    }

    val map1 = new GraphXPrimitiveKeyOpenHashMap[Int, Int]
    map1.update(1, 1)
    check(map1)

    val map2 = new GraphXPrimitiveKeyOpenHashMap[Int, Long]
    map2.update(2, 2L)
    check(map2)

    val map3 = new GraphXPrimitiveKeyOpenHashMap[Int, Double]
    map3.update(3, 3D)
    check(map3)

    val map4 = new GraphXPrimitiveKeyOpenHashMap[Long, Int]
    map4.update(4L, 4)
    check(map4)

    val map5 = new GraphXPrimitiveKeyOpenHashMap[Long, Long]
    map5.update(5L, 5L)
    check(map5)

    val map6 = new GraphXPrimitiveKeyOpenHashMap[Long, Double]
    map6.update(6L, 6D)
    check(map6)

    val map7 = new GraphXPrimitiveKeyOpenHashMap[String, String]
    map7.update("7", "7S")
    check(map7)
  }
}
