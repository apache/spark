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

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.Kryo._
import org.apache.spark.serializer.KryoSerializer

class GraphXPrimitiveKeyOpenHashMapSuite extends SparkFunSuite {
  test("Kryo class register") {
    val conf = new SparkConf(false)
    conf.set(KRYO_REGISTRATION_REQUIRED, true)

    val ser = new KryoSerializer(conf).newInstance()

    val instance1 = new GraphXPrimitiveKeyOpenHashMap[Int, Int]
    instance1.update(1, 1)
    assert(instance1.toMap ==
      ser.deserialize[GraphXPrimitiveKeyOpenHashMap[Int, Int]](ser.serialize(instance1)).toMap)

    val instance2 = new GraphXPrimitiveKeyOpenHashMap[Int, Long]
    instance2.update(2, 2L)
    assert(instance2.toMap ==
      ser.deserialize[GraphXPrimitiveKeyOpenHashMap[Int, Long]](ser.serialize(instance2)).toMap)

    val instance3 = new GraphXPrimitiveKeyOpenHashMap[Int, Double]
    instance3.update(3, 3D)
    assert(instance3.toMap ==
      ser.deserialize[GraphXPrimitiveKeyOpenHashMap[Int, Double]](ser.serialize(instance3)).toMap)

    val instance4 = new GraphXPrimitiveKeyOpenHashMap[Long, Int]
    instance4.update(4L, 4)
    assert(instance4.toMap ==
      ser.deserialize[GraphXPrimitiveKeyOpenHashMap[Long, Int]](ser.serialize(instance4)).toMap)

    val instance5 = new GraphXPrimitiveKeyOpenHashMap[Long, Long]
    instance5.update(5L, 5L)
    assert(instance5.toMap ==
      ser.deserialize[GraphXPrimitiveKeyOpenHashMap[Long, Long]](ser.serialize(instance5)).toMap)

    val instance6 = new GraphXPrimitiveKeyOpenHashMap[Long, Double]
    instance6.update(6L, 6D)
    assert(instance6.toMap ==
      ser.deserialize[GraphXPrimitiveKeyOpenHashMap[Long, Double]](ser.serialize(instance6)).toMap)

    val instance7 = new GraphXPrimitiveKeyOpenHashMap[String, String]
    instance7.update("7", "7S")
    assert(instance7.toMap ==
      ser.deserialize[GraphXPrimitiveKeyOpenHashMap[String, String]](
        ser.serialize(instance7)).toMap)
  }
}
