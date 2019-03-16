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

package org.apache.spark.ml.feature

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.Kryo._
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.serializer.KryoSerializer

class InstanceSuite extends SparkFunSuite{
  test("Kryo class register") {
    val conf = new SparkConf(false)
    conf.set(KRYO_REGISTRATION_REQUIRED, true)

    val ser = new KryoSerializer(conf).newInstance()

    val instance1 = Instance(19.0, 2.0, Vectors.dense(1.0, 7.0))
    val instance2 = Instance(17.0, 1.0, Vectors.dense(0.0, 5.0).toSparse)
    Seq(instance1, instance2).foreach { i =>
      val i2 = ser.deserialize[Instance](ser.serialize(i))
      assert(i === i2)
    }

    val oInstance1 = OffsetInstance(0.2, 1.0, 2.0, Vectors.dense(0.0, 5.0))
    val oInstance2 = OffsetInstance(0.2, 1.0, 2.0, Vectors.dense(0.0, 5.0).toSparse)
    Seq(oInstance1, oInstance2).foreach { o =>
      val o2 = ser.deserialize[OffsetInstance](ser.serialize(o))
      assert(o === o2)
    }
  }
}
