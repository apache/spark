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

class LabeledPointSuite extends SparkFunSuite {
  test("Kryo class register") {
    val conf = new SparkConf(false)
    conf.set(KRYO_REGISTRATION_REQUIRED, true)

    val ser = new KryoSerializer(conf).newInstance()

    val labeled1 = LabeledPoint(1.0, Vectors.dense(Array(1.0, 2.0)))
    val labeled2 = LabeledPoint(1.0, Vectors.sparse(10, Array(5, 7), Array(1.0, 2.0)))

    Seq(labeled1, labeled2).foreach { l =>
      val l2 = ser.deserialize[LabeledPoint](ser.serialize(l))
      assert(l === l2)
    }
  }
}
