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

package org.apache.spark.ml.tree.impl

import scala.reflect.ClassTag

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.serializer.KryoSerializer

class TreePointSuite extends SparkFunSuite{
  test("Kryo class register") {
    val conf = new SparkConf(false)
    conf.set("spark.kryo.registrationRequired", "true")

    val ser = new KryoSerializer(conf)
    val serInstance = new KryoSerializer(conf).newInstance()

    def check[T: ClassTag](t: T) {
      assert(serInstance.deserialize[T](serInstance.serialize(t)) === t)
    }

    val point = new TreePoint(1.0, Array(1, 2, 3))
    check(point)
  }
}
