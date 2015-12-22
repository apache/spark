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

package org.apache.spark.serializer

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.SparkContext
import org.apache.spark.LocalSparkContext
import org.apache.spark.SparkException


class KryoSerializerResizableOutputSuite extends SparkFunSuite {

  // trial and error showed this will not serialize with 1mb buffer
  val x = (1 to 400000).toArray

  test("kryo without resizable output buffer should fail on large array") {
    val conf = new SparkConf(false)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryoserializer.buffer", "1m")
    conf.set("spark.kryoserializer.buffer.max", "1m")
    val sc = new SparkContext("local", "test", conf)
    intercept[SparkException](sc.parallelize(x).collect())
    LocalSparkContext.stop(sc)
  }

  test("kryo with resizable output buffer should succeed on large array") {
    val conf = new SparkConf(false)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryoserializer.buffer", "1m")
    conf.set("spark.kryoserializer.buffer.max", "2m")
    val sc = new SparkContext("local", "test", conf)
    assert(sc.parallelize(x).collect() === x)
    LocalSparkContext.stop(sc)
  }
}
