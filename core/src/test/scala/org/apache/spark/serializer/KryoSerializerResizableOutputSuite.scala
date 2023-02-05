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
import org.apache.spark.SparkException
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Kryo._

class KryoSerializerResizableOutputSuite extends SparkFunSuite {

  // trial and error showed this will not serialize with 1mb buffer
  val x = (1 to 400000).toArray

  test("kryo without resizable output buffer should fail on large array") {
    val conf = new SparkConf(false)
    conf.set(SERIALIZER, "org.apache.spark.serializer.KryoSerializer")
    conf.set(KRYO_SERIALIZER_BUFFER_SIZE.key, "1m")
    conf.set(KRYO_SERIALIZER_MAX_BUFFER_SIZE.key, "1m")

    val ser = new KryoSerializer(conf)
    val serInstance = ser.newInstance()
    intercept[SparkException](serInstance.serialize(x))
  }

  test("kryo with resizable output buffer should succeed on large array") {
    val conf = new SparkConf(false)
    conf.set(SERIALIZER, "org.apache.spark.serializer.KryoSerializer")
    conf.set(KRYO_SERIALIZER_BUFFER_SIZE.key, "1m")
    conf.set(KRYO_SERIALIZER_MAX_BUFFER_SIZE.key, "2m")

    val ser = new KryoSerializer(conf)
    val serInstance = ser.newInstance()
    serInstance.serialize(x)
  }
}
