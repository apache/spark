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

package org.apache.spark.api.python

import java.util.HashMap

import org.apache.hadoop.io.{MapWritable, ShortWritable, Text}
import org.junit.Assert
import org.mockito.Mockito.mock

import org.apache.spark.SparkFunSuite
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.SerializableConfiguration

class PythonHadoopUtilSuite extends SparkFunSuite {
  test("Testing that ShortWritables convert to Shorts") {
    val broadcast = mock(classOf[Broadcast[SerializableConfiguration]])
    val writableToJavaConverter = new WritableToJavaConverter(broadcast)
    val shortWritable = new ShortWritable(5)
    val result = writableToJavaConverter.convert(shortWritable)
    val expected = 5.asInstanceOf[Short]
    Assert.assertEquals(expected, result)
  }

  test("Testing that ShortWritables in MapWritables convert to Shorts") {
    val broadcast = mock(classOf[Broadcast[SerializableConfiguration]])
    val writableToJavaConverter = new WritableToJavaConverter(broadcast)
    val keyString = "key"
    val key = new Text(keyString)
    val shortWritable = new ShortWritable(5)
    val mapWritable = new MapWritable()
    mapWritable.put(key, shortWritable)
    val result = writableToJavaConverter.convert(mapWritable)
    val expected = new HashMap[String, Short]()
    expected.put(keyString, 5.asInstanceOf[Short])
    Assert.assertEquals(expected, result)
  }
}
