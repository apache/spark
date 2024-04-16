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

import org.apache.hadoop.io.{BooleanWritable, BytesWritable, ByteWritable, DoubleWritable, FloatWritable, IntWritable, LongWritable,
  MapWritable, NullWritable, ShortWritable, Text, Writable}
import org.mockito.Mockito.mock

import org.apache.spark.SparkFunSuite
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.SerializableConfiguration

class PythonHadoopUtilSuite extends SparkFunSuite {
  def checkConversion(input: Writable, expected: Any): Unit = {
    val broadcast = mock(classOf[Broadcast[SerializableConfiguration]])
    val writableToJavaConverter = new WritableToJavaConverter(broadcast)
    val result = writableToJavaConverter.convert(input)
    expected match {
      case _: Array[Byte] => assert(expected.asInstanceOf[Array[Byte]]
        sameElements result.asInstanceOf[Array[Byte]])
      case _ => assert(expected == result)
    }
    val javaToWritableConverter = new JavaToWritableConverter()
    val reConverted = javaToWritableConverter.convert(result)
    assert(input == reConverted, "Round trip conversion failed")
  }

  test("Testing roundtrip conversion of various types") {
    checkConversion(new IntWritable(5), 5)
    checkConversion(new DoubleWritable(5.2), 5.2)
    checkConversion(new LongWritable(Long.MaxValue), Long.MaxValue)
    checkConversion(new ShortWritable(5), 5.asInstanceOf[Short])
    checkConversion(new FloatWritable(5.2f), 5.2.asInstanceOf[Float])
    checkConversion(new Text("This is some text"), "This is some text")
    checkConversion(new BooleanWritable(true), true)
    checkConversion(new BooleanWritable(false), false)
    checkConversion(new ByteWritable(5), 5.asInstanceOf[Byte])
    checkConversion(NullWritable.get(), null)
  }

  test("Testing that BytesWritables convert to arrays of bytes and back") {
    val byteArray = Array.fill[Byte](5)(1)
    val bytesWritable = new BytesWritable(byteArray)
    checkConversion(bytesWritable, byteArray)
  }

  test("Testing that MapWritables convert to Maps and back") {
    val key1String = "key1"
    val key1 = new Text(key1String)
    val key2String = "key2"
    val key2 = new Text(key2String)
    val shortWritable = new ShortWritable(5)
    val longWritable = new LongWritable(55)
    val mapWritable = new MapWritable()
    mapWritable.put(key1, shortWritable)
    mapWritable.put(key2, longWritable)
    val expected = new HashMap[String, Any]()
    expected.put(key1String, 5.asInstanceOf[Short])
    expected.put(key2String, 55.asInstanceOf[Long])
    checkConversion(mapWritable, expected)
  }
}
