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

package org.apache.spark.sql.hive

import java.util

import org.apache.hadoop.hive.serde2.io.ShortWritable
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory._
import org.apache.hadoop.io._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class HiveHashSuite extends SparkFunSuite {
  private val Seed = 0

  def verifyHashes(pair: (DataType, Any, Writable, ObjectInspector)): Unit = {
    val (dataType, value, writableValue, inspector) = pair
    val expectedHash = ObjectInspectorUtils.hashCode(writableValue, inspector)
    val actualHash = HiveHashFunction.hash(value, dataType, Seed)
    assert(expectedHash == actualHash)
  }

  test("primitive datatypes") {
    val (boolTrue, boolFalse) = (true, false)
    val (byte, short, integer, long) = (198.toByte, 5544.toShort, 123457878, 89945787834323L)
    val (float, double) = (12.65F, 1945.9009)

    Seq(
      (BooleanType, boolTrue, new BooleanWritable(boolTrue), writableBooleanObjectInspector),
      (BooleanType, boolFalse, new BooleanWritable(boolFalse), writableBooleanObjectInspector),
      (ByteType, byte, new ByteWritable(byte), writableByteObjectInspector),
      (ShortType, short, new ShortWritable(short), writableShortObjectInspector),
      (IntegerType, integer, new IntWritable(integer), writableIntObjectInspector),
      (LongType, long, new LongWritable(long), writableLongObjectInspector),
      (FloatType, float, new FloatWritable(float), writableFloatObjectInspector),
      (DoubleType, double, new DoubleWritable(double), writableDoubleObjectInspector)
    ).foreach(verifyHashes)
  }

  test("UTF8String data") {
    val input = "my_test"
    val text = new Text(input)
    val inspector = writableStringObjectInspector
    val expectedHash = ObjectInspectorUtils.hashCode(text, inspector)

    val utf8String = UTF8String.fromString(input)
    val actualHash = HiveHashFunction.hash(utf8String, StringType, Seed)
    assert(expectedHash == actualHash)
  }

  test("array datatype") {
    val array = Array(1, 2, 3)
    val list = new util.ArrayList[IntWritable]()
    array.foreach(item => list.add(new IntWritable(item)))

    val listInspector =
      ObjectInspectorFactory.getStandardListObjectInspector(writableIntObjectInspector)
    val expectedHash = ObjectInspectorUtils.hashCode(list, listInspector)

    val input = new GenericArrayData(array)
    val actualHash = HiveHashFunction.hash(input, ArrayType(IntegerType), Seed)
    assert(expectedHash == actualHash)
  }

  test("map datatype") {
    val keys = Array(1, 2, 3)
    val values = Array(10, 20, 30)
    val map = new util.LinkedHashMap[IntWritable, IntWritable]()
    keys.zip(values).map {
      case (k: Int, v: Int) =>
        map.put(new IntWritable(k), new IntWritable(v))
    }

    val listInspector =
      ObjectInspectorFactory.getStandardMapObjectInspector(
        writableIntObjectInspector,
        writableIntObjectInspector)

    val expectedHash = ObjectInspectorUtils.hashCode(map, listInspector)

    val input = new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(values))
    val actualHash = HiveHashFunction.hash(input, MapType(IntegerType, IntegerType), Seed)
    assert(expectedHash == actualHash)
  }
}
