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

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkFunSuite

class PythonRDDSuite extends SparkFunSuite {

  test("Writing large strings to the worker") {
    val input: List[String] = List("a"*100000)
    val buffer = new DataOutputStream(new ByteArrayOutputStream)
    PythonRDD.writeIteratorToStream(input.iterator, buffer)
  }

  test("Handle nulls gracefully") {
    val buffer = new DataOutputStream(new ByteArrayOutputStream)
    // Should not have NPE when write an Iterator with null in it
    // The correctness will be tested in Python
    PythonRDD.writeIteratorToStream(Iterator("a", null), buffer)
    PythonRDD.writeIteratorToStream(Iterator(null, "a"), buffer)
    PythonRDD.writeIteratorToStream(Iterator("a".getBytes(StandardCharsets.UTF_8), null), buffer)
    PythonRDD.writeIteratorToStream(Iterator(null, "a".getBytes(StandardCharsets.UTF_8)), buffer)
    PythonRDD.writeIteratorToStream(Iterator((null, null), ("a", null), (null, "b")), buffer)
    PythonRDD.writeIteratorToStream(Iterator(
      (null, null),
      ("a".getBytes(StandardCharsets.UTF_8), null),
      (null, "b".getBytes(StandardCharsets.UTF_8))), buffer)
  }
}
