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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.serializer.JavaSerializer

class CaseInsensitiveMapSuite extends SparkFunSuite {
  private def shouldBeSerializable(m: Map[String, String]): Unit = {
    new JavaSerializer(new SparkConf()).newInstance().serialize(m)
  }

  test("Keys are case insensitive") {
    val m = CaseInsensitiveMap(Map("a" -> "b", "foO" -> "bar"))
    assert(m("FOO") == "bar")
    assert(m("fOo") == "bar")
    assert(m("A") == "b")
    shouldBeSerializable(m)
  }

  test("CaseInsensitiveMap should be serializable after '-' operator") {
    val m = CaseInsensitiveMap(Map("a" -> "b", "foo" -> "bar")) - "a"
    assert(m == Map("foo" -> "bar"))
    shouldBeSerializable(m)
  }

  test("CaseInsensitiveMap should be serializable after '+' operator") {
    val m = CaseInsensitiveMap(Map("a" -> "b", "foo" -> "bar")) + ("x" -> "y")
    assert(m == Map("a" -> "b", "foo" -> "bar", "x" -> "y"))
    shouldBeSerializable(m)
  }
}
