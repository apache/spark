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
package org.apache.spark.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, NotSerializableException, ObjectInputStream, ObjectOutputStream}
import java.util.Base64

import org.apache.spark.SparkFunSuite

@SerialVersionUID(2108928072825530631L)
case class LazyUsage() extends Serializable {
  private val lazyString = new Lazy(LazyFunction.GetString())
  def getLazyString(): String = lazyString()
}

object LazyFunction {
  def GetString(): String = "hello world"
}

class LazySuite extends SparkFunSuite {
  // scalastyle:off
  private val base64WithLambda = "rO0ABXNyAB9vcmcuYXBhY2hlLnNwYXJrLnV0aWwuTGF6eVVzYWdlHURq6J2daQcCAAFMAApsYXp5U3RyaW5ndAAcTG9yZy9hcGFjaGUvc3BhcmsvdXRpbC9MYXp5O3hwc3IAGm9yZy5hcGFjaGUuc3BhcmsudXRpbC5MYXp5bofmkNVKElQDAANaAAhiaXRtYXAkMEwAC2luaXRpYWxpemVydAARTHNjYWxhL0Z1bmN0aW9uMDtMAAV2YWx1ZXQAEkxqYXZhL2xhbmcvT2JqZWN0O3hwAXB0AAtoZWxsbyB3b3JsZHg="
  // scalastyle:on

  test("Lazy val works") {
    var test: Option[Object] = None

    val lazyval = new Lazy({
      test = Some(new Object())
      test
    })

    // Ensure no initialization happened before the lazy value was dereferenced
    assert(test.isEmpty)

    // Ensure the first invocation creates a new object
    assert(lazyval() == test && test.isDefined)

    // Ensure the subsequent invocation serves the same object
    assert(lazyval() == test && test.isDefined)
  }

  test("Lazy val serialization fails if the dereferenced object is not serializable") {
    val lazyval = new Lazy({
      new Object()
    })

    // Ensure we are not serializable after the dereference (type "Object" is not serializable)
    intercept[NotSerializableException] {
      val oos = new ObjectOutputStream(new ByteArrayOutputStream())
      oos.writeObject(lazyval)
    }
  }

  test("Lazy val serializes the value, even if dereference was never called") {
    var times = 100
    val lazyval = new Lazy({
      val result = "0" * times
      times = 0
      result
    })

    // Ensure we serialized the value even if it was never dereferenced
    val base64str = serializeToBase64(lazyval)
    assert(base64str.contains("MDAw" * 32)) // MDAw is the base64 encoding of "000"
  }

  test("Lazy val serializes the value, if dereference was called") {
    var times = 100
    val lazyval = new Lazy({
      val result = "0" * times
      times = 0
      result
    })
    val zeros = lazyval()
    assert(zeros.length == 100)
    // Ensure we serialized the same value
    val base64str = serializeToBase64(lazyval)
    assert(base64str.contains("MDAw" * 32)) // MDAw is the base64 encoding of "000"
  }

  test("Lazy val can be deserialized from lambda") {
    val lazyObj = deserializeFromBase64[LazyUsage](base64WithLambda)
    assert(lazyObj.getLazyString() === "hello world")
  }

  test("Lazy value round trip serialization works correctly") {
    val lazyval = LazyUsage()
    val serialized = serializeToBase64(lazyval)
    val obj = deserializeFromBase64[LazyUsage](serialized)
    assert(obj.getLazyString() === lazyval.getLazyString())
  }

  test("serialVersionUID was serialized and deserialized correctly") {
    val lazyval = new Lazy("")
    val serialized = serializeToBase64(lazyval)
    val obj = deserializeFromBase64[Lazy[String]](serialized)
    val field = obj.getClass.getDeclaredField("serialVersionUID")
    field.setAccessible(true)
    assert(field.getLong(obj) == 7964587975756091988L)
  }

  def serializeToBase64[T](o: T): String = {
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    try {
      oos.writeObject(o)
      Base64.getEncoder.encodeToString(baos.toByteArray)
    } finally {
      oos.close()
    }
  }

  def deserializeFromBase64[T](base64Str: String): T = {
    val bytes = Base64.getDecoder.decode(base64Str)
    val bais = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bais)
    ois.readObject().asInstanceOf[T]
  }
}
