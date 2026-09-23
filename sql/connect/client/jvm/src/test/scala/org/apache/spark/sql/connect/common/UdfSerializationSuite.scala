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
package org.apache.spark.sql.connect.common

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InvalidClassException}
import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.spark.sql.connect.test.ConnectFunSuite
import org.apache.spark.sql.types.{SuidCompatV1, SuidCompatV2, SuidCustomV1, SuidExplicitV1}
import org.apache.spark.sql.types.{SuidLayoutV1, SuidLayoutV2}

/**
 * Tests for [[UdfSerialization]]'s tolerance of `serialVersionUID` drift for
 * `org.apache.spark.sql.types` classes.
 *
 * Each fixture pair has same-length class names, so serializing the `*V1` instance and patching
 * the class name in the byte stream to `*V2` yields exactly what a `*V2` reader would see from a
 * `*V1` producer whose `serialVersionUID` differs -- a deterministic stand-in for a cross-version
 * payload without needing two builds.
 */
class UdfSerializationSuite extends ConnectFunSuite {

  private def serialize(o: AnyRef): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
    bos.toByteArray
  }

  /** Replace the ASCII `from` class name with the same-length `to` in a serialized stream. */
  private def patchClassName(bytes: Array[Byte], from: String, to: String): Array[Byte] = {
    assert(from.length == to.length, "class names must be the same length for an in-place patch")
    val fromBytes = from.getBytes("US-ASCII")
    val toBytes = to.getBytes("US-ASCII")
    val idx = bytes.indexOfSlice(fromBytes)
    assert(idx >= 0, s"$from not found in the serialized stream")
    val patched = bytes.clone()
    System.arraycopy(toBytes, 0, patched, idx, toBytes.length)
    patched
  }

  private val loader = getClass.getClassLoader

  test("tolerates serialVersionUID drift for a sql.types class (bytes overload)") {
    val stream = patchClassName(serialize(SuidCompatV1(7, "hi")), "SuidCompatV1", "SuidCompatV2")

    // A plain ObjectInputStream rejects the SUID mismatch: this is the failure being fixed.
    intercept[InvalidClassException] {
      new ObjectInputStream(new ByteArrayInputStream(stream)).readObject()
    }

    // The tolerant reader rebinds to the local class and recovers the value.
    val result = UdfSerialization.deserialize[SuidCompatV2](stream, loader)
    assert(result === SuidCompatV2(7, "hi"))
  }

  test("tolerates serialVersionUID drift for a sql.types class (InputStream overload)") {
    val stream = patchClassName(serialize(SuidCompatV1(9, "yo")), "SuidCompatV1", "SuidCompatV2")
    val result = UdfSerialization.deserialize[SuidCompatV2](new ByteArrayInputStream(stream))
    assert(result === SuidCompatV2(9, "yo"))
  }

  test("fails fast on a genuine field-layout change for a sql.types class") {
    val stream = patchClassName(serialize(SuidLayoutV1(1, "x")), "SuidLayoutV1", "SuidLayoutV2")
    intercept[InvalidClassException] {
      UdfSerialization.deserialize[SuidLayoutV2](stream, loader)
    }
  }

  test("does not tolerate an explicit serialVersionUID change") {
    val stream =
      patchClassName(serialize(SuidExplicitV1(1, "x")), "SuidExplicitV1", "SuidExplicitV2")
    intercept[InvalidClassException] {
      UdfSerialization.deserialize[AnyRef](stream, loader)
    }
  }

  test("does not tolerate drift for a class with a custom readObject") {
    val stream = patchClassName(serialize(SuidCustomV1(1, "x")), "SuidCustomV1", "SuidCustomV2")
    intercept[InvalidClassException] {
      UdfSerialization.deserialize[AnyRef](stream, loader)
    }
  }

  test("does not tolerate serialVersionUID drift outside sql.types") {
    val stream =
      patchClassName(serialize(SuidUntolerantV1(1, "x")), "SuidUntolerantV1", "SuidUntolerantV2")
    intercept[InvalidClassException] {
      UdfSerialization.deserialize[AnyRef](stream, loader)
    }
  }
}
