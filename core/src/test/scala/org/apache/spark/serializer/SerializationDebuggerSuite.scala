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

import java.io.{NotSerializableException, ObjectOutputStream, ByteArrayOutputStream}

import org.scalatest.FunSuite


class SerializationDebuggerSuite extends FunSuite {

  test("normal serialization") {
    SerializationDebugger.enableDebugging = true
    val out = new ObjectOutputStream(new ByteArrayOutputStream)
    SerializationDebugger.writeObject(out, 1)
    out.close()
  }

  test("NotSerializableException with stack") {
    SerializationDebugger.enableDebugging = true
    val out = new ObjectOutputStream(new ByteArrayOutputStream)
    val obj = new SerializableClass1(new SerializableClass2(new NotSerializableClass))
    val e = intercept[NotSerializableException] {
      SerializationDebugger.writeObject(out, obj)
    }
    out.close()

    assert(e.getMessage.contains("SerializableClass1"))
    assert(e.getMessage.contains("SerializableClass2"))
    assert(e.getMessage.contains("NotSerializableClass"))

    // 6 lines:
    // the original message, "Serialization stack", 3 elements in the stack, and debugging tip.
    // And then unfortunately 4 more lines because we enabled sun.io.serialization.extendedDebugInfo
    assert(e.getMessage.split("\n").size === 6 + 4)
  }
}

class NotSerializableClass

class SerializableClass1(val a: Object) extends Serializable

class SerializableClass2(val b: Object) extends Serializable
