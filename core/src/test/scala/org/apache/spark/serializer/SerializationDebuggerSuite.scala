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

import java.io.{ObjectOutput, ObjectInput}

import org.scalatest.{BeforeAndAfterEach, FunSuite}


class SerializationDebuggerSuite extends FunSuite with BeforeAndAfterEach {

  import SerializationDebugger.find

  override def beforeEach(): Unit = {
    SerializationDebugger.enableDebugging = true
  }

  test("primitives, strings, and nulls") {
    assert(find(1) === List.empty)
    assert(find(1L) === List.empty)
    assert(find(1.toShort) === List.empty)
    assert(find(1.0) === List.empty)
    assert(find("1") === List.empty)
    assert(find(null) === List.empty)
  }

  test("primitive arrays") {
    assert(find(Array[Int](1, 2)) === List.empty)
    assert(find(Array[Long](1, 2)) === List.empty)
  }

  test("non-primitive arrays") {
    assert(find(Array("aa", "bb")) === List.empty)
    assert(find(Array(new SerializableClass1)) === List.empty)
  }

  test("serializable object") {
    assert(find(new Foo(1, "b", 'c', 'd', null, null, null)) === List.empty)
  }

  test("nested arrays") {
    val foo1 = new Foo(1, "b", 'c', 'd', null, null, null)
    val foo2 = new Foo(1, "b", 'c', 'd', null, Array(foo1), null)
    assert(find(new Foo(1, "b", 'c', 'd', null, Array(foo2), null)) === List.empty)
  }

  test("nested objects") {
    val foo1 = new Foo(1, "b", 'c', 'd', null, null, null)
    val foo2 = new Foo(1, "b", 'c', 'd', null, null, foo1)
    assert(find(new Foo(1, "b", 'c', 'd', null, null, foo2)) === List.empty)
  }

  test("cycles (should not loop forever)") {
    val foo1 = new Foo(1, "b", 'c', 'd', null, null, null)
    foo1.g = foo1
    assert(find(new Foo(1, "b", 'c', 'd', null, null, foo1)) === List.empty)
  }

  test("root object not serializable") {
    val s = find(new NotSerializable)
    assert(s.size === 1)
    assert(s.head.contains("NotSerializable"))
  }

  test("array containing not serializable element") {
    val s = find(new SerializableArray(Array(new NotSerializable)))
    assert(s.size === 5)
    assert(s(0).contains("NotSerializable"))
    assert(s(1).contains("element of array"))
    assert(s(2).contains("array"))
    assert(s(3).contains("arrayField"))
    assert(s(4).contains("SerializableArray"))
  }

  test("object containing not serializable field") {
    val s = find(new SerializableClass2(new NotSerializable))
    assert(s.size === 3)
    assert(s(0).contains("NotSerializable"))
    assert(s(1).contains("objectField"))
    assert(s(2).contains("SerializableClass2"))
  }

  test("externalizable class writing out not serializable object") {
    val s = find(new ExternalizableClass)
    assert(s.size === 5)
    assert(s(0).contains("NotSerializable"))
    assert(s(1).contains("objectField"))
    assert(s(2).contains("SerializableClass2"))
    assert(s(3).contains("writeExternal"))
    assert(s(4).contains("ExternalizableClass"))
  }
}


class SerializableClass1 extends Serializable


class SerializableClass2(val objectField: Object) extends Serializable


class SerializableArray(val arrayField: Array[Object]) extends Serializable


class ExternalizableClass extends java.io.Externalizable {
  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeInt(1)
    out.writeObject(new SerializableClass2(new NotSerializable))
  }

  override def readExternal(in: ObjectInput): Unit = {}
}


class Foo(
    a: Int,
    b: String,
    c: Char,
    d: Byte,
    e: Array[Int],
    f: Array[Object],
    var g: Foo) extends Serializable


class NotSerializable
