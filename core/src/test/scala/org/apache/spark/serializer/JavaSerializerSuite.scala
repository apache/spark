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
import org.apache.spark.internal.config.JAVA_SERIALIZER_CACHE_RESOLVED_CLASSES

class JavaSerializerSuite extends SparkFunSuite {
  test("JavaSerializer(without resolve class cache) instances are serializable") {
    val serializer = getSerializer(false)
    val instance = serializer.newInstance()
    val obj = instance.deserialize[JavaSerializer](instance.serialize(serializer))
    // enforce class cast
    obj.getClass
  }

  test("JavaSerializer(with resolve class cache) instances are serializable") {
    val serializer = getSerializer(true)
    val instance = serializer.newInstance()
    val obj = instance.deserialize[JavaSerializer](instance.serialize(serializer))
    // enforce class cast
    obj.getClass
  }

  test("Deserialize(without resolve class cache) object containing a primitive " +
    "Class as attribute") {
    val serializer = getSerializer(false)
    val instance = serializer.newInstance()
    val obj = instance.deserialize[ContainsPrimitiveClass](instance.serialize(
      new ContainsPrimitiveClass()))
    // enforce class cast
    obj.getClass
  }

  test("Deserialize(with resolve class cache) object containing a primitive " +
    "Class as attribute") {
    val serializer = getSerializer(true)
    val instance = serializer.newInstance()
    val obj = instance.deserialize[ContainsPrimitiveClass](instance.serialize(
      new ContainsPrimitiveClass()))
    // enforce class cast
    obj.getClass
  }

  test ("Deserialize(without resolve class cache) object with provider ClassLoader") {
    val serializer = getSerializer(false)
    val instance = serializer.newInstance()
    deserializeWithClassLoader(instance)
    deserializeWithClassLoader(instance)
  }

  test ("Deserialize(with resolve class cache) object with provider ClassLoader") {
    val serializer = getSerializer(true)
    val instance = serializer.newInstance()
    deserializeWithClassLoader(instance)
    deserializeWithClassLoader(instance)
  }

  private def deserializeWithClassLoader(instance: SerializerInstance): Unit = {
    val myClass1Instance1 = new MyClass1
    val myClass2Instance1 = new MyClass2
    val bytes1 = instance.serialize(myClass1Instance1)
    val bytes2 = instance.serialize(myClass2Instance1)
    val deserialized1 = instance.deserialize[MyClass1](bytes1, loader1)
    val deserialized2 = instance.deserialize[MyClass2](bytes2, loader2)
    deserialized1.getClass
    deserialized2.getClass

    bytes1.rewind()
    bytes2.rewind()

    val thrown1 = intercept[ClassNotFoundException] {
      instance.deserialize(bytes1, loader2)
    }
    assert(thrown1.getMessage.contains("ClassLoader2 can't load class"))

    val thrown2 = intercept[ClassNotFoundException] {
      instance.deserialize(bytes2, loader1)
    }
    assert(thrown2.getMessage.contains("ClassLoader1 can't load class"))
  }

  def getSerializer(cache: Boolean): JavaSerializer = {
    val conf = new SparkConf()
    conf.set(JAVA_SERIALIZER_CACHE_RESOLVED_CLASSES, cache)

    new JavaSerializer(conf)
  }

  lazy val loader1 = new ClassLoader(null) {
    override def loadClass(name: String): Class[_] = {
      if (classOf[MyClass1].getName == name) {
        classOf[MyClass1]
      } else {
        throw new ClassNotFoundException(s"ClassLoader1 can't load class :${name}")
      }
    }
  }

  lazy val loader2 = new ClassLoader(null) {
    override def loadClass(name: String): Class[_] = {
      if (classOf[MyClass2].getName == name) {
        classOf[MyClass2]
      } else {
        throw new ClassNotFoundException(s"ClassLoader2 can't load class :${name}")
      }
    }
  }
}

class MyClass1 extends Serializable
class MyClass2 extends Serializable

private class ContainsPrimitiveClass extends Serializable {
  val intClass = classOf[Int]
  val longClass = classOf[Long]
  val shortClass = classOf[Short]
  val charClass = classOf[Char]
  val doubleClass = classOf[Double]
  val floatClass = classOf[Float]
  val booleanClass = classOf[Boolean]
  val byteClass = classOf[Byte]
  val voidClass = classOf[Void]
}
