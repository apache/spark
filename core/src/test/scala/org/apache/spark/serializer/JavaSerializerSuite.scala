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

import java.io.ObjectInputFilter

import org.apache.spark.{SparkConf, SparkFunSuite}

class JavaSerializerSuite extends SparkFunSuite {
  test("JavaSerializer instances are serializable") {
    val serializer = new JavaSerializer(new SparkConf())
    val instance = serializer.newInstance()
    val obj = instance.deserialize[JavaSerializer](instance.serialize(serializer))
    // enforce class cast
    obj.getClass
  }

  test("Deserialize object containing a primitive Class as attribute") {
    val serializer = new JavaSerializer(new SparkConf())
    val instance = serializer.newInstance()
    val obj = instance.deserialize[ContainsPrimitiveClass](
      instance.serialize(new ContainsPrimitiveClass()))
    // enforce class cast
    obj.getClass
  }

  test("SPARK-36627: Deserialize object containing a proxy Class as attribute") {
    var classesLoaded = Set[String]()
    val outer = Thread.currentThread.getContextClassLoader
    val inner = new ClassLoader() {
      override def loadClass(name: String): Class[_] = {
        classesLoaded = classesLoaded + name
        outer.loadClass(name)
      }
    }
    Thread.currentThread.setContextClassLoader(inner)

    val serializer = new JavaSerializer(new SparkConf())
    val instance = serializer.newInstance()
    val obj =
      instance.deserialize[ContainsProxyClass](instance.serialize(new ContainsProxyClass()))
    // enforce class cast
    obj.getClass

    // check that serializer's loader is used to resolve proxied interface.
    assert(classesLoaded.exists(klass => klass.contains("MyInterface")))
  }

  test("JavaDeserializationStream filter composition preserves rejection by either filter") {
    def filterInfo(clazz: Class[_], arrayLen: Long): ObjectInputFilter.FilterInfo =
      new ObjectInputFilter.FilterInfo {
        override def serialClass(): Class[_] = clazz
        override def arrayLength(): Long = arrayLen
        override def depth(): Long = 1
        override def references(): Long = 0
        override def streamBytes(): Long = 0
      }

    val allowAll = ObjectInputFilter.Config.createFilter("*")
    val rejectAll = ObjectInputFilter.Config.createFilter("!*")
    val maxOneElementArray = ObjectInputFilter.Config.createFilter("maxarray=1")
    val stringsOnly = ObjectInputFilter.Config.createFilter("java.lang.String")

    val byteArray = filterInfo(classOf[Array[Byte]], 2)
    val string = filterInfo(classOf[String], -1)

    import JavaDeserializationStream.composeFilters
    import ObjectInputFilter.Status._
    // A rejection by either the existing (e.g. JVM-wide) or the new filter rejects.
    // Note the JDK's pattern filters apply limits like maxarray to array classes and
    // match class patterns against the array's component type, so a byte[] (primitive
    // component) is left undecided by class patterns.
    assert(composeFilters(maxOneElementArray, allowAll).checkInput(byteArray) === REJECTED)
    assert(composeFilters(allowAll, rejectAll).checkInput(string) === REJECTED)
    // An allow by either filter allows when neither rejects.
    assert(composeFilters(stringsOnly, allowAll).checkInput(string) === ALLOWED)
    assert(composeFilters(maxOneElementArray, allowAll).checkInput(string) === ALLOWED)
    // No decision by either filter stays undecided.
    assert(composeFilters(stringsOnly, stringsOnly).checkInput(byteArray) === UNDECIDED)
  }
}

private class ContainsPrimitiveClass extends Serializable {
  val intClass = classOf[Int]
  val longClass = classOf[Long]
  val shortClass = classOf[Short]
  val charClass = classOf[Char]
  val doubleClass = classOf[Double]
  val floatClass = classOf[Float]
  val booleanClass = classOf[Boolean]
  val byteClass = classOf[Byte]
  val voidClass = classOf[Unit]
}
