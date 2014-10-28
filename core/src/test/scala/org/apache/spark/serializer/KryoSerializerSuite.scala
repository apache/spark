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

import scala.collection.mutable
import scala.reflect.ClassTag

import com.esotericsoftware.kryo.Kryo
import org.scalatest.FunSuite

import org.apache.spark.{SparkConf, SharedSparkContext}
import org.apache.spark.serializer.KryoTest._


class KryoSerializerSuite extends FunSuite with SharedSparkContext {
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)

  test("basic types") {
    val ser = new KryoSerializer(conf).newInstance()
    def check[T: ClassTag](t: T) {
      assert(ser.deserialize[T](ser.serialize(t)) === t)
    }
    check(1)
    check(1L)
    check(1.0f)
    check(1.0)
    check(1.toByte)
    check(1.toShort)
    check("")
    check("hello")
    check(Integer.MAX_VALUE)
    check(Integer.MIN_VALUE)
    check(java.lang.Long.MAX_VALUE)
    check(java.lang.Long.MIN_VALUE)
    check[String](null)
    check(Array(1, 2, 3))
    check(Array(1L, 2L, 3L))
    check(Array(1.0, 2.0, 3.0))
    check(Array(1.0f, 2.9f, 3.9f))
    check(Array("aaa", "bbb", "ccc"))
    check(Array("aaa", "bbb", null))
    check(Array(true, false, true))
    check(Array('a', 'b', 'c'))
    check(Array[Int]())
    check(Array(Array("1", "2"), Array("1", "2", "3", "4")))
  }

  test("pairs") {
    val ser = new KryoSerializer(conf).newInstance()
    def check[T: ClassTag](t: T) {
      assert(ser.deserialize[T](ser.serialize(t)) === t)
    }
    check((1, 1))
    check((1, 1L))
    check((1L, 1))
    check((1L,  1L))
    check((1.0, 1))
    check((1, 1.0))
    check((1.0, 1.0))
    check((1.0, 1L))
    check((1L, 1.0))
    check((1.0, 1L))
    check(("x", 1))
    check(("x", 1.0))
    check(("x", 1L))
    check((1, "x"))
    check((1.0, "x"))
    check((1L, "x"))
    check(("x", "x"))
  }

  test("Scala data structures") {
    val ser = new KryoSerializer(conf).newInstance()
    def check[T: ClassTag](t: T) {
      assert(ser.deserialize[T](ser.serialize(t)) === t)
    }
    check(List[Int]())
    check(List[Int](1, 2, 3))
    check(List[String]())
    check(List[String]("x", "y", "z"))
    check(None)
    check(Some(1))
    check(Some("hi"))
    check(mutable.ArrayBuffer(1, 2, 3))
    check(mutable.ArrayBuffer("1", "2", "3"))
    check(mutable.Map())
    check(mutable.Map(1 -> "one", 2 -> "two"))
    check(mutable.Map("one" -> 1, "two" -> 2))
    check(mutable.HashMap(1 -> "one", 2 -> "two"))
    check(mutable.HashMap("one" -> 1, "two" -> 2))
    check(List(Some(mutable.HashMap(1->1, 2->2)), None, Some(mutable.HashMap(3->4))))
    check(List(mutable.HashMap("one" -> 1, "two" -> 2),mutable.HashMap(1->"one",2->"two",3->"three")))
  }

  test("ranges") {
    val ser = new KryoSerializer(conf).newInstance()
    def check[T: ClassTag](t: T) {
      assert(ser.deserialize[T](ser.serialize(t)) === t)
      // Check that very long ranges don't get written one element at a time
      assert(ser.serialize(t).limit < 100)
    }
    check(1 to 1000000)
    check(1 to 1000000 by 2)
    check(1 until 1000000)
    check(1 until 1000000 by 2)
    check(1L to 1000000L)
    check(1L to 1000000L by 2L)
    check(1L until 1000000L)
    check(1L until 1000000L by 2L)
    check(1.0 to 1000000.0 by 1.0)
    check(1.0 to 1000000.0 by 2.0)
    check(1.0 until 1000000.0 by 1.0)
    check(1.0 until 1000000.0 by 2.0)
  }

  test("asJavaIterable") {
    // Serialize a collection wrapped by asJavaIterable
    val ser = new KryoSerializer(conf).newInstance()
    val a = ser.serialize(scala.collection.convert.WrapAsJava.asJavaIterable(Seq(12345)))
    val b = ser.deserialize[java.lang.Iterable[Int]](a)
    assert(b.iterator().next() === 12345)

    // Serialize a normal Java collection
    val col = new java.util.ArrayList[Int]
    col.add(54321)
    val c = ser.serialize(col)
    val d = ser.deserialize[java.lang.Iterable[Int]](c)
    assert(b.iterator().next() === 12345)
  }

  test("custom registrator") {
    val ser = new KryoSerializer(conf).newInstance()
    def check[T: ClassTag](t: T) {
      assert(ser.deserialize[T](ser.serialize(t)) === t)
    }

    check(CaseClass(17, "hello"))

    val c1 = new ClassWithNoArgConstructor
    c1.x = 32
    check(c1)

    val c2 = new ClassWithoutNoArgConstructor(47)
    check(c2)

    val hashMap = new java.util.HashMap[String, String]
    hashMap.put("foo", "bar")
    check(hashMap)

    System.clearProperty("spark.kryo.registrator")
  }

  test("kryo with collect") {
    val control = 1 :: 2 :: Nil
    val result = sc.parallelize(control, 2).map(new ClassWithoutNoArgConstructor(_)).collect().map(_.x)
    assert(control === result.toSeq)
  }

  test("kryo with parallelize") {
    val control = 1 :: 2 :: Nil
    val result = sc.parallelize(control.map(new ClassWithoutNoArgConstructor(_))).map(_.x).collect()
    assert (control === result.toSeq)
  }

  test("kryo with parallelize for specialized tuples") {
    assert (sc.parallelize( Array((1, 11), (2, 22), (3, 33)) ).count === 3)
  }

  test("kryo with parallelize for primitive arrays") {
    assert (sc.parallelize( Array(1, 2, 3) ).count === 3)
  }

  test("kryo with collect for specialized tuples") {
    assert (sc.parallelize( Array((1, 11), (2, 22), (3, 33)) ).collect().head === (1, 11))
  }

  test("kryo with SerializableHyperLogLog") {
    assert(sc.parallelize( Array(1, 2, 3, 2, 3, 3, 2, 3, 1) ).countApproxDistinct(0.01) === 3)
  }

  test("kryo with reduce") {
    val control = 1 :: 2 :: Nil
    val result = sc.parallelize(control, 2).map(new ClassWithoutNoArgConstructor(_))
        .reduce((t1, t2) => new ClassWithoutNoArgConstructor(t1.x + t2.x)).x
    assert(control.sum === result)
  }

  // TODO: this still doesn't work
  ignore("kryo with fold") {
    val control = 1 :: 2 :: Nil
    val result = sc.parallelize(control, 2).map(new ClassWithoutNoArgConstructor(_))
        .fold(new ClassWithoutNoArgConstructor(10))((t1, t2) => new ClassWithoutNoArgConstructor(t1.x + t2.x)).x
    assert(10 + control.sum === result)
  }

  test("kryo with nonexistent custom registrator should fail") {
    import org.apache.spark.SparkException

    val conf = new SparkConf(false)
    conf.set("spark.kryo.registrator", "this.class.does.not.exist")

    val thrown = intercept[SparkException](new KryoSerializer(conf).newInstance())
    assert(thrown.getMessage.contains("Failed to register classes with Kryo"))
  }

  test("default class loader can be set by a different thread") {
    val ser = new KryoSerializer(new SparkConf)

    // First serialize the object
    val serInstance = ser.newInstance()
    val bytes = serInstance.serialize(new ClassLoaderTestingObject)

    // Deserialize the object to make sure normal deserialization works
    serInstance.deserialize[ClassLoaderTestingObject](bytes)

    // Set a special, broken ClassLoader and make sure we get an exception on deserialization
    ser.setDefaultClassLoader(new ClassLoader() {
      override def loadClass(name: String) = throw new UnsupportedOperationException
    })
    intercept[UnsupportedOperationException] {
      ser.newInstance().deserialize[ClassLoaderTestingObject](bytes)
    }
  }
}


class ClassLoaderTestingObject


object KryoTest {

  case class CaseClass(i: Int, s: String) {}

  class ClassWithNoArgConstructor {
    var x: Int = 0
    override def equals(other: Any) = other match {
      case c: ClassWithNoArgConstructor => x == c.x
      case _ => false
    }
  }

  class ClassWithoutNoArgConstructor(val x: Int) {
    override def equals(other: Any) = other match {
      case c: ClassWithoutNoArgConstructor => x == c.x
      case _ => false
    }
  }

  class MyRegistrator extends KryoRegistrator {
    override def registerClasses(k: Kryo) {
      k.register(classOf[CaseClass])
      k.register(classOf[ClassWithNoArgConstructor])
      k.register(classOf[ClassWithoutNoArgConstructor])
      k.register(classOf[java.util.HashMap[_, _]])
    }
  }
}
