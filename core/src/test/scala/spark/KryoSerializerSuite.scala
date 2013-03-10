package spark

import scala.collection.mutable
import scala.collection.immutable

import org.scalatest.FunSuite
import com.esotericsoftware.kryo._

import SparkContext._

class KryoSerializerSuite extends FunSuite {
  test("basic types") {
    val ser = (new KryoSerializer).newInstance()
    def check[T](t: T) {
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
  }

  test("pairs") {
    val ser = (new KryoSerializer).newInstance()
    def check[T](t: T) {
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
    val ser = (new KryoSerializer).newInstance()
    def check[T](t: T) {
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

  test("custom registrator") {
    import spark.test._
    System.setProperty("spark.kryo.registrator", classOf[MyRegistrator].getName)

    val ser = (new KryoSerializer).newInstance()
    def check[T](t: T) {
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
}

package test {
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
