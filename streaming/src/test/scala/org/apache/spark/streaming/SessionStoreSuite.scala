package org.apache.spark.streaming

import scala.reflect._

import org.apache.spark.SparkFunSuite
import org.apache.spark.streaming.dstream.{HashMapBasedSessionStore, OpenHashMapBasedSessionStore, Session, SessionStore}
import org.apache.spark.util.Utils

class SessionStoreSuite extends SparkFunSuite {

  HashMapBasedSessionStoreTester.testStore()
  OpenHashMapBasedSessionStoreTester.testStore()

  abstract class SessionStoreTester[StoreType <: SessionStore[Int, Int]: ClassTag] {

    private val clazz = classTag[StoreType].runtimeClass
    private val className = clazz.getSimpleName

    protected def newStore(): StoreType

    def testStore(): Unit = {

      test(className + "- put, get, remove, iterator") {
        val map = newStore()

        map.put(1, 100)
        assert(map.get(1) === Some(100))
        assert(map.get(2) === None)
        map.put(2, 200)
        assert(map.iterator(updatedSessionsOnly = true).toSet ===
          Set(Session(1, 100, true), Session(2, 200, true)))
        assert(map.iterator(updatedSessionsOnly = false).toSet ===
          Set(Session(1, 100, true), Session(2, 200, true)))

        map.remove(1)
        assert(map.get(1) === None)

        assert(map.iterator(updatedSessionsOnly = true).toSet ===
          Set(Session(1, 100, false), Session(2, 200, true)))
        assert(map.iterator(updatedSessionsOnly = false).toSet ===
          Set(Session(1, 100, false), Session(2, 200, true)))
      }

      test(className + " - put, get, remove, iterator after copy") {
        val parentMap = newStore()
        parentMap.put(1, 100)
        parentMap.put(2, 200)
        parentMap.remove(1)

        val map = parentMap.copy()
        assert(map.iterator(updatedSessionsOnly = true).toSet === Set())
        assert(map.iterator(updatedSessionsOnly = false).toSet ===
          Set(Session(1, 100, false), Session(2, 200, true)))

        map.put(3, 300)
        map.put(4, 400)
        map.remove(4)

        assert(map.iterator(updatedSessionsOnly = true).toSet ===
          Set(Session(3, 300, true), Session(4, 400, false)))
        assert(map.iterator(updatedSessionsOnly = false).toSet ===
          Set(Session(1, 100, false), Session(2, 200, true),
            Session(3, 300, true), Session(4, 400, false)))

        assert(parentMap.iterator(updatedSessionsOnly = true).toSet ===
          Set(Session(1, 100, false), Session(2, 200, true)))
        assert(parentMap.iterator(updatedSessionsOnly = false).toSet ===
          Set(Session(1, 100, false), Session(2, 200, true)))

        map.put(1, 1000)
        map.put(2, 2000)
        assert(map.iterator(updatedSessionsOnly = true).toSet ===
          Set(Session(3, 300, true), Session(4, 400, false),
            Session(1, 1000, true), Session(2, 2000, true)))
        assert(map.iterator(updatedSessionsOnly = false).toSet ===
          Set(Session(1, 1000, true), Session(2, 2000, true),
            Session(3, 300, true), Session(4, 400, false)))
      }
      /*
      test(className + " - copying with consolidation") {
        val map1 = newStore()
        map1.put(1, 100)
        map1.put(2, 200)

        val map2 = map1.copy()
        map2.put(3, 300)
        map2.put(4, 400)

        val map3 = map2.copy()
        map3.put(3, 600)
        map3.put(4, 700)

        assert(map3.iterator(false).toSet ===
          map3.asInstanceOf[HashMapBasedSessionStore[Int, Int]].doCopy(true).iterator(false).toSet)

      }*/

      test(className + " - serializing and deserializing") {
        val map1 = newStore()
        map1.put(1, 100)
        map1.put(2, 200)

        val map2 = map1.copy()
        map2.put(3, 300)
        map2.put(4, 400)

        val map3 = map2.copy()
        map3.put(3, 600)
        map3.remove(2)

        val map3_ = Utils.deserialize[SessionStore[Int, Int]](Utils.serialize(map3), Thread.currentThread().getContextClassLoader)
        assert(map3_.iterator(true).toSet === map3.iterator(true).toSet)
        assert(map3_.iterator(false).toSet === map3.iterator(false).toSet)
      }
    }
  }

  object HashMapBasedSessionStoreTester extends SessionStoreTester[HashMapBasedSessionStore[Int, Int]] {
    override protected def newStore(): HashMapBasedSessionStore[Int, Int] = {
      new HashMapBasedSessionStore[Int, Int]()
    }
  }

  object OpenHashMapBasedSessionStoreTester extends SessionStoreTester[OpenHashMapBasedSessionStore[Int, Int]] {
    override protected def newStore(): OpenHashMapBasedSessionStore[Int, Int] = {
      new OpenHashMapBasedSessionStore[Int, Int]()
    }
  }
}