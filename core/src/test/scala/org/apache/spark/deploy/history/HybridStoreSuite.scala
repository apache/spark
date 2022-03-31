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

package org.apache.spark.deploy.history

import java.io.File
import java.util.NoSuchElementException
import java.util.concurrent.LinkedBlockingQueue

import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.TimeLimits
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkFunSuite
import org.apache.spark.status.KVUtils._
import org.apache.spark.tags.ExtendedLevelDBTest
import org.apache.spark.util.kvstore._

abstract class HybridStoreSuite extends SparkFunSuite with BeforeAndAfter with TimeLimits {

  var db: KVStore = _
  var dbpath: File = _

  after {
    if (db != null) {
      db.close()
    }
    if (dbpath != null) {
      FileUtils.deleteQuietly(dbpath)
    }
  }

  test("test multiple objects write read delete") {
    val store = createHybridStore()

    val t1 = createCustomType1(1)
    val t2 = createCustomType1(2)

    intercept[NoSuchElementException] {
      store.read(t1.getClass(), t1.key)
    }

    store.write(t1)
    store.write(t2)
    store.delete(t2.getClass(), t2.key)

    Seq(false, true).foreach { switch =>
      if (switch) switchHybridStore(store)

      intercept[NoSuchElementException] {
        store.read(t2.getClass(), t2.key)
      }
      assert(store.read(t1.getClass(), t1.key) === t1)
      assert(store.count(t1.getClass()) === 1L)
    }
  }

  test("test metadata") {
    val store = createHybridStore()
    assert(store.getMetadata(classOf[CustomType1]) === null)

    val t1 = createCustomType1(1)
    store.setMetadata(t1)
    assert(store.getMetadata(classOf[CustomType1]) === t1)

    // Switch to LevelDB and set a new metadata
    switchHybridStore(store)

    val t2 = createCustomType1(2)
    store.setMetadata(t2)
    assert(store.getMetadata(classOf[CustomType1]) === t2)
  }

  test("test update") {
    val store = createHybridStore()
    val t = createCustomType1(1)

    store.write(t)
    t.name = "name2"
    store.write(t)

    Seq(false, true).foreach { switch =>
      if (switch) switchHybridStore(store)

      assert(store.count(t.getClass()) === 1L)
      assert(store.read(t.getClass(), t.key) === t)
    }
  }

  test("test basic iteration") {
    val store = createHybridStore()

    val t1 = createCustomType1(1)
    store.write(t1)
    val t2 = createCustomType1(2)
    store.write(t2)

    Seq(false, true).foreach { switch =>
      if (switch) switchHybridStore(store)

      assert(store.view(t1.getClass()).iterator().next().id === t1.id)
      assert(store.view(t1.getClass()).skip(1).iterator().next().id === t2.id)
      assert(store.view(t1.getClass()).skip(1).max(1).iterator().next().id === t2.id)
      assert(store.view(t1.getClass()).first(t1.key).max(1).iterator().next().id === t1.id)
      assert(store.view(t1.getClass()).first(t2.key).max(1).iterator().next().id === t2.id)
    }
  }

  test("test delete after switch") {
    val store = createHybridStore()
    val t = createCustomType1(1)
    store.write(t)
    switchHybridStore(store)
    intercept[IllegalStateException] {
      store.delete(t.getClass(), t.key)
    }
  }

  test("test klassMap") {
    val store = createHybridStore()
    val t1 = createCustomType1(1)
    store.write(t1)
    assert(store.klassMap.size === 1)
    val t2 = new CustomType2("key2")
    store.write(t2)
    assert(store.klassMap.size === 2)

    switchHybridStore(store)
    val t3 = new CustomType3("key3")
    store.write(t3)
    // Cannot put new klass to klassMap after the switching starts
    assert(store.klassMap.size === 2)
  }

  private def createHybridStore(): HybridStore = {
    val store = new HybridStore()
    store.setDiskStore(db)
    store
  }

  private def createCustomType1(i: Int): CustomType1 = {
    new CustomType1("key" + i, "id" + i, "name" + i, i, "child" + i)
  }

  private def switchHybridStore(store: HybridStore): Unit = {
    assert(store.getStore().isInstanceOf[InMemoryStore])
    val listener = new SwitchListener()
    store.switchToDiskStore(listener, "test", None)
    failAfter(2.seconds) {
      assert(listener.waitUntilDone())
    }
    while (!store.getStore().isInstanceOf[LevelDB] && !store.getStore().isInstanceOf[RocksDB]) {
      Thread.sleep(10)
    }
  }

  private class SwitchListener extends HybridStore.SwitchToDiskStoreListener {

    // Put true to the queue when switch succeeds, and false when fails.
    private val results = new LinkedBlockingQueue[Boolean]()

    override def onSwitchToDiskStoreSuccess(): Unit = {
      try {
        results.put(true)
      } catch {
        case _: InterruptedException =>
          // no-op
      }
    }

    override def onSwitchToDiskStoreFail(e: Exception): Unit = {
      try {
        results.put(false)
      } catch {
        case _: InterruptedException =>
          // no-op
      }
    }

    def waitUntilDone(): Boolean = {
      results.take()
    }
  }
}

@ExtendedLevelDBTest
class LevelDBHybridStoreSuite extends HybridStoreSuite {
  before {
    dbpath = File.createTempFile("test.", ".ldb")
    dbpath.delete()
    db = new LevelDB(dbpath, new KVStoreScalaSerializer())
  }
}

class RocksDBHybridStoreSuite extends HybridStoreSuite {
  before {
    dbpath = File.createTempFile("test.", ".rdb")
    dbpath.delete()
    db = new RocksDB(dbpath, new KVStoreScalaSerializer())
  }
}

class CustomType1(
    @KVIndexParam var key: String,
    @KVIndexParam("id") var id: String,
    @KVIndexParam(value = "name", copy = true) var name: String,
    @KVIndexParam("int") var num: Int,
    @KVIndexParam(value = "child", parent = "id") var child: String) {

  override def equals(o: Any): Boolean = {
    o match {
      case t: CustomType1 =>
        id.equals(t.id) && name.equals(t.name)
      case _ => false
    }
  }

  override def hashCode: Int = {
    id.hashCode
  }

  override def toString: String = {
    "CustomType1[key=" + key + ",id=" + id + ",name=" + name + ",num=" + num;
  }
}

class CustomType2(@KVIndexParam var key: String) {}

class CustomType3(@KVIndexParam var key: String) {}
