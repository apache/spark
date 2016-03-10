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

package org.apache.spark.sql.execution.streaming.state

import java.io.File

import scala.collection.mutable
import scala.util.Random

import org.apache.hadoop.fs.Path
import org.scalatest.{PrivateMethodTester, BeforeAndAfter}

import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.util.Utils

class StateStoreSuite extends SparkFunSuite with BeforeAndAfter with PrivateMethodTester {
  type MapType = mutable.HashMap[InternalRow, InternalRow]

  private val tempDir = Utils.createTempDir().toString

  after {
    StateStore.clearAll()
  }

  test("startUpdates, update, remove, commitUpdates") {
    val store = newStore()

    // Verify state before starting a new set of updates
    assert(store.getAll().isEmpty)
    assert(store.getInternalMap(0).isEmpty)
    intercept[IllegalStateException] {
      store.update(null, null)
    }
    intercept[IllegalStateException] {
      store.remove(_ => true)
    }
    intercept[IllegalStateException] {
      store.commitUpdates()
    }
    intercept[IllegalStateException] {
      store.cancelUpdates()
    }

    // Verify states after starting updates
    store.startUpdates(0)
    intercept[IllegalStateException] {
      store.getAll()
    }
    update(store, "a", 1)
    intercept[IllegalStateException] {
      store.getAll()
    }

    // Make updates and commit
    update(store, "b", 2)
    update(store, "aa", 3)
    remove(store, _.startsWith("a"))
    store.commitUpdates()

    // Very state after committing
    assert(getData(store) === Set("b" -> 2))
    assertMap(store.getInternalMap(0), Map("b" -> 2))
    assert(fileExists(store, 0, isSnapshot = false))
    assert(store.getInternalMap(1).isEmpty)

    // Reload store from the directory
    val reloadedStore = new StateStore(store.id, store.directory)
    assert(getData(reloadedStore) === Set("b" -> 2))

    // New updates to the reload store with new version, and does not change old version
    reloadedStore.startUpdates(1)
    update(reloadedStore, "c", 4)
    reloadedStore.commitUpdates()
    assert(getData(reloadedStore) === Set("b" -> 2, "c" -> 4))
    assertMap(reloadedStore.getInternalMap(0), Map("b" -> 2))
    assertMap(reloadedStore.getInternalMap(1), Map("b" -> 2, "c" -> 4))
    assert(fileExists(reloadedStore, 1, isSnapshot = false))
  }

  test("cancelUpdates") {
    val store = newStore()
    store.startUpdates(0)
    update(store, "a", 1)
    store.commitUpdates()
    assert(getData(store) === Set("a" -> 1))

    // cancelUpdates should not change the data
    store.startUpdates(1)
    update(store, "b", 1)
    store.cancelUpdates()
    assert(getData(store) === Set("a" -> 1))

    // Calling startUpdates again should cancel previous updates
    store.startUpdates(1)
    update(store, "b", 1)
    store.startUpdates(1)
    update(store, "c", 1)
    store.commitUpdates()
    assert(getData(store) === Set("a" -> 1, "c" -> 1))
  }

  test("startUpdates with unexpected versions") {
    val store = newStore()

    intercept[IllegalArgumentException] {
      store.startUpdates(-1)
    }

    // Prepare some data in the stoer
    store.startUpdates(0)
    update(store, "a", 1)
    store.commitUpdates()
    assert(getData(store) === Set("a" -> 1))

    intercept[IllegalStateException] {
      store.startUpdates(2)
    }

    // Update store version with some data
    println("here")
    store.startUpdates(1)
    update(store, "b", 1)
    store.commitUpdates()
    println("xyz")
    assert(getData(store) === Set("a" -> 1, "b" -> 1))
    println("bla")

    assert(getData(new StateStore(store.id, store.directory)) === Set("a" -> 1, "b" -> 1))

    // Overwrite the version with other data
    store.startUpdates(1)
    update(store, "c", 1)
    store.commitUpdates()
    assert(getData(store) === Set("a" -> 1, "c" -> 1))
    assert(getData(new StateStore(store.id, store.directory)) === Set("a" -> 1, "c" -> 1))
  }

  def getData(store: StateStore): Set[(String, Int)] = {
    store.getAll.map(unwrapKeyValue).toSet
  }

  def assertMap(
    testMapOption: Option[MapType],
    expectedMap: Map[String, Int]): Unit = {
    assert(testMapOption.nonEmpty, "no map present")
    val convertedMap = testMapOption.get.map(unwrapKeyValue)
    assert(convertedMap === expectedMap)
  }

  def fileExists(store: StateStore, version: Long, isSnapshot: Boolean): Boolean = {
    val method = PrivateMethod[Path]('baseDir)
    val basePath = store invokePrivate method()
    val fileName = if (isSnapshot) s"$version.snapshot" else s"$version.delta"
    val filePath = new File(basePath.toString, fileName)
    filePath.exists
  }

  def storeLoaded(storeId: StateStoreId): Boolean = {
    val method = PrivateMethod[mutable.HashMap[StateStoreId, StateStore]]('loadedStores)
    val loadedStores = StateStore invokePrivate method()
    loadedStores.contains(storeId)
  }

  def unloadStore(storeId: StateStoreId): Boolean = {
    val method = PrivateMethod('remove)
    StateStore invokePrivate method(storeId)
  }

  def newStore(opId: Long = Random.nextLong, partition: Int = 0): StateStore = {
    new StateStore(
      StateStoreId(opId, partition),
      Utils.createDirectory(tempDir, Random.nextString(5)).toString)
  }

  def remove(store: StateStore, condition: String => Boolean): Unit = {
    store.remove(row => condition(unwrapKey(row)))
  }

  private def update(store: StateStore, key: String, value: Int): Unit = {
    store.update(wrapKey(key), _ => wrapValue(value))
  }

  private def increment(store: StateStore, key: String): Unit = {
    val keyRow = new GenericInternalRow(Array(key).asInstanceOf[Array[Any]])
    store.update(keyRow, oldRow => {
      val oldValue = oldRow.map(unwrapValue).getOrElse(0)
      wrapValue(oldValue + 1)
    })
  }

  private def wrapValue(i: Int): InternalRow = {
    new GenericInternalRow(Array[Any](i))
  }

  private def wrapKey(s: String): InternalRow = {
    new GenericInternalRow(Array[Any](UTF8String.fromString(s)))
  }

  private def unwrapKey(row: InternalRow): String = {
    row.asInstanceOf[GenericInternalRow].getString(0)
  }

  private def unwrapValue(row: InternalRow): Int = {
    row.asInstanceOf[GenericInternalRow].getInt(0)
  }

  private def unwrapKeyValue(row: (InternalRow, InternalRow)): (String, Int) = {
    (unwrapKey(row._1), unwrapValue(row._2))
  }

  private def unwrapKeyValue(row: InternalRow): (String, Int) = {
    (row.getString(0), row.getInt(1))
  }
}
