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
    assert(!store.hasUncommittedUpdates)
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
    store.prepareForUpdates(0)
    intercept[IllegalStateException] {
      store.getAll()
    }
    assert(store.hasUncommittedUpdates)
    update(store, "a", 1)
    intercept[IllegalStateException] {
      store.getAll()
    }

    // Make updates and commit
    update(store, "b", 2)
    update(store, "aa", 3)
    remove(store, _.startsWith("a"))
    store.commitUpdates()

    // Verify state after committing
    assert(!store.hasUncommittedUpdates)
    assert(getData(store) === Set("b" -> 2))
    assert(fileExists(store, 0, isSnapshot = false))

    // Trying to get newer versions should fail
    intercept[Exception] {
      getData(store, 1)
    }

    intercept[Exception] {
      getDataFromFiles(store, 1)
    }

    // Reload store from the directory
    val reloadedStore = new StateStore(store.id, store.directory)
    assert(getData(reloadedStore) === Set("b" -> 2))

    // New updates to the reload store with new version, and does not change old version
    reloadedStore.prepareForUpdates(1)
    update(reloadedStore, "c", 4)
    reloadedStore.commitUpdates()
    assert(getData(reloadedStore) === Set("b" -> 2, "c" -> 4))
    assert(getData(reloadedStore, version = 0) === Set("b" -> 2))
    assert(getData(reloadedStore, version = 1) === Set("b" -> 2, "c" -> 4))
    assert(fileExists(reloadedStore, 1, isSnapshot = false))
  }

  test("cancelUpdates") {
    val store = newStore()
    store.prepareForUpdates(0)
    update(store, "a", 1)
    store.commitUpdates()
    assert(getData(store) === Set("a" -> 1))

    // cancelUpdates should not change the data
    store.prepareForUpdates(1)
    update(store, "b", 1)
    store.cancelUpdates()
    assert(getData(store) === Set("a" -> 1))

    // Calling startUpdates again should cancel previous updates
    store.prepareForUpdates(1)
    update(store, "b", 1)
    store.prepareForUpdates(1)
    update(store, "c", 1)
    store.commitUpdates()
    assert(getData(store) === Set("a" -> 1, "c" -> 1))
  }

  test("startUpdates with unexpected versions") {
    val store = newStore()

    intercept[IllegalArgumentException] {
      store.prepareForUpdates(-1)
    }

    // Prepare some data in the stoer
    store.prepareForUpdates(0)
    update(store, "a", 1)
    store.commitUpdates()
    assert(getData(store) === Set("a" -> 1))

    intercept[IllegalStateException] {
      store.prepareForUpdates(2)
    }

    // Update store version with some data
    store.prepareForUpdates(1)
    update(store, "b", 1)
    store.commitUpdates()
    assert(getData(store) === Set("a" -> 1, "b" -> 1))

    assert(getDataFromFiles(store) === Set("a" -> 1, "b" -> 1))

    // Overwrite the version with other data
    store.prepareForUpdates(1)
    update(store, "c", 1)
    store.commitUpdates()
    assert(getData(store) === Set("a" -> 1, "c" -> 1))
    assert(getDataFromFiles(store) === Set("a" -> 1, "c" -> 1))
  }

  test("snapshotting") {
    val store = newStore(maxDeltaChainForSnapshots = 5)

    var currentVersion = -1
    def updateVersionTo(targetVersion: Int): Unit = {
      for (i <- currentVersion + 1 to targetVersion) {
        store.prepareForUpdates(i)
        update(store, "a", i)
        store.commitUpdates()
      }
    }

    updateVersionTo(2)
    require(getData(store) === Set("a" -> 2))
    store.manageFiles()
    assert(getDataFromFiles(store) === Set("a" -> 2))
    for (i <- 0 to 2) {
      assert(fileExists(store, i, isSnapshot = false))  // all delta files present
      assert(!fileExists(store, i, isSnapshot = true))  // no snapshot files present
    }

    // After version 6, snapshotting should generate one snapshot file
    updateVersionTo(6)
    require(getData(store) === Set("a" -> 6), "Store not updated correctly")
    store.manageFiles()       // do snapshot
    assert(getData(store) === Set("a" -> 6), "manageFiles() messed up the data")
    assert(getDataFromFiles(store) === Set("a" -> 6))

    val snapshotVersion = (0 to 6).find(version => fileExists(store, version, isSnapshot = true))
    assert(snapshotVersion.nonEmpty, "Snapshot file not generated")


    // After version 20, snapshotting should generate newer snapshot files
    updateVersionTo(20)
    require(getData(store) === Set("a" -> 20), "Store not updated correctly")
    store.manageFiles()       // do snapshot
    assert(getData(store) === Set("a" -> 20), "manageFiles() messed up the data")
    assert(getDataFromFiles(store) === Set("a" -> 20))

    val latestSnapshotVersion = (0 to 20).filter(version =>
      fileExists(store, version, isSnapshot = true)).lastOption
    assert(latestSnapshotVersion.nonEmpty, "No snapshot file found")
    assert(latestSnapshotVersion.get > snapshotVersion.get, "Newer snapshot not generated")

  }

  test("cleaning") {
    val store = newStore(maxDeltaChainForSnapshots = 5)

    for (i <- 0 to 20) {
      store.prepareForUpdates(i)
      update(store, "a", i)
      store.commitUpdates()
    }
    require(getData(store) === Set("a" -> 20), "Store not updated correctly")
    store.manageFiles()     // do cleanup
    assert(fileExists(store, 0, isSnapshot = false))

    assert(getDataFromFiles(store, 20) === Set("a" -> 20))
    assert(getDataFromFiles(store, 19) === Set("a" -> 19))
  }

  def getData(store: StateStore, version: Int = -1): Set[(String, Int)] = {
    if (version < 0) {
      store.getAll.map(unwrapKeyValue).toSet
    } else {
      store.getAll(version).map(unwrapKeyValue).toSet
    }

  }

  def getDataFromFiles(store: StateStore, version: Int = -1): Set[(String, Int)] = {
    getData(new StateStore(store.id, store.directory), version)
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

  def newStore(
      opId: Long = Random.nextLong,
      partition: Int = 0,
      maxDeltaChainForSnapshots: Int = 10
    ): StateStore = {
    new StateStore(
      StateStoreId(opId, partition),
      Utils.createDirectory(tempDir, Random.nextString(5)).toString,
      maxDeltaChainForSnapshots = maxDeltaChainForSnapshots)
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
