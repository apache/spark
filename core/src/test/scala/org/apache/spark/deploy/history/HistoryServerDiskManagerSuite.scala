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

import org.mockito.AdditionalAnswers
import org.mockito.ArgumentMatchers.{anyBoolean, anyLong, eq => meq}
import org.mockito.Mockito.{doAnswer, spy}
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.History._
import org.apache.spark.internal.config.History.HybridStoreDiskBackend
import org.apache.spark.status.KVUtils
import org.apache.spark.tags.{ExtendedLevelDBTest, ExtendedRocksDBTest}
import org.apache.spark.util.{ManualClock, Utils}
import org.apache.spark.util.kvstore.KVStore

abstract class HistoryServerDiskManagerSuite extends SparkFunSuite with BeforeAndAfter {

  protected def backend: HybridStoreDiskBackend.Value

  protected def extension: String

  protected def conf: SparkConf = new SparkConf()
    .set(HYBRID_STORE_DISK_BACKEND, backend.toString)

  private def doReturn(value: Any) = org.mockito.Mockito.doReturn(value, Seq.empty: _*)

  private val MAX_USAGE = 3L

  private var testDir: File = _
  private var store: KVStore = _

  before {
    testDir = Utils.createTempDir()
    store = KVUtils.open(new File(testDir, "listing"), "test", conf)
  }

  after {
    store.close()
    if (testDir != null) {
      Utils.deleteRecursively(testDir)
    }
  }

  private def mockManager(): HistoryServerDiskManager = {
    val conf = new SparkConf().set(MAX_LOCAL_DISK_USAGE, MAX_USAGE)
    val manager = spy(new HistoryServerDiskManager(conf, testDir, store, new ManualClock()))
    doAnswer(AdditionalAnswers.returnsFirstArg[Long]()).when(manager)
      .approximateSize(anyLong(), anyBoolean())
    manager
  }

  test("leasing space") {
    val manager = mockManager()

    // Lease all available space.
    val leaseA = manager.lease(1)
    val leaseB = manager.lease(1)
    val leaseC = manager.lease(1)
    assert(manager.free() === 0)

    // Revert one lease, get another one.
    leaseA.rollback()
    assert(manager.free() > 0)
    assert(!leaseA.tmpPath.exists())

    val leaseD = manager.lease(1)
    assert(manager.free() === 0)

    // Committing B should bring the "used" space up to 4, so there shouldn't be space left yet.
    doReturn(2L).when(manager).sizeOf(meq(leaseB.tmpPath))
    val dstB = leaseB.commit("app2", None)
    assert(manager.free() === 0)
    assert(manager.committed() === 2)

    // Rollback C and D, now there should be 1 left.
    leaseC.rollback()
    leaseD.rollback()
    assert(manager.free() === 1)

    // Release app 2 to make it available for eviction.
    doReturn(2L).when(manager).sizeOf(meq(dstB))
    manager.release("app2", None)
    assert(manager.committed() === 2)

    // Emulate an updated event log by replacing the store for lease B. Lease 1, and commit with
    // size 3.
    val leaseE = manager.lease(1)
    doReturn(3L).when(manager).sizeOf(meq(leaseE.tmpPath))
    val dstE = leaseE.commit("app2", None)
    assert(dstE === dstB)
    assert(dstE.exists())
    doReturn(3L).when(manager).sizeOf(meq(dstE))
    assert(!leaseE.tmpPath.exists())
    assert(manager.free() === 0)
    manager.release("app2", None)
    assert(manager.committed() === 3)

    // Try a big lease that should cause the released app to be evicted.
    val leaseF = manager.lease(6)
    assert(!dstB.exists())
    assert(manager.free() === 0)
    assert(manager.committed() === 0)

    // Leasing when no free space is available should still be allowed.
    manager.lease(1)
    assert(manager.free() === 0)
  }

  test("tracking active stores") {
    val manager = mockManager()

    // Lease and commit space for app 1, making it active.
    val leaseA = manager.lease(2)
    assert(manager.free() === 1)
    doReturn(2L).when(manager).sizeOf(leaseA.tmpPath)
    assert(manager.openStore("appA", None).isEmpty)
    val dstA = leaseA.commit("appA", None)

    // Create a new lease. Leases are always granted, but this shouldn't cause app1's store
    // to be deleted.
    val leaseB = manager.lease(2)
    assert(dstA.exists())

    // Trying to commit on top of an active application should fail.
    intercept[IllegalArgumentException] {
      leaseB.commit("appA", None)
    }

    leaseB.rollback()

    // Close appA with an updated size, then create a new lease. Now the app's directory should be
    // deleted.
    doReturn(3L).when(manager).sizeOf(dstA)
    manager.release("appA", None)
    assert(manager.free() === 0)

    val leaseC = manager.lease(1)
    assert(!dstA.exists())
    leaseC.rollback()

    assert(manager.openStore("appA", None).isEmpty)
  }

  test("approximate size heuristic") {
    val manager = new HistoryServerDiskManager(new SparkConf(false), testDir, store,
      new ManualClock())
    assert(manager.approximateSize(50L, false) < 50L)
    assert(manager.approximateSize(50L, true) > 50L)
  }

  test("SPARK-32024: update ApplicationStoreInfo.size during initializing") {
    val manager = mockManager()
    val leaseA = manager.lease(2)
    doReturn(3L).when(manager).sizeOf(meq(leaseA.tmpPath))
    val dstPathA = manager.appStorePath("app1", None)
    doReturn(3L).when(manager).sizeOf(meq(dstPathA))
    val dstA = leaseA.commit("app1", None)
    assert(manager.free() === 0)
    assert(manager.committed() === 3)
    // Listing store tracks dstA now.
    assert(store.read(classOf[ApplicationStoreInfo], dstA.getAbsolutePath).size === 3)

    // Simulate: service restarts, new disk manager (manager1) is initialized.
    val manager1 = mockManager()
    // Simulate: event KVstore compaction before restart, directory size reduces.
    doReturn(2L).when(manager1).sizeOf(meq(dstA))
    doReturn(2L).when(manager1).sizeOf(meq(new File(testDir, "apps")))
    manager1.initialize()
    // "ApplicationStoreInfo.size" is updated for dstA.
    assert(store.read(classOf[ApplicationStoreInfo], dstA.getAbsolutePath).size === 2)
    assert(manager1.free() === 1)
    // If "ApplicationStoreInfo.size" is not correctly updated, "IllegalStateException"
    // would be thrown.
    val leaseB = manager1.lease(2)
    assert(manager1.free() === 1)
    doReturn(2L).when(manager1).sizeOf(meq(leaseB.tmpPath))
    val dstPathB = manager.appStorePath("app2", None)
    doReturn(2L).when(manager1).sizeOf(meq(dstPathB))
    val dstB = leaseB.commit("app2", None)
    assert(manager1.committed() === 2)
    // Listing store tracks dstB only, dstA is evicted by "makeRoom()".
    assert(store.read(classOf[ApplicationStoreInfo], dstB.getAbsolutePath).size === 2)

    val manager2 = mockManager()
    // Simulate: cache entities are written after replaying, directory size increases.
    doReturn(3L).when(manager2).sizeOf(meq(dstB))
    doReturn(3L).when(manager2).sizeOf(meq(new File(testDir, "apps")))
    manager2.initialize()
    // "ApplicationStoreInfo.size" is updated for dstB.
    assert(store.read(classOf[ApplicationStoreInfo], dstB.getAbsolutePath).size === 3)
    assert(manager2.free() === 0)
    val leaseC = manager2.lease(2)
    doReturn(2L).when(manager2).sizeOf(meq(leaseC.tmpPath))
    val dstPathC = manager.appStorePath("app3", None)
    doReturn(2L).when(manager2).sizeOf(meq(dstPathC))
    val dstC = leaseC.commit("app3", None)
    assert(manager2.free() === 1)
    assert(manager2.committed() === 2)
    // Listing store tracks dstC only, dstB is evicted by "makeRoom()".
    assert(store.read(classOf[ApplicationStoreInfo], dstC.getAbsolutePath).size === 2)
  }

  test("SPARK-38095: appStorePath should use backend extensions") {
    val conf = new SparkConf().set(HYBRID_STORE_DISK_BACKEND, backend.toString)
    val manager = new HistoryServerDiskManager(conf, testDir, store, new ManualClock())
    assert(manager.appStorePath("appId", None).getName.endsWith(extension))
  }
}

@ExtendedLevelDBTest
class HistoryServerDiskManagerUseLevelDBSuite extends HistoryServerDiskManagerSuite {
  override protected def backend: HybridStoreDiskBackend.Value = HybridStoreDiskBackend.LEVELDB
  override protected def extension: String = ".ldb"
}

@ExtendedRocksDBTest
class HistoryServerDiskManagerUseRocksDBSuite extends HistoryServerDiskManagerSuite {
  override protected def backend: HybridStoreDiskBackend.Value = HybridStoreDiskBackend.ROCKSDB
  override protected def extension: String = ".rdb"
}
