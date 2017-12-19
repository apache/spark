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

import org.mockito.Matchers.{any, anyBoolean, anyLong, eq => meq}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.status.KVUtils
import org.apache.spark.util.{ManualClock, Utils}
import org.apache.spark.util.kvstore.KVStore

class DiskStoreManagerSuite extends SparkFunSuite with BeforeAndAfter {

  import config._

  private val MAX_USAGE = 3L

  private var testDir: File = _
  private var store: KVStore = _

  before {
    testDir = Utils.createTempDir()
    store = KVUtils.open(new File(testDir, "listing"), "test")
  }

  after {
    store.close()
    if (testDir != null) {
      Utils.deleteRecursively(testDir)
    }
  }

  private def mockManager(): DiskStoreManager = {
    val conf = new SparkConf().set(MAX_LOCAL_DISK_USAGE, MAX_USAGE)
    val manager = spy(new DiskStoreManager(conf, testDir, store, new ManualClock()))
    doReturn(0L).when(manager).sizeOf(any(classOf[File]))
    doAnswer(new Answer[Long] {
      def answer(invocation: InvocationOnMock): Long = {
        invocation.getArguments()(0).asInstanceOf[Long]
      }
    }).when(manager).approximateSize(anyLong(), anyBoolean())
    manager
  }

  private def hasFreeSpace(manager: DiskStoreManager, size: Long): Boolean = {
    size <= manager.free()
  }

  test("leasing space") {
    val manager = mockManager()

    // Lease all available space.
    val lease1 = manager.lease(1)
    val lease2 = manager.lease(1)
    val lease3 = manager.lease(1)
    assert(!hasFreeSpace(manager, 1))

    // Revert one lease, get another one.
    lease1.rollback()
    assert(hasFreeSpace(manager, 1))
    assert(!lease1.path.exists())

    val lease4 = manager.lease(1)
    assert(!hasFreeSpace(manager, 1))

    // Committing 2 should bring the "used" space up to 4, so there shouldn't be space left yet.
    doReturn(2L).when(manager).sizeOf(meq(lease2.path))
    val dst2 = lease2.commit("app2", None)
    assert(!hasFreeSpace(manager, 1))

    // Rollback 3 and 4, now there should be 1 left.
    lease3.rollback()
    lease4.rollback()
    assert(hasFreeSpace(manager, 1))
    assert(!hasFreeSpace(manager, 2))

    // Release app 2 to make it available for eviction.
    doReturn(2L).when(manager).sizeOf(meq(dst2))
    manager.release("app2", None)

    // Lease 1, commit with size 3, replacing previously commited lease 2.
    val lease5 = manager.lease(1)
    doReturn(3L).when(manager).sizeOf(meq(lease5.path))
    lease5.commit("app2", None)
    assert(dst2.exists())
    assert(!lease5.path.exists())
    assert(!hasFreeSpace(manager, 1))
    manager.release("app2", None)

    // Try a big lease that should cause the committed app to be evicted.
    val lease6 = manager.lease(6)
    assert(!dst2.exists())
    assert(!hasFreeSpace(manager, 1))
  }

  test("tracking active stores") {
    val manager = mockManager()

    // Lease and commit space for app 1, making it active.
    val lease1 = manager.lease(2)
    assert(!hasFreeSpace(manager, 2))
    doReturn(2L).when(manager).sizeOf(lease1.path)
    assert(manager.openStore("app1", None).isEmpty)
    val dst1 = lease1.commit("app1", None)

    // Create a new lease. Leases are always granted, but this shouldn't cause app1's store
    // to be deleted.
    val lease2 = manager.lease(2)
    assert(dst1.exists())

    // Trying to commit on top of an active application should fail.
    intercept[IllegalArgumentException] {
      lease2.commit("app1", None)
    }

    lease2.rollback()

    // Close app1 with an updated size, then create a new lease. Now the app's directory should be
    // deleted.
    doReturn(3L).when(manager).sizeOf(dst1)
    manager.release("app1", None)
    assert(!hasFreeSpace(manager, 1))

    val lease3 = manager.lease(1)
    assert(!dst1.exists())
    lease3.rollback()

    assert(manager.openStore("app1", None).isEmpty)
  }

  test("approximate size heuristic") {
    val conf = new SparkConf().set(MAX_LOCAL_DISK_USAGE, 1024L)
    val manager = new DiskStoreManager(conf, testDir, store, new ManualClock())

    assert(manager.approximateSize(50L, false) < 50L)
    assert(manager.approximateSize(50L, true) > 50L)
    assert(manager.approximateSize(1024L, false) <= 1024L / 10)
  }

}
