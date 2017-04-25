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

import org.mockito.Matchers.{any, eq => meq}
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.status.KVUtils
import org.apache.spark.util.{ManualClock, Utils}
import org.apache.spark.util.kvstore.KVStore

class DiskStoreManagerSuite extends SparkFunSuite with BeforeAndAfter {

  import config._

  private val MAX_USAGE = 3L

  private val clock = new ManualClock()
  private var testDir: File = _
  private var store: KVStore = _
  private var manager: DiskStoreManager = _

  before {
    if (testDir != null) {
      Utils.deleteRecursively(testDir)
    }
    testDir = Utils.createTempDir()
    store = KVUtils.open(new File(testDir, "listing"), "test")

    val conf = new SparkConf()
      .set(MAX_LOCAL_DISK_USAGE, MAX_USAGE)
      .set(EVENT_TO_STORE_SIZE_RATIO, 1.0D)
    manager = spy(new DiskStoreManager(conf, testDir, store, clock))
    doReturn(0L).when(manager).sizeOf(any(classOf[File]))
  }

  after {
    store.close()
  }

  test("leasing space") {
    // Lease all available space.
    val lease1 = manager.lease(1)
    val lease2 = manager.lease(1)
    val lease3 = manager.lease(1)
    assert(!manager.hasFreeSpace(1))

    // Revert one lease, get another one.
    lease1.rollback()
    assert(manager.hasFreeSpace(1))
    assert(!lease1.path.exists())

    val lease4 = manager.lease(1)
    assert(!manager.hasFreeSpace(1))

    // Committing 2 should bring the "used" space up to 4, so there shouldn't be space left yet.
    doReturn(2L).when(manager).sizeOf(meq(lease2.path))
    val dst2 = lease2.commit("app2", None)
    assert(!manager.hasFreeSpace(1))

    // Rollback 3 and 4, now there should be 1 left.
    lease3.rollback()
    lease4.rollback()
    assert(manager.hasFreeSpace(1))
    assert(!manager.hasFreeSpace(2))

    // Release app 2 to make it available for eviction.
    doReturn(2L).when(manager).sizeOf(meq(dst2))
    manager.release("app2", None)

    // Lease 1, commit with size 3, replacing previously commited lease 2.
    val lease5 = manager.lease(1)
    doReturn(3L).when(manager).sizeOf(meq(lease5.path))
    lease5.commit("app2", None)
    assert(dst2.exists())
    assert(!lease5.path.exists())
    assert(!manager.hasFreeSpace(1))
    manager.release("app2", None)

    // Try a big lease that should cause the committed app to be evicted.
    val lease6 = manager.lease(6)
    assert(!dst2.exists())
    assert(!manager.hasFreeSpace(1))
  }

  test("tracking active stores") {
    // Lease and commit space for app 1, making it active.
    val lease1 = manager.lease(2)
    assert(!manager.hasFreeSpace(2))
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
    assert(!manager.hasFreeSpace(1))

    val lease3 = manager.lease(1)
    assert(!dst1.exists())
    lease3.rollback()

    assert(manager.openStore("app1", None).isEmpty)
  }

}
