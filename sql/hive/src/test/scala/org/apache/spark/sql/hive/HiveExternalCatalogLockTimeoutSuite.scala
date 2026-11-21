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

package org.apache.spark.sql.hive

import java.util.concurrent.{CountDownLatch, TimeoutException, TimeUnit}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

import org.apache.hadoop.conf.Configuration
import org.mockito.Mockito.{mock, when}
import org.mockito.invocation.InvocationOnMock

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.util.ThreadUtils

/**
 * Tests of `spark.sql.hive.externalCatalog.lockTimeoutMs` (SPARK-32118): a catalog operation must
 * not wait indefinitely behind another thread whose metastore call is stuck.
 */
class HiveExternalCatalogLockTimeoutSuite extends SparkFunSuite {

  private val stuckDb = "stuck"
  private val otherDb = "other"

  /**
   * A catalog whose client blocks in `databaseExists(stuckDb)`, holding the catalog lock, until
   * `release` is counted down. `entered` tells when the blocked call is inside the lock.
   */
  private class Fixture(lockTimeout: Option[String]) {
    val entered = new CountDownLatch(1)
    val release = new CountDownLatch(1)

    private val mockClient = mock(classOf[HiveClient])
    when(mockClient.databaseExists(stuckDb)).thenAnswer((_: InvocationOnMock) => {
      entered.countDown()
      release.await()
      true
    })
    when(mockClient.databaseExists(otherDb)).thenReturn(true)

    private val conf = new SparkConf(false)
    lockTimeout.foreach(conf.set(HiveUtils.HIVE_EXTERNAL_CATALOG_LOCK_TIMEOUT.key, _))

    val catalog: HiveExternalCatalog = new HiveExternalCatalog(conf, new Configuration) {
      override lazy val client: HiveClient = mockClient
    }
  }

  private def withFixture(lockTimeout: Option[String])(body: (Fixture, ExecutionContext) => Unit)
    : Unit = {
    val fixture = new Fixture(lockTimeout)
    val pool = ThreadUtils.newDaemonFixedThreadPool(2, "catalog-lock-timeout-test")
    try {
      body(fixture, ExecutionContext.fromExecutorService(pool))
    } finally {
      fixture.release.countDown()
      pool.shutdownNow()
    }
  }

  test("SPARK-32118: an operation fails after the lock timeout instead of waiting for a " +
    "stuck one") {
    withFixture(Some("200ms")) { (fixture, ec) =>
      val stuck = Future(fixture.catalog.databaseExists(stuckDb))(ec)
      assert(fixture.entered.await(30, TimeUnit.SECONDS))

      val start = System.nanoTime()
      val e = intercept[AnalysisException] {
        fixture.catalog.databaseExists(otherDb)
      }
      val elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)
      assert(e.getCause.isInstanceOf[TimeoutException])
      assert(e.getMessage.contains(HiveUtils.HIVE_EXTERNAL_CATALOG_LOCK_TIMEOUT.key))
      assert(elapsedMs >= 200, s"failed after $elapsedMs ms, before the timeout")
      // The stuck call is still blocked: the failure came from the timeout, not from it ending.
      assert(!stuck.isCompleted)

      // Once the stuck call ends, the catalog works again.
      fixture.release.countDown()
      assert(ThreadUtils.awaitResult(stuck, 30.seconds))
      assert(fixture.catalog.databaseExists(otherDb))
    }
  }

  test("SPARK-32118: without a lock timeout an operation waits for the stuck one") {
    withFixture(None) { (fixture, ec) =>
      val stuck = Future(fixture.catalog.databaseExists(stuckDb))(ec)
      assert(fixture.entered.await(30, TimeUnit.SECONDS))

      val waiting = Future(fixture.catalog.databaseExists(otherDb))(ec)
      intercept[TimeoutException] {
        ThreadUtils.awaitResult(waiting, 500.millis)
      }

      fixture.release.countDown()
      assert(ThreadUtils.awaitResult(stuck, 30.seconds))
      assert(ThreadUtils.awaitResult(waiting, 30.seconds))
    }
  }

  test("SPARK-32118: the lock timeout does not affect nested operations of the same thread") {
    withFixture(Some("200ms")) { (fixture, _) =>
      // The client call runs inside the catalog lock and re-enters the catalog.
      val client = fixture.catalog.client
      when(client.databaseExists("outer")).thenAnswer((_: InvocationOnMock) =>
        fixture.catalog.databaseExists(otherDb))
      assert(fixture.catalog.databaseExists("outer"))
    }
  }
}
