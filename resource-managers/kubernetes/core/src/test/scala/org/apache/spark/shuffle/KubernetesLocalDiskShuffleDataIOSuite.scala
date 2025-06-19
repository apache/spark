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

package org.apache.spark.shuffle

import java.io.File
import java.nio.file.Files

import scala.concurrent.duration._

import org.mockito.Mockito.{mock, when}
import org.scalatest.concurrent.Eventually.{eventually, interval, timeout}

import org.apache.spark.{LocalRootDirsTest, MapOutputTrackerMaster, SparkContext, SparkFunSuite, TestUtils}
import org.apache.spark.LocalSparkContext.withSpark
import org.apache.spark.deploy.k8s.Config.KUBERNETES_DRIVER_REUSE_PVC
import org.apache.spark.internal.config._
import org.apache.spark.scheduler.cluster.StandaloneSchedulerBackend
import org.apache.spark.storage.BlockManager

class KubernetesLocalDiskShuffleDataIOSuite extends SparkFunSuite with LocalRootDirsTest {

  override def beforeAll(): Unit = {
    super.beforeAll()
    conf
      .setAppName("ShuffleExecutorComponentsSuite")
      .setMaster("local-cluster[1,1,1024]")
      .set(UI.UI_ENABLED, false)
      .set(DYN_ALLOCATION_ENABLED, true)
      .set(DYN_ALLOCATION_SHUFFLE_TRACKING_ENABLED, true)
      .set(DYN_ALLOCATION_INITIAL_EXECUTORS, 1)
      .set(DYN_ALLOCATION_MIN_EXECUTORS, 1)
      .set(IO_ENCRYPTION_ENABLED, false)
      .set(KUBERNETES_DRIVER_REUSE_PVC, true)
      .set(SHUFFLE_IO_PLUGIN_CLASS, classOf[KubernetesLocalDiskShuffleDataIO].getName)
  }

  test("recompute is not blocked by the recovery") {
    sc = new SparkContext(conf)
    withSpark(sc) { sc =>
      val master = sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
      assert(master.shuffleStatuses.isEmpty)

      val rdd = sc.parallelize(Seq((1, "one"), (2, "two"), (3, "three")), 3)
        .groupByKey()
      rdd.collect()
      assert(master.shuffleStatuses.keys.toSet == Set(0))
      val loc1 = master.shuffleStatuses(0).mapStatuses(0).location
      assert(master.shuffleStatuses(0).mapStatuses.map(_.mapId).toSet == Set(0, 1, 2))

      // Reuse the existing shuffle data
      rdd.collect()
      assert(master.shuffleStatuses.keys.toSet == Set(0))
      assert(master.shuffleStatuses(0).mapStatuses.map(_.mapId).toSet == Set(0, 1, 2))

      // Decommission all executors
      val sched = sc.schedulerBackend.asInstanceOf[StandaloneSchedulerBackend]
      sc.getExecutorIds().foreach { id =>
        sched.killExecutor(id)
      }
      TestUtils.waitUntilExecutorsUp(sc, 1, 60000)
      // Shuffle status are removed
      eventually(timeout(60.second), interval(1.seconds)) {
        assert(master.shuffleStatuses.keys.toSet == Set(0))
        assert(master.shuffleStatuses(0).mapStatuses.forall(_ == null))
      }

      rdd.collect()
      assert(master.shuffleStatuses.keys.toSet == Set(0))
      assert(master.shuffleStatuses(0).mapStatuses.map(_.mapId).toSet == Set(9, 10, 11))
    }
  }

  test("Partial recompute shuffle data") {
    sc = new SparkContext(conf)
    withSpark(sc) { sc =>
      val master = sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
      assert(master.shuffleStatuses.isEmpty)

      val rdd = sc.parallelize(Seq((1, "one"), (2, "two"), (3, "three")), 3).groupByKey()
      rdd.collect()
      assert(master.shuffleStatuses.keys.toSet == Set(0))
      val loc1 = master.shuffleStatuses(0).mapStatuses(0).location
      assert(master.shuffleStatuses(0).mapStatuses.forall(_.location == loc1))
      assert(master.shuffleStatuses(0).mapStatuses.map(_.mapId).toSet == Set(0, 1, 2))

      // Reuse the existing shuffle data
      rdd.collect()
      assert(master.shuffleStatuses.keys.toSet == Set(0))
      assert(master.shuffleStatuses(0).mapStatuses.forall(_.location == loc1))
      assert(master.shuffleStatuses(0).mapStatuses.map(_.mapId).toSet == Set(0, 1, 2))

      val rdd2 = sc.parallelize(Seq((4, "four"), (5, "five"), (6, "six"), (7, "seven")), 4)
        .groupByKey()
      rdd2.collect()
      assert(master.shuffleStatuses.keys.toSet == Set(0, 1))
      assert(master.shuffleStatuses(0).mapStatuses.forall(_.location == loc1))
      assert(master.shuffleStatuses(1).mapStatuses.forall(_.location == loc1))
      assert(master.shuffleStatuses(0).mapStatuses.map(_.mapId).toSet == Set(0, 1, 2))
      assert(master.shuffleStatuses(1).mapStatuses.map(_.mapId).toSet == Set(9, 10, 11, 12))

      // Decommission all executors
      val sched = sc.schedulerBackend.asInstanceOf[StandaloneSchedulerBackend]
      sc.getExecutorIds().foreach { id =>
        sched.killExecutor(id)
      }
      TestUtils.waitUntilExecutorsUp(sc, 1, 60000)
      // Shuffle status are removed
      eventually(timeout(60.second), interval(1.seconds)) {
        assert(master.shuffleStatuses.keys.toSet == Set(0, 1))
        assert(master.shuffleStatuses(0).mapStatuses.forall(_ == null))
        assert(master.shuffleStatuses(1).mapStatuses.forall(_ == null))
      }

      rdd.collect()
      assert(master.shuffleStatuses.keys.toSet == Set(0, 1))
      assert(master.shuffleStatuses(0).mapStatuses.map(_.mapId).toSet == Set(17, 18, 19))
      assert(master.shuffleStatuses(1).mapStatuses.map(_.mapId).toSet == Set(9, 10, 11, 12))

      rdd2.collect()
      assert(master.shuffleStatuses(1).mapStatuses.map(_.mapId).toSet == Set(9, 10, 11, 12))
    }
  }

  test("A new rdd and full recovery of old data") {
    sc = new SparkContext(conf)
    withSpark(sc) { sc =>
      val master = sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
      assert(master.shuffleStatuses.isEmpty)

      val rdd = sc.parallelize(Seq((1, "one"), (2, "two"), (3, "three")), 3)
        .groupByKey()
      rdd.collect()
      assert(master.shuffleStatuses.keys.toSet == Set(0))
      val loc1 = master.shuffleStatuses(0).mapStatuses(0).location
      assert(master.shuffleStatuses(0).mapStatuses.forall(_.location == loc1))
      assert(master.shuffleStatuses(0).mapStatuses.map(_.mapId).toSet == Set(0, 1, 2))

      // Reuse the existing shuffle data
      rdd.collect()
      assert(master.shuffleStatuses.keys.toSet == Set(0))
      assert(master.shuffleStatuses(0).mapStatuses.forall(_.location == loc1))
      assert(master.shuffleStatuses(0).mapStatuses.map(_.mapId).toSet == Set(0, 1, 2))

      val rdd2 = sc.parallelize(Seq((4, "four"), (5, "five"), (6, "six"), (7, "seven")), 4)
        .groupByKey()
      rdd2.collect()
      assert(master.shuffleStatuses.keys.toSet == Set(0, 1))
      assert(master.shuffleStatuses(0).mapStatuses.forall(_.location == loc1))
      assert(master.shuffleStatuses(1).mapStatuses.forall(_.location == loc1))
      assert(master.shuffleStatuses(0).mapStatuses.map(_.mapId).toSet == Set(0, 1, 2))
      assert(master.shuffleStatuses(1).mapStatuses.map(_.mapId).toSet == Set(9, 10, 11, 12))

      // Decommission all executors
      val sched = sc.schedulerBackend.asInstanceOf[StandaloneSchedulerBackend]
      sc.getExecutorIds().foreach { id =>
        sched.killExecutor(id)
      }
      TestUtils.waitUntilExecutorsUp(sc, 1, 60000)
      // Shuffle status are removed
      eventually(timeout(60.second), interval(1.seconds)) {
        assert(master.shuffleStatuses.keys.toSet == Set(0, 1))
        assert(master.shuffleStatuses(0).mapStatuses.forall(_ == null))
        assert(master.shuffleStatuses(1).mapStatuses.forall(_ == null))
      }
      sc.parallelize(Seq((1, 1), (1, 2), (1, 3), (2, 1)), 2).groupByKey().collect()
      eventually(timeout(60.second), interval(1.seconds)) {
        assert(master.shuffleStatuses(0).mapStatuses.map(_.mapId).toSet == Set(0, 1, 2))
        assert(master.shuffleStatuses(1).mapStatuses.map(_.mapId).toSet == Set(9, 10, 11, 12))
      }
      rdd.count()
      rdd2.count()
      assert(master.shuffleStatuses(0).mapStatuses.map(_.mapId).toSet == Set(0, 1, 2))
      assert(master.shuffleStatuses(1).mapStatuses.map(_.mapId).toSet == Set(9, 10, 11, 12))
    }
  }

  test("Multi stages") {
    sc = new SparkContext(conf)
    withSpark(sc) { sc =>
      val master = sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
      assert(master.shuffleStatuses.isEmpty)

      val rdd1 = sc.parallelize(1 to 10, 3)
      val rdd2 = rdd1.map(x => (x % 2, 1))
      val rdd3 = rdd2.reduceByKey(_ + _)
      val rdd4 = rdd3.sortByKey()

      assert(rdd4.count() === 2)
      assert(master.shuffleStatuses.keys.toSet == Set(0, 1))
      assert(master.shuffleStatuses(0).mapStatuses.map(_.mapId).toSet == Set(0, 1, 2))
      assert(master.shuffleStatuses(1).mapStatuses.map(_.mapId).toSet == Set(6, 7, 8))

      // Kill all executors
      val sched = sc.schedulerBackend.asInstanceOf[StandaloneSchedulerBackend]
      sc.getExecutorIds().foreach { id =>
        sched.killExecutor(id)
      }
      TestUtils.waitUntilExecutorsUp(sc, 1, 60000)
      // Shuffle status are removed
      eventually(timeout(10.second), interval(1.seconds)) {
        assert(master.shuffleStatuses.keys.toSet == Set(0, 1))
        assert(master.shuffleStatuses(0).mapStatuses.forall(_ == null))
        assert(master.shuffleStatuses(1).mapStatuses.forall(_ == null))
      }
      sc.parallelize(Seq((1, 1)), 2).groupByKey().collect()
      eventually(timeout(60.second), interval(1.seconds)) {
        assert(master.shuffleStatuses(0).mapStatuses.map(_.mapId).toSet == Set(0, 1, 2))
        assert(master.shuffleStatuses(1).mapStatuses.map(_.mapId).toSet == Set(6, 7, 8))
      }
    }
  }

  test("SPARK-44501: Ignore checksum files") {
    val sparkConf = conf.clone.set("spark.local.dir",
      conf.get("spark.local.dir") + "/spark-x/executor-y")
    val dir = sparkConf.get("spark.local.dir") + "/blockmgr-z/00"
    Files.createDirectories(new File(dir).toPath())
    Seq("ADLER32", "CRC32", "CRC32C").foreach { algorithm =>
      new File(dir, s"1.checksum.$algorithm").createNewFile()
    }

    val bm = mock(classOf[BlockManager])
    when(bm.TempFileBasedBlockStoreUpdater).thenAnswer(_ => throw new Exception())
    KubernetesLocalDiskShuffleExecutorComponents.recoverDiskStore(sparkConf, bm)
  }

  test("SPARK-44534: Handle only shuffle files") {
    val sparkConf = conf.clone.set("spark.local.dir",
      conf.get("spark.local.dir") + "/spark-x/executor-y")
    val dir = sparkConf.get("spark.local.dir") + "/blockmgr-z/00"
    Files.createDirectories(new File(dir).toPath())
    Seq("broadcast_0", "broadcast_0_piece0", "temp_shuffle_uuid").foreach { f =>
      new File(dir, f).createNewFile()
    }

    val bm = mock(classOf[BlockManager])
    when(bm.TempFileBasedBlockStoreUpdater).thenAnswer(_ => throw new Exception())
    KubernetesLocalDiskShuffleExecutorComponents.recoverDiskStore(sparkConf, bm)
  }
}
