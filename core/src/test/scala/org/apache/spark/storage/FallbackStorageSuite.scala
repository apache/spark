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
package org.apache.spark.storage

import java.io.{DataOutputStream, File, FileOutputStream, IOException}
import java.nio.file.Files

import scala.concurrent.duration._

import org.apache.hadoop.conf.Configuration
import org.mockito.{ArgumentMatchers => mc}
import org.mockito.Mockito.{mock, times, verify, when}
import org.scalatest.concurrent.Eventually.{eventually, interval, timeout}

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite, TestUtils}
import org.apache.spark.LocalSparkContext.withSpark
import org.apache.spark.internal.config._
import org.apache.spark.launcher.SparkLauncher.{EXECUTOR_MEMORY, SPARK_MASTER}
import org.apache.spark.network.BlockTransferService
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.scheduler.ExecutorDecommissionInfo
import org.apache.spark.scheduler.cluster.StandaloneSchedulerBackend
import org.apache.spark.shuffle.{IndexShuffleBlockResolver, ShuffleBlockInfo}
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.util.Utils.tryWithResource

class FallbackStorageSuite extends SparkFunSuite with LocalSparkContext {

  def getSparkConf(initialExecutor: Int = 1, minExecutor: Int = 1): SparkConf = {
    new SparkConf(false)
      .setAppName(getClass.getName)
      .set(SPARK_MASTER, s"local-cluster[$initialExecutor,1,1024]")
      .set(EXECUTOR_MEMORY, "1g")
      .set(UI.UI_ENABLED, false)
      .set(DYN_ALLOCATION_ENABLED, true)
      .set(DYN_ALLOCATION_SHUFFLE_TRACKING_ENABLED, true)
      .set(DYN_ALLOCATION_INITIAL_EXECUTORS, initialExecutor)
      .set(DYN_ALLOCATION_MIN_EXECUTORS, minExecutor)
      .set(DECOMMISSION_ENABLED, true)
      .set(STORAGE_DECOMMISSION_ENABLED, true)
      .set(STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED, true)
      .set(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH,
         Files.createTempDirectory("tmp").toFile.getAbsolutePath + "/")
  }

  test("fallback storage APIs - copy/exists") {
    val conf = new SparkConf(false)
      .set("spark.app.id", "testId")
      .set(SHUFFLE_COMPRESS, false)
      .set(STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED, true)
      .set(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH,
        Files.createTempDirectory("tmp").toFile.getAbsolutePath + "/")
    val fallbackStorage = new FallbackStorage(conf)
    val bmm = new BlockManagerMaster(new NoopRpcEndpointRef(conf), null, conf, false)

    val bm = mock(classOf[BlockManager])
    val dbm = new DiskBlockManager(conf, deleteFilesOnStop = false, isDriver = false)
    when(bm.diskBlockManager).thenReturn(dbm)
    when(bm.master).thenReturn(bmm)
    val resolver = new IndexShuffleBlockResolver(conf, bm)
    when(bm.migratableResolver).thenReturn(resolver)

    resolver.getIndexFile(1, 1L).createNewFile()
    resolver.getDataFile(1, 1L).createNewFile()

    val indexFile = resolver.getIndexFile(1, 2L)
    tryWithResource(new FileOutputStream(indexFile)) { fos =>
      tryWithResource(new DataOutputStream(fos)) { dos =>
        dos.writeLong(0)
        dos.writeLong(4)
      }
    }

    val dataFile = resolver.getDataFile(1, 2L)
    tryWithResource(new FileOutputStream(dataFile)) { fos =>
      tryWithResource(new DataOutputStream(fos)) { dos =>
        dos.writeLong(0)
      }
    }

    fallbackStorage.copy(ShuffleBlockInfo(1, 1L), bm)
    fallbackStorage.copy(ShuffleBlockInfo(1, 2L), bm)

    assert(fallbackStorage.exists(1, ShuffleIndexBlockId(1, 1L, NOOP_REDUCE_ID).name))
    assert(fallbackStorage.exists(1, ShuffleDataBlockId(1, 1L, NOOP_REDUCE_ID).name))
    assert(fallbackStorage.exists(1, ShuffleIndexBlockId(1, 2L, NOOP_REDUCE_ID).name))
    assert(fallbackStorage.exists(1, ShuffleDataBlockId(1, 2L, NOOP_REDUCE_ID).name))

    // The files for shuffle 1 and map 1 are empty intentionally.
    intercept[java.io.EOFException] {
      FallbackStorage.read(conf, ShuffleBlockId(1, 1L, 0))
    }
    FallbackStorage.read(conf, ShuffleBlockId(1, 2L, 0))
  }

  test("SPARK-34142: fallback storage API - cleanUp") {
    withTempDir { dir =>
      Seq(true, false).foreach { cleanUp =>
        val appId = s"test$cleanUp"
        val conf = new SparkConf(false)
          .set("spark.app.id", appId)
          .set(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH, dir.getAbsolutePath + "/")
          .set(STORAGE_DECOMMISSION_FALLBACK_STORAGE_CLEANUP, cleanUp)

        val location = new File(dir, appId)
        assert(location.mkdir())
        assert(location.exists())
        FallbackStorage.cleanUp(conf, new Configuration())
        assert(location.exists() != cleanUp)
      }
    }
  }

  test("migrate shuffle data to fallback storage") {
    val conf = new SparkConf(false)
      .set("spark.app.id", "testId")
      .set(STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED, true)
      .set(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH,
        Files.createTempDirectory("tmp").toFile.getAbsolutePath + "/")

    val ids = Set((1, 1L, 1))
    val bm = mock(classOf[BlockManager])
    val dbm = new DiskBlockManager(conf, deleteFilesOnStop = false, isDriver = false)
    when(bm.diskBlockManager).thenReturn(dbm)
    val indexShuffleBlockResolver = new IndexShuffleBlockResolver(conf, bm)
    val indexFile = indexShuffleBlockResolver.getIndexFile(1, 1L)
    val dataFile = indexShuffleBlockResolver.getDataFile(1, 1L)
    indexFile.createNewFile()
    dataFile.createNewFile()

    val resolver = mock(classOf[IndexShuffleBlockResolver])
    when(resolver.getStoredShuffles())
      .thenReturn(ids.map(triple => ShuffleBlockInfo(triple._1, triple._2)).toSeq)
    ids.foreach { case (shuffleId: Int, mapId: Long, reduceId: Int) =>
      when(resolver.getMigrationBlocks(mc.any()))
        .thenReturn(List(
          (ShuffleIndexBlockId(shuffleId, mapId, reduceId), mock(classOf[ManagedBuffer])),
          (ShuffleDataBlockId(shuffleId, mapId, reduceId), mock(classOf[ManagedBuffer]))))
      when(resolver.getIndexFile(shuffleId, mapId)).thenReturn(indexFile)
      when(resolver.getDataFile(shuffleId, mapId)).thenReturn(dataFile)
    }

    when(bm.getPeers(mc.any()))
      .thenReturn(Seq(FallbackStorage.FALLBACK_BLOCK_MANAGER_ID))
    val bmm = new BlockManagerMaster(new NoopRpcEndpointRef(conf), null, conf, false)
    when(bm.master).thenReturn(bmm)
    val blockTransferService = mock(classOf[BlockTransferService])
    when(blockTransferService.uploadBlockSync(mc.any(), mc.any(), mc.any(), mc.any(), mc.any(),
      mc.any(), mc.any())).thenThrow(new IOException)
    when(bm.blockTransferService).thenReturn(blockTransferService)
    when(bm.migratableResolver).thenReturn(resolver)
    when(bm.getMigratableRDDBlocks()).thenReturn(Seq())

    val decommissioner = new BlockManagerDecommissioner(conf, bm)

    try {
      decommissioner.start()
      val fallbackStorage = new FallbackStorage(conf)
      eventually(timeout(10.second), interval(1.seconds)) {
        // uploadBlockSync is not used
        verify(blockTransferService, times(1))
          .uploadBlockSync(mc.any(), mc.any(), mc.any(), mc.any(), mc.any(), mc.any(), mc.any())

        Seq("shuffle_1_1_0.index", "shuffle_1_1_0.data").foreach { filename =>
          assert(fallbackStorage.exists(shuffleId = 1, filename))
        }
      }
    } finally {
      decommissioner.stop()
    }
  }

  test("Upload from all decommissioned executors") {
    sc = new SparkContext(getSparkConf(2, 2))
    withSpark(sc) { sc =>
      TestUtils.waitUntilExecutorsUp(sc, 2, 60000)
      val rdd1 = sc.parallelize(1 to 10, 10)
      val rdd2 = rdd1.map(x => (x % 2, 1))
      val rdd3 = rdd2.reduceByKey(_ + _)
      assert(rdd3.count() === 2)

      // Decommission all
      val sched = sc.schedulerBackend.asInstanceOf[StandaloneSchedulerBackend]
      sc.getExecutorIds().foreach {
        sched.decommissionExecutor(_, ExecutorDecommissionInfo(""), false)
      }

      val files = Seq("shuffle_0_0_0.index", "shuffle_0_0_0.data")
      val fallbackStorage = new FallbackStorage(sc.getConf)
      // Uploading is not started yet.
      files.foreach { file => assert(!fallbackStorage.exists(0, file)) }

      // Uploading is completed on decommissioned executors
      eventually(timeout(20.seconds), interval(1.seconds)) {
        files.foreach { file => assert(fallbackStorage.exists(0, file)) }
      }

      // All executors are still alive.
      assert(sc.getExecutorIds().size == 2)
    }
  }

  test("Upload multi stages") {
    sc = new SparkContext(getSparkConf())
    withSpark(sc) { sc =>
      TestUtils.waitUntilExecutorsUp(sc, 1, 60000)
      val rdd1 = sc.parallelize(1 to 10, 2)
      val rdd2 = rdd1.map(x => (x % 2, 1))
      val rdd3 = rdd2.reduceByKey(_ + _)
      val rdd4 = rdd3.sortByKey()
      assert(rdd4.count() === 2)

      val shuffle0_files = Seq(
        "shuffle_0_0_0.index", "shuffle_0_0_0.data",
        "shuffle_0_1_0.index", "shuffle_0_1_0.data")
      val shuffle1_files = Seq(
        "shuffle_1_4_0.index", "shuffle_1_4_0.data",
        "shuffle_1_5_0.index", "shuffle_1_5_0.data")
      val fallbackStorage = new FallbackStorage(sc.getConf)
      shuffle0_files.foreach { file => assert(!fallbackStorage.exists(0, file)) }
      shuffle1_files.foreach { file => assert(!fallbackStorage.exists(1, file)) }

      // Decommission all
      val sched = sc.schedulerBackend.asInstanceOf[StandaloneSchedulerBackend]
      sc.getExecutorIds().foreach {
        sched.decommissionExecutor(_, ExecutorDecommissionInfo(""), false)
      }

      eventually(timeout(20.seconds), interval(1.seconds)) {
        shuffle0_files.foreach { file => assert(fallbackStorage.exists(0, file)) }
        shuffle1_files.foreach { file => assert(fallbackStorage.exists(1, file)) }
      }
    }
  }

  Seq("lz4", "lzf", "snappy", "zstd").foreach { codec =>
    test(s"$codec - Newly added executors should access old data from remote storage") {
      sc = new SparkContext(getSparkConf(2, 0).set(IO_COMPRESSION_CODEC, codec))
      withSpark(sc) { sc =>
        TestUtils.waitUntilExecutorsUp(sc, 2, 60000)
        val rdd1 = sc.parallelize(1 to 10, 2)
        val rdd2 = rdd1.map(x => (x % 2, 1))
        val rdd3 = rdd2.reduceByKey(_ + _)
        assert(rdd3.collect() === Array((0, 5), (1, 5)))

        // Decommission all
        val sched = sc.schedulerBackend.asInstanceOf[StandaloneSchedulerBackend]
        sc.getExecutorIds().foreach {
          sched.decommissionExecutor(_, ExecutorDecommissionInfo(""), false)
        }

        // Make it sure that fallback storage are ready
        val fallbackStorage = new FallbackStorage(sc.getConf)
        eventually(timeout(20.seconds), interval(1.seconds)) {
          Seq(
            "shuffle_0_0_0.index", "shuffle_0_0_0.data",
            "shuffle_0_1_0.index", "shuffle_0_1_0.data").foreach { file =>
            assert(fallbackStorage.exists(0, file))
          }
        }

        // Since the data is safe, force to shrink down to zero executor
        sc.getExecutorIds().foreach { id =>
          sched.killExecutor(id)
        }
        eventually(timeout(20.seconds), interval(1.seconds)) {
          assert(sc.getExecutorIds().isEmpty)
        }

        // Dynamic allocation will start new executors
        assert(rdd3.collect() === Array((0, 5), (1, 5)))
        assert(rdd3.sortByKey().count() == 2)
        assert(sc.getExecutorIds().nonEmpty)
      }
    }
  }
}
