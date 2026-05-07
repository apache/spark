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

package org.apache.spark

import org.scalatest.Assertions._
import org.scalatest.concurrent.{Signaler, ThreadSignaler, TimeLimits}
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.{Millis, Span}

import org.apache.spark.internal.config
import org.apache.spark.internal.config.Tests._
import org.apache.spark.security.EncryptionFunSuite
import org.apache.spark.storage.{RDDBlockId, StorageLevel}
import org.apache.spark.util.io.ChunkedByteBuffer

class NotSerializableClass
class NotSerializableExn(val notSer: NotSerializableClass) extends Throwable() {}


class DistributedSuite extends SparkFunSuite with Matchers with LocalSparkContext
  with EncryptionFunSuite with TimeLimits {

  // Necessary to make ScalaTest 3.x interrupt a thread on the JVM like ScalaTest 2.2.x
  implicit val defaultSignaler: Signaler = ThreadSignaler

  val clusterUrl = "local-cluster[3,1,1024]"

  test("task throws not serializable exception") {
    // Ensures that executors do not crash when an exn is not serializable. If executors crash,
    // this test will hang. Correct behavior is that executors don't crash but fail tasks
    // and the scheduler throws a SparkException.

    // numWorkers must be less than numPartitions
    val numWorkers = 3
    val numPartitions = 10

    sc = new SparkContext("local-cluster[%s,1,1024]".format(numWorkers), "test")
    val data = sc.parallelize(1 to 100, numPartitions).
      map(x => throw new NotSerializableExn(new NotSerializableClass))
    intercept[SparkException] {
      data.count()
    }
    resetSparkContext()
  }

  test("local-cluster format") {
    import SparkMasterRegex._

    val masterStrings = Seq(
      "local-cluster[2,1,1024]",
      "local-cluster[2 , 1 , 1024]",
      "local-cluster[2, 1, 1024]",
      "local-cluster[ 2, 1, 1024 ]"
    )

    masterStrings.foreach {
      case LOCAL_CLUSTER_REGEX(numWorkers, coresPerWorker, memoryPerWorker) =>
        assert(numWorkers.toInt == 2)
        assert(coresPerWorker.toInt == 1)
        assert(memoryPerWorker.toInt == 1024)
    }
  }

  test("simple groupByKey") {
    sc = new SparkContext(clusterUrl, "test")
    val pairs = sc.parallelize(Seq((1, 1), (1, 2), (1, 3), (2, 1)), 5)
    val groups = pairs.groupByKey(5).collect()
    assert(groups.length === 2)
    val valuesFor1 = groups.find(_._1 == 1).get._2
    assert(valuesFor1.toList.sorted === List(1, 2, 3))
    val valuesFor2 = groups.find(_._1 == 2).get._2
    assert(valuesFor2.toList.sorted === List(1))
  }

  test("groupByKey where map output sizes exceed maxMbInFlight") {
    val conf = new SparkConf().set(config.REDUCER_MAX_SIZE_IN_FLIGHT.key, "1m")
    sc = new SparkContext(clusterUrl, "test", conf)
    // This data should be around 20 MB, so even with 4 mappers and 2 reducers, each map output
    // file should be about 2.5 MB
    val pairs = sc.parallelize(1 to 2000, 4).map(x => (x % 16, new Array[Byte](10000)))
    val groups = pairs.groupByKey(2).map(x => (x._1, x._2.size)).collect()
    assert(groups.length === 16)
    assert(groups.map(_._2).sum === 2000)
  }

  test("accumulators") {
    sc = new SparkContext(clusterUrl, "test")
    val accum = sc.longAccumulator
    sc.parallelize(1 to 10, 10).foreach(x => accum.add(x))
    assert(accum.value === 55)
  }

  test("broadcast variables") {
    sc = new SparkContext(clusterUrl, "test")
    val array = new Array[Int](100)
    val bv = sc.broadcast(array)
    array(2) = 3     // Change the array -- this should not be seen on workers
    val rdd = sc.parallelize(1 to 10, 10)
    val sum = rdd.map(x => bv.value.sum).reduce(_ + _)
    assert(sum === 0)
  }

  test("repeatedly failing task") {
    sc = new SparkContext(clusterUrl, "test")
    val thrown = intercept[SparkException] {
      // scalastyle:off println
      sc.parallelize(1 to 10, 10).foreach(x => println(x / 0))
      // scalastyle:on println
    }
    assert(thrown.getClass === classOf[SparkException])
    assert(thrown.getMessage.contains("failed 4 times"))
  }

  test("repeatedly failing task that crashes JVM") {
    // Ensures that if a task fails in a way that crashes the JVM, the job eventually fails rather
    // than hanging due to retrying the failed task infinitely many times (eventually the
    // standalone scheduler will remove the application, causing the job to hang waiting to
    // reconnect to the master).
    sc = new SparkContext(clusterUrl, "test")
    failAfter(Span(100000, Millis)) {
      val thrown = intercept[SparkException] {
        // One of the tasks always fails.
        sc.parallelize(1 to 10, 2).foreach { x => if (x == 1) System.exit(42) }
      }
      assert(thrown.getClass === classOf[SparkException])
      assert(thrown.getMessage.contains("failed 4 times"))
    }
  }

  test("repeatedly failing task that crashes JVM with a zero exit code (SPARK-16925)") {
    // Ensures that if a task which causes the JVM to exit with a zero exit code will cause the
    // Spark job to eventually fail.
    sc = new SparkContext(clusterUrl, "test")
    failAfter(Span(100000, Millis)) {
      val thrown = intercept[SparkException] {
        sc.parallelize(1 to 1, 1).foreachPartition { _ => System.exit(0) }
      }
      assert(thrown.getClass === classOf[SparkException])
      assert(thrown.getMessage.contains("failed 4 times"))
    }
    // Check that the cluster is still usable:
    sc.parallelize(1 to 10).count()
  }

  private def testCaching(testName: String, conf: SparkConf, storageLevel: StorageLevel): Unit = {
    test(testName) {
      testCaching(conf, storageLevel)
    }
    if (storageLevel.replication > 1) {
      // also try with block replication as a stream
      val uploadStreamConf = new SparkConf()
      uploadStreamConf.setAll(conf.getAll)
      uploadStreamConf.set(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM, 1L)
      test(s"$testName (with replication as stream)") {
        testCaching(uploadStreamConf, storageLevel)
      }
    }
  }

  private def testCaching(conf: SparkConf, storageLevel: StorageLevel): Unit = {
    sc = new SparkContext(conf.setMaster(clusterUrl).setAppName("test"))
    TestUtils.waitUntilExecutorsUp(sc, 3, 60000)
    val data = sc.parallelize(1 to 1000, 10)
    val cachedData = data.persist(storageLevel)
    assert(cachedData.count() === 1000)
    assert(sc.getRDDStorageInfo.filter(_.id == cachedData.id).map(_.numCachedPartitions).sum ===
      data.getNumPartitions)
    // Get all the locations of the first partition and try to fetch the partitions
    // from those locations.
    val blockIds = data.partitions.indices.map(index => RDDBlockId(data.id, index)).toArray
    val blockId = blockIds(0)
    val blockManager = SparkEnv.get.blockManager
    val blockTransfer = blockManager.blockTransferService
    val serializerManager = SparkEnv.get.serializerManager
    val locations = blockManager.master.getLocations(blockId)
    assert(locations.size === storageLevel.replication,
      s"; got ${locations.size} replicas instead of ${storageLevel.replication}")
    locations.foreach { cmId =>
      val bytes = blockTransfer.fetchBlockSync(cmId.host, cmId.port, cmId.executorId,
        blockId.toString, null)
      val deserialized = serializerManager.dataDeserializeStream(blockId,
        new ChunkedByteBuffer(bytes.nioByteBuffer()).toInputStream())(data.elementClassTag).toList
      assert(deserialized === (1 to 100).toList)
    }
    // This will exercise the getRemoteValues code path:
    assert(blockIds.flatMap(id => blockManager.get[Int](id).get.data).toSet === (1 to 1000).toSet)
  }

  Seq(
    "caching" -> StorageLevel.MEMORY_ONLY,
    "caching on disk" -> StorageLevel.DISK_ONLY,
    "caching in memory, replicated" -> StorageLevel.MEMORY_ONLY_2,
    "caching in memory, serialized, replicated" -> StorageLevel.MEMORY_ONLY_SER_2,
    "caching on disk, replicated 2" -> StorageLevel.DISK_ONLY_2,
    "caching on disk, replicated 3" -> StorageLevel.DISK_ONLY_3,
    "caching in memory and disk, replicated" -> StorageLevel.MEMORY_AND_DISK_2,
    "caching in memory and disk, serialized, replicated" -> StorageLevel.MEMORY_AND_DISK_SER_2
  ).foreach { case (testName, storageLevel) =>
    encryptionTestHelper(testName) { case (name, conf) =>
      testCaching(name, conf, storageLevel)
    }
  }

  test("compute without caching when no partitions fit in memory") {
    val size = 10000
    val conf = new SparkConf()
      .set(config.STORAGE_UNROLL_MEMORY_THRESHOLD, 1024L)
      .set(TEST_MEMORY, size.toLong / 2)

    sc = new SparkContext(clusterUrl, "test", conf)
    val data = sc.parallelize(1 to size, 2).persist(StorageLevel.MEMORY_ONLY)
    assert(data.count() === size)
    assert(data.count() === size)
    assert(data.count() === size)
    // ensure only a subset of partitions were cached
    val rddBlocks = sc.env.blockManager.master.getMatchingBlockIds(_.isRDD,
      askStorageEndpoints = true)
    assert(rddBlocks.size === 0, s"expected no RDD blocks, found ${rddBlocks.size}")
  }

  test("compute when only some partitions fit in memory") {
    val size = 10000
    val numPartitions = 20
    val conf = new SparkConf()
      .set(config.STORAGE_UNROLL_MEMORY_THRESHOLD, 1024L)
      .set(TEST_MEMORY, size.toLong)

    sc = new SparkContext(clusterUrl, "test", conf)
    val data = sc.parallelize(1 to size, numPartitions).persist(StorageLevel.MEMORY_ONLY)
    assert(data.count() === size)
    assert(data.count() === size)
    assert(data.count() === size)
    // ensure only a subset of partitions were cached
    val rddBlocks = sc.env.blockManager.master.getMatchingBlockIds(_.isRDD,
      askStorageEndpoints = true)
    assert(rddBlocks.size > 0, "no RDD blocks found")
    assert(rddBlocks.size < numPartitions, s"too many RDD blocks found, expected <$numPartitions")
  }

  test("passing environment variables to cluster") {
    sc = new SparkContext(clusterUrl, "test", null, Nil, Map("TEST_VAR" -> "TEST_VALUE"))
    val values = sc.parallelize(1 to 2, 2).map(x => System.getenv("TEST_VAR")).collect()
    assert(values.toSeq === Seq("TEST_VALUE", "TEST_VALUE"))
  }

  test("recover from node failures") {
    import DistributedSuite.{markNodeIfIdentity, failOnMarkedIdentity}
    DistributedSuite.amMaster = true
    sc = new SparkContext(clusterUrl, "test")
    val data = sc.parallelize(Seq(true, true), 2)
    assert(data.count() === 2) // force executors to start
    assert(data.map(markNodeIfIdentity).collect().length === 2)
    assert(data.map(failOnMarkedIdentity).collect().length === 2)
  }

  test("recover from repeated node failures during shuffle-map") {
    import DistributedSuite.{markNodeIfIdentity, failOnMarkedIdentity}
    DistributedSuite.amMaster = true
    sc = new SparkContext(clusterUrl, "test")
    for (i <- 1 to 3) {
      val data = sc.parallelize(Seq(true, false), 2)
      assert(data.count() === 2)
      assert(data.map(markNodeIfIdentity).collect().length === 2)
      assert(data.map(failOnMarkedIdentity).map(x => x -> x).groupByKey().count() === 2)
    }
  }

  test("recover from repeated node failures during shuffle-reduce") {
    import DistributedSuite.{markNodeIfIdentity, failOnMarkedIdentity}
    DistributedSuite.amMaster = true
    sc = new SparkContext(clusterUrl, "test")
    for (i <- 1 to 3) {
      val data = sc.parallelize(Seq(true, true), 2)
      assert(data.count() === 2)
      assert(data.map(markNodeIfIdentity).collect().length === 2)
      // This relies on mergeCombiners being used to perform the actual reduce for this
      // test to actually be testing what it claims.
      val grouped = data.map(x => x -> x).combineByKey(
                      x => x,
                      (x: Boolean, y: Boolean) => x,
                      (x: Boolean, y: Boolean) => failOnMarkedIdentity(x)
                    )
      assert(grouped.collect().length === 1)
    }
  }

  test("recover from node failures with replication") {
    import DistributedSuite.{markNodeIfIdentity, failOnMarkedIdentity}
    DistributedSuite.amMaster = true
    // Using more than two nodes so we don't have a symmetric communication pattern and might
    // cache a partially correct list of peers.
    sc = new SparkContext("local-cluster[3,1,1024]", "test")
    for (i <- 1 to 3) {
      val data = sc.parallelize(Seq(true, false, false, false), 4)
      data.persist(StorageLevel.MEMORY_ONLY_2)

      assert(data.count() === 4)
      assert(data.map(markNodeIfIdentity).collect().length === 4)
      assert(data.map(failOnMarkedIdentity).collect().length === 4)

      // Create a new replicated RDD to make sure that cached peer information doesn't cause
      // problems.
      val data2 = sc.parallelize(Seq(true, true), 2).persist(StorageLevel.MEMORY_ONLY_2)
      assert(data2.count() === 2)
    }
  }

  test("unpersist RDDs") {
    DistributedSuite.amMaster = true
    sc = new SparkContext("local-cluster[3,1,1024]", "test")
    val data = sc.parallelize(Seq(true, false, false, false), 4)
    data.persist(StorageLevel.MEMORY_ONLY_2)
    data.count()
    assert(sc.persistentRdds.nonEmpty)
    data.unpersist(blocking = true)
    assert(sc.persistentRdds.isEmpty)

    failAfter(Span(3000, Millis)) {
      try {
        while (! sc.getRDDStorageInfo.isEmpty) {
          Thread.sleep(200)
        }
      } catch {
        case _: Throwable => Thread.sleep(10)
          // Do nothing. We might see exceptions because block manager
          // is racing this thread to remove entries from the driver.
      }
    }
  }

  test("reference partitions inside a task") {
    // Run a simple job which just makes sure there is no failure if we touch rdd.partitions
    // inside a task.  This requires the stateLock to be serializable.  This is very convoluted
    // use case, it's just a check for backwards-compatibility after the fix for SPARK-28917.
    sc = new SparkContext("local-cluster[1,1,1024]", "test")
    val rdd1 = sc.parallelize(1 to 10, 1)
    val rdd2 = rdd1.map { x => x + 1}
    // ensure we can force computation of rdd2.dependencies inside a task.  Just touching
    // it will force computation and touching the stateLock.  The check for null is to just
    // to make sure that we've setup our test correctly, and haven't precomputed dependencies
    // in the driver
    val dependencyComputeCount = rdd1.map { x => if (rdd2.dependencies == null) 1 else 0}.sum()
    assert(dependencyComputeCount > 0)
  }

}

object DistributedSuite {
  // Indicates whether this JVM is marked for failure.
  var mark = false

  // Set by test to remember if we are in the driver program so we can assert
  // that we are not.
  var amMaster = false

  // Act like an identity function, but if the argument is true, set mark to true.
  def markNodeIfIdentity(item: Boolean): Boolean = {
    if (item) {
      assert(!amMaster)
      mark = true
    }
    item
  }

  // Act like an identity function, but if mark was set to true previously, fail,
  // crashing the entire JVM.
  def failOnMarkedIdentity(item: Boolean): Boolean = {
    if (mark) {
      System.exit(42)
    }
    item
  }
}
