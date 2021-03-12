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

import java.io.{File, IOException}
import java.net.URI
import java.util
import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.scalatest.{BeforeAndAfter, PrivateMethodTester}
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark._
import org.apache.spark.LocalSparkContext._
import org.apache.spark.internal.config.Network.RPC_NUM_RETRIES
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

class StateStoreSuite extends StateStoreSuiteBase[HDFSBackedStateStoreProvider]
  with BeforeAndAfter with PrivateMethodTester {
  type MapType = mutable.HashMap[UnsafeRow, UnsafeRow]
  type ProviderMapType = java.util.concurrent.ConcurrentHashMap[UnsafeRow, UnsafeRow]

  import StateStoreCoordinatorSuite._
  import StateStoreTestsHelper._

  val keySchema = StructType(Seq(StructField("key", StringType, true)))
  val valueSchema = StructType(Seq(StructField("value", IntegerType, true)))

  before {
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
  }

  after {
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
  }

  def updateVersionTo(
      provider: StateStoreProvider,
      currentVersion: Int,
      targetVersion: Int): Int = {
    var newCurrentVersion = currentVersion
    for (i <- newCurrentVersion until targetVersion) {
      newCurrentVersion = incrementVersion(provider, i)
    }
    require(newCurrentVersion === targetVersion)
    newCurrentVersion
  }

  def incrementVersion(provider: StateStoreProvider, currentVersion: Int): Int = {
    val store = provider.getStore(currentVersion)
    put(store, "a", currentVersion + 1)
    store.commit()
    currentVersion + 1
  }

  def checkLoadedVersions(
      loadedMaps: util.SortedMap[Long, ProviderMapType],
      count: Int,
      earliestKey: Long,
      latestKey: Long): Unit = {
    assert(loadedMaps.size() === count)
    assert(loadedMaps.firstKey() === earliestKey)
    assert(loadedMaps.lastKey() === latestKey)
  }

  def checkVersion(
      loadedMaps: util.SortedMap[Long, ProviderMapType],
      version: Long,
      expectedData: Map[String, Int]): Unit = {

    val originValueMap = loadedMaps.get(version).asScala.map { entry =>
      rowToString(entry._1) -> rowToInt(entry._2)
    }.toMap

    assert(originValueMap === expectedData)
  }

  test("retaining only two latest versions when MAX_BATCHES_TO_RETAIN_IN_MEMORY set to 2") {
    val provider = newStoreProvider(opId = Random.nextInt, partition = 0,
      numOfVersToRetainInMemory = 2)

    var currentVersion = 0

    // commit the ver 1 : cache will have one element
    currentVersion = incrementVersion(provider, currentVersion)
    assert(getData(provider) === Set("a" -> 1))
    var loadedMaps = provider.getLoadedMaps()
    checkLoadedVersions(loadedMaps, count = 1, earliestKey = 1, latestKey = 1)
    checkVersion(loadedMaps, 1, Map("a" -> 1))

    // commit the ver 2 : cache will have two elements
    currentVersion = incrementVersion(provider, currentVersion)
    assert(getData(provider) === Set("a" -> 2))
    loadedMaps = provider.getLoadedMaps()
    checkLoadedVersions(loadedMaps, count = 2, earliestKey = 2, latestKey = 1)
    checkVersion(loadedMaps, 2, Map("a" -> 2))
    checkVersion(loadedMaps, 1, Map("a" -> 1))

    // commit the ver 3 : cache has already two elements and adding ver 3 incurs exceeding cache,
    // and ver 3 will be added but ver 1 will be evicted
    currentVersion = incrementVersion(provider, currentVersion)
    assert(getData(provider) === Set("a" -> 3))
    loadedMaps = provider.getLoadedMaps()
    checkLoadedVersions(loadedMaps, count = 2, earliestKey = 3, latestKey = 2)
    checkVersion(loadedMaps, 3, Map("a" -> 3))
    checkVersion(loadedMaps, 2, Map("a" -> 2))
  }

  test("failure after committing with MAX_BATCHES_TO_RETAIN_IN_MEMORY set to 1") {
    val provider = newStoreProvider(opId = Random.nextInt, partition = 0,
      numOfVersToRetainInMemory = 1)

    var currentVersion = 0

    // commit the ver 1 : cache will have one element
    currentVersion = incrementVersion(provider, currentVersion)
    assert(getData(provider) === Set("a" -> 1))
    var loadedMaps = provider.getLoadedMaps()
    checkLoadedVersions(loadedMaps, count = 1, earliestKey = 1, latestKey = 1)
    checkVersion(loadedMaps, 1, Map("a" -> 1))

    // commit the ver 2 : cache has already one elements and adding ver 2 incurs exceeding cache,
    // and ver 2 will be added but ver 1 will be evicted
    // this fact ensures cache miss will occur when this partition succeeds commit
    // but there's a failure afterwards so have to reprocess previous batch
    currentVersion = incrementVersion(provider, currentVersion)
    assert(getData(provider) === Set("a" -> 2))
    loadedMaps = provider.getLoadedMaps()
    checkLoadedVersions(loadedMaps, count = 1, earliestKey = 2, latestKey = 2)
    checkVersion(loadedMaps, 2, Map("a" -> 2))

    // suppose there has been failure after committing, and it decided to reprocess previous batch
    currentVersion = 1

    // committing to existing version which is committed partially but abandoned globally
    val store = provider.getStore(currentVersion)
    // negative value to represent reprocessing
    put(store, "a", -2)
    store.commit()
    currentVersion += 1

    // make sure newly committed version is reflected to the cache (overwritten)
    assert(getData(provider) === Set("a" -> -2))
    loadedMaps = provider.getLoadedMaps()
    checkLoadedVersions(loadedMaps, count = 1, earliestKey = 2, latestKey = 2)
    checkVersion(loadedMaps, 2, Map("a" -> -2))
  }

  test("no cache data with MAX_BATCHES_TO_RETAIN_IN_MEMORY set to 0") {
    val provider = newStoreProvider(opId = Random.nextInt, partition = 0,
      numOfVersToRetainInMemory = 0)

    var currentVersion = 0

    // commit the ver 1 : never cached
    currentVersion = incrementVersion(provider, currentVersion)
    assert(getData(provider) === Set("a" -> 1))
    var loadedMaps = provider.getLoadedMaps()
    assert(loadedMaps.size() === 0)

    // commit the ver 2 : never cached
    currentVersion = incrementVersion(provider, currentVersion)
    assert(getData(provider) === Set("a" -> 2))
    loadedMaps = provider.getLoadedMaps()
    assert(loadedMaps.size() === 0)
  }

  test("snapshotting") {
    val provider = newStoreProvider(opId = Random.nextInt, partition = 0, minDeltasForSnapshot = 5)

    var currentVersion = 0

    currentVersion = updateVersionTo(provider, currentVersion, 2)
    require(getData(provider) === Set("a" -> 2))
    provider.doMaintenance()               // should not generate snapshot files
    assert(getData(provider) === Set("a" -> 2))

    for (i <- 1 to currentVersion) {
      assert(fileExists(provider, i, isSnapshot = false))  // all delta files present
      assert(!fileExists(provider, i, isSnapshot = true))  // no snapshot files present
    }

    // After version 6, snapshotting should generate one snapshot file
    currentVersion = updateVersionTo(provider, currentVersion, 6)
    require(getData(provider) === Set("a" -> 6), "store not updated correctly")
    provider.doMaintenance()       // should generate snapshot files

    val snapshotVersion = (0 to 6).find(version => fileExists(provider, version, isSnapshot = true))
    assert(snapshotVersion.nonEmpty, "snapshot file not generated")
    deleteFilesEarlierThanVersion(provider, snapshotVersion.get)
    assert(
      getData(provider, snapshotVersion.get) === Set("a" -> snapshotVersion.get),
      "snapshotting messed up the data of the snapshotted version")
    assert(
      getData(provider) === Set("a" -> 6),
      "snapshotting messed up the data of the final version")

    // After version 20, snapshotting should generate newer snapshot files
    currentVersion = updateVersionTo(provider, currentVersion, 20)
    require(getData(provider) === Set("a" -> 20), "store not updated correctly")
    provider.doMaintenance()       // do snapshot

    val latestSnapshotVersion = (0 to 20).filter(version =>
      fileExists(provider, version, isSnapshot = true)).lastOption
    assert(latestSnapshotVersion.nonEmpty, "no snapshot file found")
    assert(latestSnapshotVersion.get > snapshotVersion.get, "newer snapshot not generated")

    deleteFilesEarlierThanVersion(provider, latestSnapshotVersion.get)
    assert(getData(provider) === Set("a" -> 20), "snapshotting messed up the data")
  }

  test("cleaning") {
    val provider = newStoreProvider(opId = Random.nextInt, partition = 0, minDeltasForSnapshot = 5)

    for (i <- 1 to 20) {
      val store = provider.getStore(i - 1)
      put(store, "a", i)
      store.commit()
      provider.doMaintenance() // do cleanup
    }
    require(
      rowsToSet(provider.latestIterator()) === Set("a" -> 20),
      "store not updated correctly")

    assert(!fileExists(provider, version = 1, isSnapshot = false)) // first file should be deleted

    // last couple of versions should be retrievable
    assert(getData(provider, 20) === Set("a" -> 20))
    assert(getData(provider, 19) === Set("a" -> 19))
  }

  testQuietly("SPARK-19677: Committing a delta file atop an existing one should not fail on HDFS") {
    val conf = new Configuration()
    conf.set("fs.fake.impl", classOf[RenameLikeHDFSFileSystem].getName)
    conf.set("fs.defaultFS", "fake:///")

    val provider = newStoreProvider(opId = Random.nextInt, partition = 0, hadoopConf = conf)
    provider.getStore(0).commit()
    provider.getStore(0).commit()

    // Verify we don't leak temp files
    val tempFiles = FileUtils.listFiles(new File(provider.stateStoreId.checkpointRootLocation),
      null, true).asScala.filter(_.getName.startsWith("temp-"))
    assert(tempFiles.isEmpty)
  }

  test("corrupted file handling") {
    val provider = newStoreProvider(opId = Random.nextInt, partition = 0, minDeltasForSnapshot = 5)
    for (i <- 1 to 6) {
      val store = provider.getStore(i - 1)
      put(store, "a", i)
      store.commit()
      provider.doMaintenance() // do cleanup
    }
    val snapshotVersion = (0 to 10).find( version =>
      fileExists(provider, version, isSnapshot = true)).getOrElse(fail("snapshot file not found"))

    // Corrupt snapshot file and verify that it throws error
    assert(getData(provider, snapshotVersion) === Set("a" -> snapshotVersion))
    corruptFile(provider, snapshotVersion, isSnapshot = true)
    intercept[Exception] {
      getData(provider, snapshotVersion)
    }

    // Corrupt delta file and verify that it throws error
    assert(getData(provider, snapshotVersion - 1) === Set("a" -> (snapshotVersion - 1)))
    corruptFile(provider, snapshotVersion - 1, isSnapshot = false)
    intercept[Exception] {
      getData(provider, snapshotVersion - 1)
    }

    // Delete delta file and verify that it throws error
    deleteFilesEarlierThanVersion(provider, snapshotVersion)
    intercept[Exception] {
      getData(provider, snapshotVersion - 1)
    }
  }

  test("reports memory usage") {
    val provider = newStoreProvider()
    val store = provider.getStore(0)
    val noDataMemoryUsed = store.metrics.memoryUsedBytes
    put(store, "a", 1)
    store.commit()
    assert(store.metrics.memoryUsedBytes > noDataMemoryUsed)
  }

  test("reports memory usage on current version") {
    def getSizeOfStateForCurrentVersion(metrics: StateStoreMetrics): Long = {
      val metricPair = metrics.customMetrics.find(_._1.name == "stateOnCurrentVersionSizeBytes")
      assert(metricPair.isDefined)
      metricPair.get._2
    }

    val provider = newStoreProvider()
    val store = provider.getStore(0)
    val noDataMemoryUsed = getSizeOfStateForCurrentVersion(store.metrics)

    put(store, "a", 1)
    store.commit()
    assert(getSizeOfStateForCurrentVersion(store.metrics) > noDataMemoryUsed)
  }

  test("StateStore.get") {
    quietly {
      val dir = newDir()
      val storeId = StateStoreProviderId(StateStoreId(dir, 0, 0), UUID.randomUUID)
      val storeConf = StateStoreConf.empty
      val hadoopConf = new Configuration()

      // Verify that trying to get incorrect versions throw errors
      intercept[IllegalArgumentException] {
        StateStore.get(
          storeId, keySchema, valueSchema, None, -1, storeConf, hadoopConf)
      }
      assert(!StateStore.isLoaded(storeId)) // version -1 should not attempt to load the store

      intercept[IllegalStateException] {
        StateStore.get(
          storeId, keySchema, valueSchema, None, 1, storeConf, hadoopConf)
      }

      // Increase version of the store and try to get again
      val store0 = StateStore.get(
        storeId, keySchema, valueSchema, None, 0, storeConf, hadoopConf)
      assert(store0.version === 0)
      put(store0, "a", 1)
      store0.commit()

      val store1 = StateStore.get(
        storeId, keySchema, valueSchema, None, 1, storeConf, hadoopConf)
      assert(StateStore.isLoaded(storeId))
      assert(store1.version === 1)
      assert(rowsToSet(store1.iterator()) === Set("a" -> 1))

      // Verify that you can also load older version
      val store0reloaded = StateStore.get(
        storeId, keySchema, valueSchema, None, 0, storeConf, hadoopConf)
      assert(store0reloaded.version === 0)
      assert(rowsToSet(store0reloaded.iterator()) === Set.empty)

      // Verify that you can remove the store and still reload and use it
      StateStore.unload(storeId)
      assert(!StateStore.isLoaded(storeId))

      val store1reloaded = StateStore.get(
        storeId, keySchema, valueSchema, None, 1, storeConf, hadoopConf)
      assert(StateStore.isLoaded(storeId))
      assert(store1reloaded.version === 1)
      put(store1reloaded, "a", 2)
      assert(store1reloaded.commit() === 2)
      assert(rowsToSet(store1reloaded.iterator()) === Set("a" -> 2))
    }
  }

  test("maintenance") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      // Make sure that when SparkContext stops, the StateStore maintenance thread 'quickly'
      // fails to talk to the StateStoreCoordinator and unloads all the StateStores
      .set(RPC_NUM_RETRIES, 1)
    val opId = 0
    val dir = newDir()
    val storeProviderId = StateStoreProviderId(StateStoreId(dir, opId, 0), UUID.randomUUID)
    val sqlConf = new SQLConf()
    sqlConf.setConf(SQLConf.MIN_BATCHES_TO_RETAIN, 2)
    // Make maintenance thread do snapshots and cleanups very fast
    sqlConf.setConf(SQLConf.STREAMING_MAINTENANCE_INTERVAL, 10L)
    val storeConf = StateStoreConf(sqlConf)
    val hadoopConf = new Configuration()
    val provider = newStoreProvider(storeProviderId.storeId)

    var latestStoreVersion = 0

    def generateStoreVersions(): Unit = {
      for (i <- 1 to 20) {
        val store = StateStore.get(storeProviderId, keySchema, valueSchema, None,
          latestStoreVersion, storeConf, hadoopConf)
        put(store, "a", i)
        store.commit()
        latestStoreVersion += 1
      }
    }

    val timeoutDuration = 1.minute

    quietly {
      withSpark(new SparkContext(conf)) { sc =>
        withCoordinatorRef(sc) { coordinatorRef =>
          require(!StateStore.isMaintenanceRunning, "StateStore is unexpectedly running")

          // Generate sufficient versions of store for snapshots
          generateStoreVersions()

          eventually(timeout(timeoutDuration)) {
            // Store should have been reported to the coordinator
            assert(coordinatorRef.getLocation(storeProviderId).nonEmpty,
              "active instance was not reported")

            // Background maintenance should clean up and generate snapshots
            assert(StateStore.isMaintenanceRunning, "Maintenance task is not running")

            // Some snapshots should have been generated
            val snapshotVersions = (1 to latestStoreVersion).filter { version =>
              fileExists(provider, version, isSnapshot = true)
            }
            assert(snapshotVersions.nonEmpty, "no snapshot file found")
          }

          // Generate more versions such that there is another snapshot and
          // the earliest delta file will be cleaned up
          generateStoreVersions()

          // Earliest delta file should get cleaned up
          eventually(timeout(timeoutDuration)) {
            assert(!fileExists(provider, 1, isSnapshot = false), "earliest file not deleted")
          }

          // If driver decides to deactivate all stores related to a query run,
          // then this instance should be unloaded
          coordinatorRef.deactivateInstances(storeProviderId.queryRunId)
          eventually(timeout(timeoutDuration)) {
            assert(!StateStore.isLoaded(storeProviderId))
          }

          // Reload the store and verify
          StateStore.get(storeProviderId, keySchema, valueSchema, indexOrdinal = None,
            latestStoreVersion, storeConf, hadoopConf)
          assert(StateStore.isLoaded(storeProviderId))

          // If some other executor loads the store, then this instance should be unloaded
          coordinatorRef.reportActiveInstance(storeProviderId, "other-host", "other-exec")
          eventually(timeout(timeoutDuration)) {
            assert(!StateStore.isLoaded(storeProviderId))
          }

          // Reload the store and verify
          StateStore.get(storeProviderId, keySchema, valueSchema, indexOrdinal = None,
            latestStoreVersion, storeConf, hadoopConf)
          assert(StateStore.isLoaded(storeProviderId))
        }
      }

      // Verify if instance is unloaded if SparkContext is stopped
      eventually(timeout(timeoutDuration)) {
        require(SparkEnv.get === null)
        assert(!StateStore.isLoaded(storeProviderId))
        assert(!StateStore.isMaintenanceRunning)
      }
    }
  }

  testQuietly("SPARK-18342: commit fails when rename fails") {
    import RenameReturnsFalseFileSystem._
    val dir = scheme + "://" + newDir()
    val conf = new Configuration()
    conf.set(s"fs.$scheme.impl", classOf[RenameReturnsFalseFileSystem].getName)
    val provider = newStoreProvider(
      opId = Random.nextInt, partition = 0, dir = dir, hadoopConf = conf)
    val store = provider.getStore(0)
    put(store, "a", 0)
    val e = intercept[IllegalStateException](store.commit())
    assert(e.getCause.getMessage.contains("Failed to rename"))
  }

  test("SPARK-18416: do not create temp delta file until the store is updated") {
    val dir = newDir()
    val storeId = StateStoreProviderId(StateStoreId(dir, 0, 0), UUID.randomUUID)
    val storeConf = StateStoreConf.empty
    val hadoopConf = new Configuration()
    val deltaFileDir = new File(s"$dir/0/0/")

    def numTempFiles: Int = {
      if (deltaFileDir.exists) {
        deltaFileDir.listFiles.map(_.getName).count(n => n.endsWith(".tmp"))
      } else 0
    }

    def numDeltaFiles: Int = {
      if (deltaFileDir.exists) {
        deltaFileDir.listFiles.map(_.getName).count(n => n.contains(".delta") && !n.startsWith("."))
      } else 0
    }

    def shouldNotCreateTempFile[T](body: => T): T = {
      val before = numTempFiles
      val result = body
      assert(numTempFiles === before)
      result
    }

    // Getting the store should not create temp file
    val store0 = shouldNotCreateTempFile {
      StateStore.get(
        storeId, keySchema, valueSchema, indexOrdinal = None, version = 0, storeConf, hadoopConf)
    }

    // Put should create a temp file
    put(store0, "a", 1)
    assert(numTempFiles === 1)
    assert(numDeltaFiles === 0)

    // Commit should remove temp file and create a delta file
    store0.commit()
    assert(numTempFiles === 0)
    assert(numDeltaFiles === 1)

    // Remove should create a temp file
    val store1 = shouldNotCreateTempFile {
      StateStore.get(
        storeId, keySchema, valueSchema, indexOrdinal = None, version = 1, storeConf, hadoopConf)
    }
    remove(store1, _ == "a")
    assert(numTempFiles === 1)
    assert(numDeltaFiles === 1)

    // Commit should remove temp file and create a delta file
    store1.commit()
    assert(numTempFiles === 0)
    assert(numDeltaFiles === 2)

    // Commit without any updates should create a delta file
    val store2 = shouldNotCreateTempFile {
      StateStore.get(
        storeId, keySchema, valueSchema, indexOrdinal = None, version = 2, storeConf, hadoopConf)
    }
    store2.commit()
    assert(numTempFiles === 0)
    assert(numDeltaFiles === 3)
  }

  test("SPARK-21145: Restarted queries create new provider instances") {
    try {
      val checkpointLocation = Utils.createTempDir().getAbsoluteFile
      val spark = SparkSession.builder().master("local[2]").getOrCreate()
      SparkSession.setActiveSession(spark)
      implicit val sqlContext = spark.sqlContext
      spark.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, "1")
      import spark.implicits._
      val inputData = MemoryStream[Int]

      def runQueryAndGetLoadedProviders(): Seq[StateStoreProvider] = {
        val aggregated = inputData.toDF().groupBy("value").agg(count("*"))
        // stateful query
        val query = aggregated.writeStream
          .format("memory")
          .outputMode("complete")
          .queryName("query")
          .option("checkpointLocation", checkpointLocation.toString)
          .start()
        inputData.addData(1, 2, 3)
        query.processAllAvailable()
        require(query.lastProgress != null) // at least one batch processed after start
        val loadedProvidersMethod =
          PrivateMethod[mutable.HashMap[StateStoreProviderId, StateStoreProvider]](
            Symbol("loadedProviders"))
        val loadedProvidersMap = StateStore invokePrivate loadedProvidersMethod()
        val loadedProviders = loadedProvidersMap.synchronized { loadedProvidersMap.values.toSeq }
        query.stop()
        loadedProviders
      }

      val loadedProvidersAfterRun1 = runQueryAndGetLoadedProviders()
      require(loadedProvidersAfterRun1.length === 1)

      val loadedProvidersAfterRun2 = runQueryAndGetLoadedProviders()
      assert(loadedProvidersAfterRun2.length === 2)   // two providers loaded for 2 runs

      // Both providers should have the same StateStoreId, but the should be different objects
      assert(loadedProvidersAfterRun2(0).stateStoreId === loadedProvidersAfterRun2(1).stateStoreId)
      assert(loadedProvidersAfterRun2(0) ne loadedProvidersAfterRun2(1))

    } finally {
      SparkSession.getActiveSession.foreach { spark =>
        spark.streams.active.foreach(_.stop())
        spark.stop()
      }
    }
  }

  test("error writing [version].delta cancels the output stream") {

    val hadoopConf = new Configuration()
    hadoopConf.set(
      SQLConf.STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key,
      classOf[CreateAtomicTestManager].getName)
    val remoteDir = Utils.createTempDir().getAbsolutePath

    val provider = newStoreProvider(
      opId = Random.nextInt, partition = 0, dir = remoteDir, hadoopConf = hadoopConf)

    // Disable failure of output stream and generate versions
    CreateAtomicTestManager.shouldFailInCreateAtomic = false
    for (version <- 1 to 10) {
      val store = provider.getStore(version - 1)
      put(store, version.toString, version) // update "1" -> 1, "2" -> 2, ...
      store.commit()
    }
    val version10Data = (1L to 10).map(_.toString).map(x => x -> x).toSet

    CreateAtomicTestManager.cancelCalledInCreateAtomic = false
    val store = provider.getStore(10)
    // Fail commit for next version and verify that reloading resets the files
    CreateAtomicTestManager.shouldFailInCreateAtomic = true
    put(store, "11", 11)
    val e = intercept[IllegalStateException] { quietly { store.commit() } }
    assert(e.getCause.isInstanceOf[IOException])
    CreateAtomicTestManager.shouldFailInCreateAtomic = false

    // Abort commit for next version and verify that reloading resets the files
    CreateAtomicTestManager.cancelCalledInCreateAtomic = false
    val store2 = provider.getStore(10)
    put(store2, "11", 11)
    store2.abort()
    assert(CreateAtomicTestManager.cancelCalledInCreateAtomic)
  }

  test("expose metrics with custom metrics to StateStoreMetrics") {
    def getCustomMetric(metrics: StateStoreMetrics, name: String): Long = {
      val metricPair = metrics.customMetrics.find(_._1.name == name)
      assert(metricPair.isDefined)
      metricPair.get._2
    }

    def getLoadedMapSizeMetric(metrics: StateStoreMetrics): Long = {
      metrics.memoryUsedBytes
    }

    def assertCacheHitAndMiss(
        metrics: StateStoreMetrics,
        expectedCacheHitCount: Long,
        expectedCacheMissCount: Long): Unit = {
      val cacheHitCount = getCustomMetric(metrics, "loadedMapCacheHitCount")
      val cacheMissCount = getCustomMetric(metrics, "loadedMapCacheMissCount")
      assert(cacheHitCount === expectedCacheHitCount)
      assert(cacheMissCount === expectedCacheMissCount)
    }

    val provider = newStoreProvider()

    // Verify state before starting a new set of updates
    assert(getLatestData(provider).isEmpty)

    val store = provider.getStore(0)
    assert(!store.hasCommitted)

    assert(store.metrics.numKeys === 0)

    val initialLoadedMapSize = getLoadedMapSizeMetric(store.metrics)
    assert(initialLoadedMapSize >= 0)
    assertCacheHitAndMiss(store.metrics, expectedCacheHitCount = 0, expectedCacheMissCount = 0)

    put(store, "a", 1)
    assert(store.metrics.numKeys === 1)

    put(store, "b", 2)
    put(store, "aa", 3)
    assert(store.metrics.numKeys === 3)
    remove(store, _.startsWith("a"))
    assert(store.metrics.numKeys === 1)
    assert(store.commit() === 1)

    assert(store.hasCommitted)

    val loadedMapSizeForVersion1 = getLoadedMapSizeMetric(store.metrics)
    assert(loadedMapSizeForVersion1 > initialLoadedMapSize)
    assertCacheHitAndMiss(store.metrics, expectedCacheHitCount = 0, expectedCacheMissCount = 0)

    val storeV2 = provider.getStore(1)
    assert(!storeV2.hasCommitted)
    assert(storeV2.metrics.numKeys === 1)

    put(storeV2, "cc", 4)
    assert(storeV2.metrics.numKeys === 2)
    assert(storeV2.commit() === 2)

    assert(storeV2.hasCommitted)

    val loadedMapSizeForVersion1And2 = getLoadedMapSizeMetric(storeV2.metrics)
    assert(loadedMapSizeForVersion1And2 > loadedMapSizeForVersion1)
    assertCacheHitAndMiss(storeV2.metrics, expectedCacheHitCount = 1, expectedCacheMissCount = 0)

    val reloadedProvider = newStoreProvider(store.id)
    // intended to load version 2 instead of 1
    // version 2 will not be loaded to the cache in provider
    val reloadedStore = reloadedProvider.getStore(1)
    assert(reloadedStore.metrics.numKeys === 1)

    assert(getLoadedMapSizeMetric(reloadedStore.metrics) === loadedMapSizeForVersion1)
    assertCacheHitAndMiss(reloadedStore.metrics, expectedCacheHitCount = 0,
      expectedCacheMissCount = 1)

    // now we are loading version 2
    val reloadedStoreV2 = reloadedProvider.getStore(2)
    assert(reloadedStoreV2.metrics.numKeys === 2)

    assert(getLoadedMapSizeMetric(reloadedStoreV2.metrics) > loadedMapSizeForVersion1)
    assertCacheHitAndMiss(reloadedStoreV2.metrics, expectedCacheHitCount = 0,
      expectedCacheMissCount = 2)
  }

  override def newStoreProvider(): HDFSBackedStateStoreProvider = {
    newStoreProvider(opId = Random.nextInt(), partition = 0)
  }

  override def newStoreProvider(storeId: StateStoreId): HDFSBackedStateStoreProvider = {
    newStoreProvider(storeId.operatorId, storeId.partitionId, dir = storeId.checkpointRootLocation)
  }

  override def getLatestData(storeProvider: HDFSBackedStateStoreProvider): Set[(String, Int)] = {
    getData(storeProvider)
  }

  override def getData(
    provider: HDFSBackedStateStoreProvider,
    version: Int = -1): Set[(String, Int)] = {
    val reloadedProvider = newStoreProvider(provider.stateStoreId)
    if (version < 0) {
      reloadedProvider.latestIterator().map(rowsToStringInt).toSet
    } else {
      reloadedProvider.getStore(version).iterator().map(rowsToStringInt).toSet
    }
  }

  def newStoreProvider(
      opId: Long,
      partition: Int,
      dir: String = newDir(),
      minDeltasForSnapshot: Int = SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.defaultValue.get,
      numOfVersToRetainInMemory: Int = SQLConf.MAX_BATCHES_TO_RETAIN_IN_MEMORY.defaultValue.get,
      hadoopConf: Configuration = new Configuration): HDFSBackedStateStoreProvider = {
    val sqlConf = new SQLConf()
    sqlConf.setConf(SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT, minDeltasForSnapshot)
    sqlConf.setConf(SQLConf.MAX_BATCHES_TO_RETAIN_IN_MEMORY, numOfVersToRetainInMemory)
    sqlConf.setConf(SQLConf.MIN_BATCHES_TO_RETAIN, 2)
    sqlConf.setConf(SQLConf.STATE_STORE_COMPRESSION_CODEC, SQLConf.get.stateStoreCompressionCodec)
    val provider = new HDFSBackedStateStoreProvider()
    provider.init(
      StateStoreId(dir, opId, partition),
      keySchema,
      valueSchema,
      indexOrdinal = None,
      new StateStoreConf(sqlConf),
      hadoopConf)
    provider
  }

  def fileExists(
      provider: HDFSBackedStateStoreProvider,
      version: Long,
      isSnapshot: Boolean): Boolean = {
    val method = PrivateMethod[Path](Symbol("baseDir"))
    val basePath = provider invokePrivate method()
    val fileName = if (isSnapshot) s"$version.snapshot" else s"$version.delta"
    val filePath = new File(basePath.toString, fileName)
    filePath.exists
  }

  def deleteFilesEarlierThanVersion(provider: HDFSBackedStateStoreProvider, version: Long): Unit = {
    val method = PrivateMethod[Path](Symbol("baseDir"))
    val basePath = provider invokePrivate method()
    for (version <- 0 until version.toInt) {
      for (isSnapshot <- Seq(false, true)) {
        val fileName = if (isSnapshot) s"$version.snapshot" else s"$version.delta"
        val filePath = new File(basePath.toString, fileName)
        if (filePath.exists) filePath.delete()
      }
    }
  }

  def corruptFile(
    provider: HDFSBackedStateStoreProvider,
    version: Long,
    isSnapshot: Boolean): Unit = {
    val method = PrivateMethod[Path](Symbol("baseDir"))
    val basePath = provider invokePrivate method()
    val fileName = if (isSnapshot) s"$version.snapshot" else s"$version.delta"
    val filePath = new File(basePath.toString, fileName)
    filePath.delete()
    filePath.createNewFile()
  }
}

abstract class StateStoreSuiteBase[ProviderClass <: StateStoreProvider]
  extends StateStoreCodecsTest {
  import StateStoreTestsHelper._

  testWithAllCodec("get, put, remove, commit, and all data iterator") {
    val provider = newStoreProvider()

    // Verify state before starting a new set of updates
    assert(getLatestData(provider).isEmpty)

    val store = provider.getStore(0)
    assert(!store.hasCommitted)
    assert(get(store, "a") === None)
    assert(store.iterator().isEmpty)
    assert(store.metrics.numKeys === 0)

    // Verify state after updating
    put(store, "a", 1)
    assert(get(store, "a") === Some(1))

    assert(store.iterator().nonEmpty)
    assert(getLatestData(provider).isEmpty)

    // Make updates, commit and then verify state
    put(store, "b", 2)
    put(store, "aa", 3)
    remove(store, _.startsWith("a"))
    assert(store.commit() === 1)

    assert(store.hasCommitted)
    assert(rowsToSet(store.iterator()) === Set("b" -> 2))
    assert(getLatestData(provider) === Set("b" -> 2))

    // Trying to get newer versions should fail
    intercept[Exception] {
      provider.getStore(2)
    }
    intercept[Exception] {
      getData(provider, 2)
    }

    // New updates to the reloaded store with new version, and does not change old version
    val reloadedProvider = newStoreProvider(store.id)
    val reloadedStore = reloadedProvider.getStore(1)
    put(reloadedStore, "c", 4)
    assert(reloadedStore.commit() === 2)
    assert(rowsToSet(reloadedStore.iterator()) === Set("b" -> 2, "c" -> 4))
    assert(getLatestData(provider) === Set("b" -> 2, "c" -> 4))
    assert(getData(provider, version = 1) === Set("b" -> 2))
  }

  testWithAllCodec("numKeys metrics") {
    val provider = newStoreProvider()

    // Verify state before starting a new set of updates
    assert(getLatestData(provider).isEmpty)

    val store = provider.getStore(0)
    put(store, "a", 1)
    put(store, "b", 2)
    put(store, "c", 3)
    put(store, "d", 4)
    put(store, "e", 5)
    assert(store.commit() === 1)
    assert(store.metrics.numKeys === 5)
    assert(rowsToSet(store.iterator()) === Set("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5))

    val reloadedProvider = newStoreProvider(store.id)
    val reloadedStore = reloadedProvider.getStore(1)
    remove(reloadedStore, _ == "b")
    assert(reloadedStore.commit() === 2)
    assert(reloadedStore.metrics.numKeys === 4)
    assert(rowsToSet(reloadedStore.iterator()) === Set("a" -> 1, "c" -> 3, "d" -> 4, "e" -> 5))
  }

  testWithAllCodec("removing while iterating") {
    val provider = newStoreProvider()

    // Verify state before starting a new set of updates
    assert(getLatestData(provider).isEmpty)
    val store = provider.getStore(0)
    put(store, "a", 1)
    put(store, "b", 2)

    // Updates should work while iterating of filtered entries
    val filtered = store.iterator.filter { tuple => rowToString(tuple.key) == "a" }
    filtered.foreach { tuple =>
      store.put(tuple.key, intToRow(rowToInt(tuple.value) + 1))
    }
    assert(get(store, "a") === Some(2))

    // Removes should work while iterating of filtered entries
    val filtered2 = store.iterator.filter { tuple => rowToString(tuple.key) == "b" }
    filtered2.foreach { tuple => store.remove(tuple.key) }
    assert(get(store, "b") === None)
  }

  testWithAllCodec("abort") {
    val provider = newStoreProvider()
    val store = provider.getStore(0)
    put(store, "a", 1)
    store.commit()
    assert(rowsToSet(store.iterator()) === Set("a" -> 1))

    // cancelUpdates should not change the data in the files
    val store1 = provider.getStore(1)
    put(store1, "b", 1)
    store1.abort()
  }

  testWithAllCodec("getStore with invalid versions") {
    val provider = newStoreProvider()

    def checkInvalidVersion(version: Int): Unit = {
      intercept[Exception] {
        provider.getStore(version)
      }
    }

    checkInvalidVersion(-1)
    checkInvalidVersion(1)

    val store = provider.getStore(0)
    put(store, "a", 1)
    assert(store.commit() === 1)
    assert(rowsToSet(store.iterator()) === Set("a" -> 1))

    val store1_ = provider.getStore(1)
    assert(rowsToSet(store1_.iterator()) === Set("a" -> 1))

    checkInvalidVersion(-1)
    checkInvalidVersion(2)

    // Update store version with some data
    val store1 = provider.getStore(1)
    assert(rowsToSet(store1.iterator()) === Set("a" -> 1))
    put(store1, "b", 1)
    assert(store1.commit() === 2)
    assert(rowsToSet(store1.iterator()) === Set("a" -> 1, "b" -> 1))

    checkInvalidVersion(-1)
    checkInvalidVersion(3)
  }

  testWithAllCodec("two concurrent StateStores - one for read-only and one for read-write") {
    // During Streaming Aggregation, we have two StateStores per task, one used as read-only in
    // `StateStoreRestoreExec`, and one read-write used in `StateStoreSaveExec`. `StateStore.abort`
    // will be called for these StateStores if they haven't committed their results. We need to
    // make sure that `abort` in read-only store after a `commit` in the read-write store doesn't
    // accidentally lead to the deletion of state.
    val dir = newDir()
    val storeId = StateStoreId(dir, 0L, 1)
    val provider0 = newStoreProvider(storeId)
    // prime state
    val store = provider0.getStore(0)
    val key = "a"
    put(store, key, 1)
    store.commit()
    assert(rowsToSet(store.iterator()) === Set(key -> 1))

    // two state stores
    val provider1 = newStoreProvider(storeId)
    val restoreStore = provider1.getReadStore(1)
    val saveStore = provider1.getStore(1)

    put(saveStore, key, get(restoreStore, key).get + 1)
    saveStore.commit()
    restoreStore.abort()

    // check that state is correct for next batch
    val provider2 = newStoreProvider(storeId)
    val finalStore = provider2.getStore(2)
    assert(rowsToSet(finalStore.iterator()) === Set(key -> 2))
  }

  test("SPARK-34270: StateStoreMetrics.combine should not override individual metrics") {
    val customSumMetric = StateStoreCustomSumMetric("metric1", "custom metric 1")
    val customSizeMetric = StateStoreCustomSizeMetric("metric2", "custom metric 2")
    val customTimingMetric = StateStoreCustomTimingMetric("metric3", "custom metric 3")

    val leftCustomMetrics: Map[StateStoreCustomMetric, Long] =
      Map(customSumMetric -> 10L, customSizeMetric -> 5L, customTimingMetric -> 100L)
    val leftMetrics = StateStoreMetrics(1, 10, leftCustomMetrics)

    val rightCustomMetrics: Map[StateStoreCustomMetric, Long] =
      Map(customSumMetric -> 20L, customSizeMetric -> 15L, customTimingMetric -> 300L)
    val rightMetrics = StateStoreMetrics(3, 20, rightCustomMetrics)

    val combinedMetrics = StateStoreMetrics.combine(Seq(leftMetrics, rightMetrics))
    assert(combinedMetrics.numKeys == 4)
    assert(combinedMetrics.memoryUsedBytes == 30)
    assert(combinedMetrics.customMetrics.size == 3)
    assert(combinedMetrics.customMetrics(customSumMetric) == 30L)
    assert(combinedMetrics.customMetrics(customSizeMetric) == 20L)
    assert(combinedMetrics.customMetrics(customTimingMetric) == 400L)
  }

  /** Return a new provider with a random id */
  def newStoreProvider(): ProviderClass

  /** Return a new provider with the given id */
  def newStoreProvider(storeId: StateStoreId): ProviderClass

  /** Get the latest data referred to by the given provider but not using this provider */
  def getLatestData(storeProvider: ProviderClass): Set[(String, Int)]

  /**
   * Get a specific version of data referred to by the given provider but not using
   * this provider
   */
  def getData(storeProvider: ProviderClass, version: Int): Set[(String, Int)]

  protected def testQuietly(name: String)(f: => Unit): Unit = {
    test(name) {
      quietly {
        f
      }
    }
  }
}

object StateStoreTestsHelper {

  val strProj = UnsafeProjection.create(Array[DataType](StringType))
  val intProj = UnsafeProjection.create(Array[DataType](IntegerType))

  def stringToRow(s: String): UnsafeRow = {
    strProj.apply(new GenericInternalRow(Array[Any](UTF8String.fromString(s)))).copy()
  }

  def intToRow(i: Int): UnsafeRow = {
    intProj.apply(new GenericInternalRow(Array[Any](i))).copy()
  }

  def rowToString(row: UnsafeRow): String = {
    row.getUTF8String(0).toString
  }

  def rowToInt(row: UnsafeRow): Int = {
    row.getInt(0)
  }

  def rowsToStringInt(row: UnsafeRowPair): (String, Int) = {
    (rowToString(row.key), rowToInt(row.value))
  }

  def rowsToSet(iterator: Iterator[UnsafeRowPair]): Set[(String, Int)] = {
    iterator.map(rowsToStringInt).toSet
  }

  def remove(store: StateStore, condition: String => Boolean): Unit = {
    store.getRange(None, None).foreach { rowPair =>
      if (condition(rowToString(rowPair.key))) store.remove(rowPair.key)
    }
  }

  def put(store: StateStore, key: String, value: Int): Unit = {
    store.put(stringToRow(key), intToRow(value))
  }

  def get(store: ReadStateStore, key: String): Option[Int] = {
    Option(store.get(stringToRow(key))).map(rowToInt)
  }

  def newDir(): String = Utils.createTempDir().toString
}

/**
 * Fake FileSystem that simulates HDFS rename semantic, i.e. renaming a file atop an existing
 * one should return false.
 * See hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/filesystem/filesystem.html
 */
class RenameLikeHDFSFileSystem extends RawLocalFileSystem {
  override def rename(src: Path, dst: Path): Boolean = {
    if (exists(dst)) {
      return false
    } else {
      return super.rename(src, dst)
    }
  }
}

/**
 * Fake FileSystem to test that the StateStore throws an exception while committing the
 * delta file, when `fs.rename` returns `false`.
 */
class RenameReturnsFalseFileSystem extends RawLocalFileSystem {
  import RenameReturnsFalseFileSystem._
  override def getUri: URI = {
    URI.create(s"$scheme:///")
  }

  override def rename(src: Path, dst: Path): Boolean = false
}

object RenameReturnsFalseFileSystem {
  val scheme = s"StateStoreSuite${math.abs(Random.nextInt)}fs"
}
