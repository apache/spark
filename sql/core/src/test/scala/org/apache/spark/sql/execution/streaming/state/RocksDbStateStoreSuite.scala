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
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.{BeforeAndAfter, PrivateMethodTester}
import org.scalatest.concurrent.Eventually.{eventually, timeout}
import org.scalatest.time.SpanSugar._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.LocalSparkContext.withSpark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.util.Utils

class RocksDbStateStoreSuite
    extends StateStoreSuiteBase[RocksDbStateStoreProvider]
    with BeforeAndAfter
    with PrivateMethodTester {
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
      rocksDbWriteInstance: RocksDbInstance,
      count: Int,
      earliestKey: Long,
      latestKey: Long): Unit = {
    assert(rocksDbWriteInstance.iterator(false).length === count)
  }

  def checkVersion(
      rocksDbWriteInstance: RocksDbInstance,
      version: Long,
      expectedData: Map[String, Int]): Unit = {

    val originValueMap = rocksDbWriteInstance
      .iterator(false)
      .map { row =>
        rowToString(row.key) -> rowToInt(row.value)
      }
      .toMap[String, Int]

    assert(originValueMap === expectedData)
  }

  test("get, put, remove, commit, and all data iterator") {
    val provider = newStoreProvider()

    // Verify state before starting a new set of updates
    assert(getLatestData(provider).isEmpty)

    val store = provider.getStore(0)
    assert(!store.hasCommitted)
    assert(get(store, "a") === None)
    assert(store.iterator().isEmpty)

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
    val reloadedProvider = newStoreProvider(store.id, provider.getLocalDirectory)
    val reloadedStore = reloadedProvider.getStore(1)
    put(reloadedStore, "c", 4)
    assert(reloadedStore.commit() === 2)
    assert(rowsToSet(reloadedStore.iterator()) === Set("b" -> 2, "c" -> 4))
    assert(getLatestData(provider) === Set("b" -> 2, "c" -> 4))
    assert(getData(provider, version = 1) === Set("b" -> 2))
  }

  test("snapshotting") {
    val provider =
      newStoreProvider(opId = Random.nextInt, partition = 0, minDeltasForSnapshot = 5)

    var currentVersion = 0

    currentVersion = updateVersionTo(provider, currentVersion, 2)
    require(getData(provider) === Set("a" -> 2))
    provider.doMaintenance() // should not generate snapshot files
    assert(getData(provider) === Set("a" -> 2))

    for (i <- 1 to currentVersion) {
      assert(fileExists(provider, i, isSnapshot = false)) // all delta files present
      assert(!fileExists(provider, i, isSnapshot = true)) // no snapshot files present
    }

    // After version 6, snapshotting should generate one snapshot file
    currentVersion = updateVersionTo(provider, currentVersion, 6)
    require(getData(provider) === Set("a" -> 6), "store not updated correctly")
    provider.doMaintenance() // should generate snapshot files

    val snapshotVersion =
      (0 to 6).find(version => fileExists(provider, version, isSnapshot = true))
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
    provider.doMaintenance() // do snapshot

    val latestSnapshotVersion =
      (0 to 20).filter(version => fileExists(provider, version, isSnapshot = true)).lastOption
    assert(latestSnapshotVersion.nonEmpty, "no snapshot file found")
    assert(latestSnapshotVersion.get > snapshotVersion.get, "newer snapshot not generated")

    deleteFilesEarlierThanVersion(provider, latestSnapshotVersion.get)
    assert(getData(provider) === Set("a" -> 20), "snapshotting messed up the data")
  }

  test("cleaning") {
    val provider =
      newStoreProvider(opId = Random.nextInt, partition = 0, minDeltasForSnapshot = 5)

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
    val tempFiles = FileUtils
      .listFiles(new File(provider.stateStoreId.checkpointRootLocation), null, true)
      .asScala
      .filter(_.getName.startsWith("temp-"))
    assert(tempFiles.isEmpty)
  }

  test("corrupted file handling") {
    val provider =
      newStoreProvider(opId = Random.nextInt, partition = 0, minDeltasForSnapshot = 5)
    for (i <- 1 to 6) {
      val store = provider.getStore(i - 1)
      put(store, "a", i)
      store.commit()
      provider.doMaintenance() // do cleanup
    }
    val snapshotVersion = (0 to 10)
      .find(version => fileExists(provider, version, isSnapshot = true))
      .getOrElse(fail("snapshot file not found"))

    // Corrupt snapshot file and verify that it throws error
    provider.close()
    assert(getData(provider, snapshotVersion) === Set("a" -> snapshotVersion))
    RocksDbInstance.destroyDB(provider.rocksDbPath)

    corruptFile(provider, snapshotVersion, isSnapshot = true)
    intercept[Exception] {
      provider.close()
      RocksDbInstance.destroyDB(provider.rocksDbPath)
      getData(provider, snapshotVersion)
    }

    // Corrupt delta file and verify that it throws error
    provider.close()
    RocksDbInstance.destroyDB(provider.rocksDbPath)
    assert(getData(provider, snapshotVersion - 1) === Set("a" -> (snapshotVersion - 1)))

    corruptFile(provider, snapshotVersion - 1, isSnapshot = false)
    intercept[Exception] {
      provider.close()
      RocksDbInstance.destroyDB(provider.rocksDbPath)
      getData(provider, snapshotVersion - 1)
    }

    // Delete delta file and verify that it throws error
    deleteFilesEarlierThanVersion(provider, snapshotVersion)
    intercept[Exception] {
      provider.close()
      RocksDbInstance.destroyDB(provider.rocksDbPath)
      getData(provider, snapshotVersion - 1)
    }
  }

  test("StateStore.get") {
    quietly {
      val dir = newDir()
      val storeId = StateStoreProviderId(StateStoreId(dir, 0, 0), UUID.randomUUID)
      val sqlConf = new SQLConf
      sqlConf.setConfString(
        SQLConf.STATE_STORE_PROVIDER_CLASS.key,
        "org.apache.spark.sql.execution.streaming.state.RocksDbStateStoreProvider")
      val localdir = Utils.createTempDir().getAbsoluteFile.toString
      sqlConf.setConfString("spark.sql.streaming.stateStore.rocksDb.localDirectory", localdir)
      val storeConf = new StateStoreConf(sqlConf)
      assert(
        storeConf.providerClass ===
          "org.apache.spark.sql.execution.streaming.state.RocksDbStateStoreProvider")
      val hadoopConf = new Configuration()

      // Verify that trying to get incorrect versions throw errors
      intercept[IllegalArgumentException] {
        StateStore.get(storeId, keySchema, valueSchema, None, -1, storeConf, hadoopConf)
      }
      assert(!StateStore.isLoaded(storeId)) // version -1 should not attempt to load the store

      intercept[IllegalStateException] {
        StateStore.get(storeId, keySchema, valueSchema, None, 1, storeConf, hadoopConf)
      }

      // Increase version of the store and try to get again
      val store0 = StateStore.get(storeId, keySchema, valueSchema, None, 0, storeConf, hadoopConf)
      assert(store0.version === 0)
      put(store0, "a", 1)
      store0.commit()

      val store1 = StateStore.get(storeId, keySchema, valueSchema, None, 1, storeConf, hadoopConf)
      assert(StateStore.isLoaded(storeId))
      assert(store1.version === 1)
      assert(rowsToSet(store1.iterator()) === Set("a" -> 1))

      // Verify that you can also load older version
      val store0reloaded =
        StateStore.get(storeId, keySchema, valueSchema, None, 0, storeConf, hadoopConf)
      assert(store0reloaded.version === 0)
      assert(rowsToSet(store0reloaded.iterator()) === Set.empty)

      // Verify that you can remove the store and still reload and use it
      StateStore.unload(storeId)
      assert(!StateStore.isLoaded(storeId))

      val store1reloaded =
        StateStore.get(storeId, keySchema, valueSchema, None, 1, storeConf, hadoopConf)
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
      // Make maintenance thread do snapshots and cleanups very fast
      .set(StateStore.MAINTENANCE_INTERVAL_CONFIG, "10ms")
      // Make sure that when SparkContext stops, the StateStore maintenance thread 'quickly'
      // fails to talk to the StateStoreCoordinator and unloads all the StateStores
      .set("spark.rpc.numRetries", "1")
    val opId = 0
    val dir = newDir()
    val storeProviderId = StateStoreProviderId(StateStoreId(dir, opId, 0), UUID.randomUUID)
    val sqlConf = new SQLConf()
    sqlConf.setConfString(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key,
      "org.apache.spark.sql.execution.streaming.state.RocksDbStateStoreProvider")
    sqlConf.setConf(SQLConf.MIN_BATCHES_TO_RETAIN, 2)
    sqlConf.setConfString(
      "spark.sql.streaming.stateStore.rocksDb.localDirectory",
      Utils.createTempDir().getAbsoluteFile.toString)
    val storeConf = StateStoreConf(sqlConf)
    val hadoopConf = new Configuration()
    val provider = newStoreProvider(storeProviderId.storeId)

    var latestStoreVersion = 0

    def generateStoreVersions() {
      for (i <- 1 to 20) {
        val store = StateStore.get(
          storeProviderId,
          keySchema,
          valueSchema,
          None,
          latestStoreVersion,
          storeConf,
          hadoopConf)
        put(store, "a", i)
        store.commit()
        latestStoreVersion += 1
      }
    }

    val timeoutDuration = 60 seconds

    quietly {
      withSpark(new SparkContext(conf)) { sc =>
        withCoordinatorRef(sc) { coordinatorRef =>
          require(!StateStore.isMaintenanceRunning, "StateStore is unexpectedly running")

          // Generate sufficient versions of store for snapshots
          generateStoreVersions()

          eventually(timeout(timeoutDuration)) {
            // Store should have been reported to the coordinator
            assert(
              coordinatorRef.getLocation(storeProviderId).nonEmpty,
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
          StateStore.get(
            storeProviderId,
            keySchema,
            valueSchema,
            indexOrdinal = None,
            latestStoreVersion,
            storeConf,
            hadoopConf)
          assert(StateStore.isLoaded(storeProviderId))

          // If some other executor loads the store, then this instance should be unloaded
          coordinatorRef.reportActiveInstance(storeProviderId, "other-host", "other-exec")
          eventually(timeout(timeoutDuration)) {
            assert(!StateStore.isLoaded(storeProviderId))
          }

          // Reload the store and verify
          StateStore.get(
            storeProviderId,
            keySchema,
            valueSchema,
            indexOrdinal = None,
            latestStoreVersion,
            storeConf,
            hadoopConf)
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

  test("SPARK-21145: Restarted queries create new provider instances") {
    try {
      val checkpointLocation = Utils.createTempDir().getAbsoluteFile
      val spark = SparkSession.builder().master("local[2]").getOrCreate()
      SparkSession.setActiveSession(spark)
      implicit val sqlContext = spark.sqlContext
      spark.conf.set("spark.sql.shuffle.partitions", "1")
      spark.conf.set(
        SQLConf.STATE_STORE_PROVIDER_CLASS.key,
        "org.apache.spark.sql.execution.streaming.state.RocksDbStateStoreProvider")
      spark.conf.set(
        "spark.sql.streaming.stateStore.rocksDb.localDirectory",
        Utils.createTempDir().getAbsoluteFile.toString)
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
            'loadedProviders)
        val loadedProvidersMap = StateStore invokePrivate loadedProvidersMethod()
        val loadedProviders = loadedProvidersMap.synchronized { loadedProvidersMap.values.toSeq }
        query.stop()
        loadedProviders
      }

      val loadedProvidersAfterRun1 = runQueryAndGetLoadedProviders()
      require(loadedProvidersAfterRun1.length === 1)

      val loadedProvidersAfterRun2 = runQueryAndGetLoadedProviders()
      assert(loadedProvidersAfterRun2.length === 2) // two providers loaded for 2 runs

      // Both providers should have the same StateStoreId, but the should be different objects
      assert(
        loadedProvidersAfterRun2(0).stateStoreId === loadedProvidersAfterRun2(1).stateStoreId)
      assert(loadedProvidersAfterRun2(0) ne loadedProvidersAfterRun2(1))

    } finally {
      SparkSession.getActiveSession.foreach { spark =>
        spark.streams.active.foreach(_.stop())
        spark.stop()
      }
    }
  }

  override def newStoreProvider(): RocksDbStateStoreProvider = {
    newStoreProvider(opId = Random.nextInt(), partition = 0)
  }

  override def newStoreProvider(storeId: StateStoreId): RocksDbStateStoreProvider = {
    newStoreProvider(
      storeId.operatorId,
      storeId.partitionId,
      dir = storeId.checkpointRootLocation)
  }

  def newStoreProvider(storeId: StateStoreId, localDir: String): RocksDbStateStoreProvider = {
    newStoreProvider(
      storeId.operatorId,
      storeId.partitionId,
      dir = storeId.checkpointRootLocation,
      localDir = localDir)
  }

  override def getLatestData(storeProvider: RocksDbStateStoreProvider): Set[(String, Int)] = {
    getData(storeProvider)
  }

  override def getData(
      provider: RocksDbStateStoreProvider,
      version: Int = -1): Set[(String, Int)] = {
    val reloadedProvider = newStoreProvider(provider.stateStoreId, provider.getLocalDirectory)
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
      localDir: String = newDir(),
      minDeltasForSnapshot: Int = SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.defaultValue.get,
      numOfVersToRetainInMemory: Int = SQLConf.MAX_BATCHES_TO_RETAIN_IN_MEMORY.defaultValue.get,
      hadoopConf: Configuration = new Configuration): RocksDbStateStoreProvider = {
    val sqlConf = new SQLConf()
    sqlConf.setConf(SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT, minDeltasForSnapshot)
    sqlConf.setConf(SQLConf.MAX_BATCHES_TO_RETAIN_IN_MEMORY, numOfVersToRetainInMemory)
    sqlConf.setConf(SQLConf.MIN_BATCHES_TO_RETAIN, 2)
    sqlConf.setConfString("spark.sql.streaming.stateStore.rocksDb.localDirectory", localDir)
    val provider = new RocksDbStateStoreProvider
    provider.init(
      StateStoreId(dir, opId, partition),
      keySchema,
      valueSchema,
      keyIndexOrdinal = None,
      new StateStoreConf(sqlConf),
      hadoopConf)
    provider
  }

  def fileExists(
      provider: RocksDbStateStoreProvider,
      version: Long,
      isSnapshot: Boolean): Boolean = {
    val method = PrivateMethod[Path]('baseDir)
    val basePath = provider invokePrivate method()
    val fileName = if (isSnapshot) s"$version.snapshot" else s"$version.delta"
    val filePath = new File(basePath.toString, fileName)
    filePath.exists
  }

  def deleteFilesEarlierThanVersion(provider: RocksDbStateStoreProvider, version: Long): Unit = {
    val method = PrivateMethod[Path]('baseDir)
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
      provider: RocksDbStateStoreProvider,
      version: Long,
      isSnapshot: Boolean): Unit = {
    val method = PrivateMethod[Path]('baseDir)
    val basePath = provider invokePrivate method()
    val fileName = if (isSnapshot) s"$version.snapshot" else s"$version.delta"
    val filePath = new File(basePath.toString, fileName)
    filePath.delete()
    filePath.createNewFile()
  }

}
