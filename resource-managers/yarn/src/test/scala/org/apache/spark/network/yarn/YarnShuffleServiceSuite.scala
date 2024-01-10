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
package org.apache.spark.network.yarn

import java.io.{DataOutputStream, File, FileOutputStream, IOException}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission._
import java.util.EnumSet

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import com.codahale.metrics.MetricSet
import com.fasterxml.jackson.databind.ObjectMapper
import com. fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.fs.Path
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem
import org.apache.hadoop.service.ServiceStateException
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.api.{ApplicationInitializationContext, ApplicationTerminationContext}
import org.mockito.Mockito.{mock, when}
import org.roaringbitmap.RoaringBitmap
import org.scalatest.concurrent.Eventually._
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.SecurityManager
import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.config._
import org.apache.spark.network.server.BlockPushNonFatalFailure
import org.apache.spark.network.shuffle.{Constants, MergedShuffleFileManager, NoOpMergedShuffleFileManager, RemoteBlockPushResolver, ShuffleTestAccessor}
import org.apache.spark.network.shuffle.RemoteBlockPushResolver._
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo
import org.apache.spark.network.shuffledb.DBBackend
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.network.util.TransportConf
import org.apache.spark.network.yarn.util.HadoopConfigProvider
import org.apache.spark.tags.ExtendedLevelDBTest
import org.apache.spark.util.Utils

abstract class YarnShuffleServiceSuite extends SparkFunSuite with Matchers {

  private[yarn] var yarnConfig: YarnConfiguration = null
  private[yarn] val SORT_MANAGER = "org.apache.spark.shuffle.sort.SortShuffleManager"
  private[yarn] val SORT_MANAGER_WITH_MERGE_SHUFFLE_META_WithAttemptID1 =
    "org.apache.spark.shuffle.sort.SortShuffleManager:" +
      "{\"mergeDir\": \"merge_manager_1\", \"attemptId\": \"1\"}"
  private[yarn] val SORT_MANAGER_WITH_MERGE_SHUFFLE_META_WithAttemptID2 =
    "org.apache.spark.shuffle.sort.SortShuffleManager:" +
      "{\"mergeDir\": \"merge_manager_2\", \"attemptId\": \"2\"}"
  private[yarn] val SORT_MANAGER_WITH_MERGE_SHUFFLE_META_WithNoAttemptID =
    "org.apache.spark.shuffle.sort.SortShuffleManager:{\"mergeDir\": \"merge_manager\"}"
  private val DUMMY_BLOCK_DATA = "dummyBlockData".getBytes(StandardCharsets.UTF_8)
  private val DUMMY_PASSWORD = "dummyPassword"
  private val EMPTY_PASSWORD = ""

  private var recoveryLocalDir: File = _
  protected var tempDir: File = _

  protected def shuffleDBBackend(): DBBackend

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Ensure that each test uses a fresh metrics system
    DefaultMetricsSystem.shutdown()
    DefaultMetricsSystem.setInstance(new MetricsSystemImpl())
    yarnConfig = new YarnConfiguration()
    yarnConfig.set(YarnConfiguration.NM_AUX_SERVICES, "spark_shuffle")
    yarnConfig.set(YarnConfiguration.NM_AUX_SERVICE_FMT.format("spark_shuffle"),
      classOf[YarnShuffleService].getCanonicalName)
    yarnConfig.setInt(SHUFFLE_SERVICE_PORT.key, 0)
    yarnConfig.setBoolean(YarnShuffleService.STOP_ON_FAILURE_KEY, true)
    val localDir = Utils.createTempDir()
    yarnConfig.set(YarnConfiguration.NM_LOCAL_DIRS, localDir.getAbsolutePath)
    yarnConfig.set("spark.shuffle.push.server.mergedShuffleFileManagerImpl",
      "org.apache.spark.network.shuffle.RemoteBlockPushResolver")
    yarnConfig.set(SHUFFLE_SERVICE_DB_BACKEND.key, shuffleDBBackend().name())

    recoveryLocalDir = Utils.createTempDir()
    tempDir = Utils.createTempDir()
  }

  var s1: YarnShuffleService = null
  var s2: YarnShuffleService = null
  var s3: YarnShuffleService = null

  override def afterEach(): Unit = {
    try {
      if (s1 != null) {
        s1.stop()
        s1 = null
      }
      if (s2 != null) {
        s2.stop()
        s2 = null
      }
      if (s3 != null) {
        s3.stop()
        s3 = null
      }
    } finally {
      super.afterEach()
    }
  }

  private def prepareAppShufflePartition(
      mergeManager: RemoteBlockPushResolver,
      partitionId: AppAttemptShuffleMergeId,
      reduceId: Int,
      blockId: String): AppShufflePartitionInfo = {
    val dataFile = ShuffleTestAccessor.getMergedShuffleDataFile(mergeManager, partitionId, reduceId)
    dataFile.getParentFile.mkdirs()
    val indexFile =
      ShuffleTestAccessor.getMergedShuffleIndexFile(mergeManager, partitionId, reduceId)
    indexFile.getParentFile.mkdirs()
    val metaFile = ShuffleTestAccessor.getMergedShuffleMetaFile(mergeManager, partitionId, reduceId)
    metaFile.getParentFile.mkdirs()
    val partitionInfo = ShuffleTestAccessor.getOrCreateAppShufflePartitionInfo(
      mergeManager, partitionId, reduceId, blockId)

    val (dataChannel, mergeMetaFile, mergeIndexFile) =
      ShuffleTestAccessor.getPartitionFileHandlers(partitionInfo)
    for (chunkId <- 1 to 5) {
      (0 until 4).foreach(_ => dataChannel.write(ByteBuffer.wrap(DUMMY_BLOCK_DATA)))
      mergeIndexFile.getDos.writeLong(chunkId * 4 * DUMMY_BLOCK_DATA.length - 1)
      val bitmap = new RoaringBitmap
      for (j <- (chunkId - 1) * 10 until chunkId * 10) {
        bitmap.add(j)
      }
      bitmap.serialize(mergeMetaFile.getDos())
    }
    dataChannel.write(ByteBuffer.wrap(DUMMY_BLOCK_DATA))
    ShuffleTestAccessor.closePartitionFiles(partitionInfo)

    partitionInfo
  }

  private def createYarnShuffleService(init: Boolean = true): YarnShuffleService = {
    val shuffleService = new YarnShuffleService
    shuffleService.setRecoveryPath(new Path(recoveryLocalDir.toURI))
    shuffleService._conf = yarnConfig
    if (init) {
      shuffleService.init(yarnConfig)
    }
    shuffleService
  }

  private def createYarnShuffleServiceWithCustomMergeManager(
      createMergeManager: (TransportConf, File) => MergedShuffleFileManager): YarnShuffleService = {
    val shuffleService = createYarnShuffleService(false)
    val dBBackend = shuffleDBBackend()
    val transportConf = new TransportConf("shuffle", new HadoopConfigProvider(yarnConfig))
    val dbName = dBBackend.fileName(YarnShuffleService.SPARK_SHUFFLE_MERGE_RECOVERY_FILE_NAME)
    val testShuffleMergeManager = createMergeManager(
        transportConf,
        shuffleService.initRecoveryDb(dbName))
    shuffleService.setShuffleMergeManager(testShuffleMergeManager)
    shuffleService.init(yarnConfig)
    shuffleService
  }

  test("executor and merged shuffle state kept across NM restart") {
    // set auth to true to test the secrets recovery
    yarnConfig.setBoolean(SecurityManager.SPARK_AUTH_CONF, true)
    s1 = createYarnShuffleService()
    val app1Id = ApplicationId.newInstance(0, 1)
    val app1Data = makeAppInfo("user", app1Id)
    s1.initializeApplication(app1Data)
    val app2Id = ApplicationId.newInstance(0, 2)
    val app2Data = makeAppInfo("user", app2Id)
    s1.initializeApplication(app2Data)
    val app3Id = ApplicationId.newInstance(0, 3)
    val app3Data = makeAppInfo("user", app3Id)
    s1.initializeApplication(app3Data)
    val app4Id = ApplicationId.newInstance(0, 4)
    val app4Data = makeAppInfo("user", app4Id, metadataStorageDisabled = false,
        authEnabled = true, DUMMY_PASSWORD)
    s1.initializeApplication(app4Data)

    val execStateFile = s1.registeredExecutorFile
    execStateFile should not be (null)
    val secretsFile = s1.secretsFile
    secretsFile should not be (null)
    val mergeMgrFile = s1.mergeManagerFile
    mergeMgrFile should not be (null)
    val shuffleInfo1 = new ExecutorShuffleInfo(Array("/foo", "/bar"), 3, SORT_MANAGER)
    val shuffleInfo2 = new ExecutorShuffleInfo(Array("/bippy"), 5, SORT_MANAGER)
    val mergedShuffleInfo3 =
      new ExecutorShuffleInfo(
        Array(new File(tempDir, "foo/foo").getAbsolutePath,
          new File(tempDir, "bar/bar").getAbsolutePath), 3,
        SORT_MANAGER_WITH_MERGE_SHUFFLE_META_WithAttemptID1)
    val mergedShuffleInfo4 =
      new ExecutorShuffleInfo(Array(new File(tempDir, "bippy/bippy").getAbsolutePath),
        5, SORT_MANAGER_WITH_MERGE_SHUFFLE_META_WithAttemptID1)

    val blockHandler = s1.blockHandler
    val blockResolver = ShuffleTestAccessor.getBlockResolver(blockHandler)
    ShuffleTestAccessor.registeredExecutorFile(blockResolver) should be (execStateFile)

    val mergeManager = s1.shuffleMergeManager.asInstanceOf[RemoteBlockPushResolver]
    ShuffleTestAccessor.recoveryFile(mergeManager) should be (mergeMgrFile)

    blockResolver.registerExecutor(app1Id.toString, "exec-1", shuffleInfo1)
    blockResolver.registerExecutor(app2Id.toString, "exec-2", shuffleInfo2)
    blockResolver.registerExecutor(app3Id.toString, "exec-3", mergedShuffleInfo3)
    blockResolver.registerExecutor(app4Id.toString, "exec-4", mergedShuffleInfo4)
    ShuffleTestAccessor.getExecutorInfo(app1Id, "exec-1", blockResolver) should
      be (Some(shuffleInfo1))
    ShuffleTestAccessor.getExecutorInfo(app2Id, "exec-2", blockResolver) should
      be (Some(shuffleInfo2))
    ShuffleTestAccessor.getExecutorInfo(app3Id, "exec-3", blockResolver) should
      be (Some(mergedShuffleInfo3))
    ShuffleTestAccessor.getExecutorInfo(app4Id, "exec-4", blockResolver) should
      be (Some(mergedShuffleInfo4))

    mergeManager.registerExecutor(app3Id.toString, mergedShuffleInfo3)
    mergeManager.registerExecutor(app4Id.toString, mergedShuffleInfo4)

    val localDirs3 = Array(new File(tempDir, "foo/merge_manager_1").getAbsolutePath,
      new File(tempDir, "bar/merge_manager_1").getAbsolutePath)
    val localDirs4 = Array(new File(tempDir, "bippy/merge_manager_1").getAbsolutePath)
    val appPathsInfo3 = new AppPathsInfo(localDirs3, 3)
    val appPathsInfo4 = new AppPathsInfo(localDirs4, 5)

    ShuffleTestAccessor.getAppPathsInfo(app3Id.toString, mergeManager) should
      be (Some(appPathsInfo3))
    ShuffleTestAccessor.getAppPathsInfo(app4Id.toString, mergeManager) should
      be (Some(appPathsInfo4))

    val partitionId3 = new AppAttemptShuffleMergeId(app3Id.toString, 1, 1, 1)
    val partitionId4 = new AppAttemptShuffleMergeId(app4Id.toString, 1, 2, 1)
    prepareAppShufflePartition(mergeManager, partitionId3, 1, "3")
    prepareAppShufflePartition(mergeManager, partitionId4, 2, "4")

    if (!execStateFile.exists()) {
      @tailrec def findExistingParent(file: File): File = {
        if (file == null) file
        else if (file.exists()) file
        else findExistingParent(file.getParentFile())
      }
      val existingParent = findExistingParent(execStateFile)
      assert(false, s"$execStateFile does not exist -- closest existing parent is $existingParent")
    }
    assert(execStateFile.exists(), s"$execStateFile did not exist")
    assert(mergeMgrFile.exists(), s"$mergeMgrFile did not exist")

    // now we pretend the shuffle service goes down, and comes back up
    s1.stop()
    s2 = createYarnShuffleService()
    s2.secretsFile should be (secretsFile)
    s2.registeredExecutorFile should be (execStateFile)
    s2.mergeManagerFile should be (mergeMgrFile)

    val handler2 = s2.blockHandler
    val resolver2 = ShuffleTestAccessor.getBlockResolver(handler2)
    val mergeManager2 = s2.shuffleMergeManager.asInstanceOf[RemoteBlockPushResolver]

    // now we reinitialize only two of the apps, and expect yarn to tell us that the other two apps
    // were stopped during the restart
    s2.initializeApplication(app1Data)
    s2.initializeApplication(app3Data)
    s2.stopApplication(new ApplicationTerminationContext(app2Id))
    s2.stopApplication(new ApplicationTerminationContext(app4Id))
    ShuffleTestAccessor.getExecutorInfo(app1Id, "exec-1", resolver2) should be (Some(shuffleInfo1))
    ShuffleTestAccessor.getExecutorInfo(app2Id, "exec-2", resolver2) should be (None)
    ShuffleTestAccessor
      .getExecutorInfo(app3Id, "exec-3", resolver2) should be (Some(mergedShuffleInfo3))
    ShuffleTestAccessor.getExecutorInfo(app4Id, "exec-4", resolver2) should be (None)
    ShuffleTestAccessor
      .getAppPathsInfo(app3Id.toString, mergeManager2) should be (Some(appPathsInfo3))
    ShuffleTestAccessor.getAppPathsInfo(app4Id.toString, mergeManager2) should be (None)

    val dataFileReload3 =
      ShuffleTestAccessor.getMergedShuffleDataFile(mergeManager2, partitionId3, 1)
    dataFileReload3.length() should be ((4 * 5 + 1) * DUMMY_BLOCK_DATA.length)

    // Regenerate the merge partitions as it was not finalized before the restart
    prepareAppShufflePartition(mergeManager2, partitionId3, 1, "3")
    // Finalize shuffle merge for partitionId3
    ShuffleTestAccessor.finalizeShuffleMerge(mergeManager2, partitionId3)

    // Act like the NM restarts one more time
    s2.stop()
    s3 = createYarnShuffleService()
    s3.registeredExecutorFile should be (execStateFile)
    s3.secretsFile should be (secretsFile)
    s3.mergeManagerFile should be (mergeMgrFile)

    val handler3 = s3.blockHandler
    val resolver3 = ShuffleTestAccessor.getBlockResolver(handler3)
    val mergeManager3 = s3.shuffleMergeManager.asInstanceOf[RemoteBlockPushResolver]

    // app1 and app3 are still running
    s3.initializeApplication(app1Data)
    ShuffleTestAccessor.getExecutorInfo(app1Id, "exec-1", resolver3) should be (Some(shuffleInfo1))
    ShuffleTestAccessor.getExecutorInfo(app2Id, "exec-2", resolver3) should be (None)
    ShuffleTestAccessor
      .getExecutorInfo(app3Id, "exec-3", resolver3) should be (Some(mergedShuffleInfo3))
    ShuffleTestAccessor.getExecutorInfo(app4Id, "exec-4", resolver3) should be (None)
    ShuffleTestAccessor
      .getAppPathsInfo(app3Id.toString, mergeManager3) should be (Some(appPathsInfo3))
    ShuffleTestAccessor.getAppPathsInfo(app4Id.toString, mergeManager3) should be (None)

    val error = intercept[BlockPushNonFatalFailure] {
      ShuffleTestAccessor.getOrCreateAppShufflePartitionInfo(
        mergeManager3, partitionId3, 2, "3")
    }
    assert(error.getMessage.contains("is finalized"))

    val dataFileReload3Again =
      ShuffleTestAccessor.getMergedShuffleDataFile(mergeManager3, partitionId3, 1)
    dataFileReload3Again.length() should be ((4 * 5 + 1) * DUMMY_BLOCK_DATA.length)
    s3.stop()
  }

  test("removed applications should not be in registered executor file and merged shuffle file") {
    s1 = createYarnShuffleServiceWithCustomMergeManager(
      ShuffleTestAccessor.createMergeManagerWithSynchronizedCleanup)
    val secretsFile = s1.secretsFile
    secretsFile should be (null)
    val app1Id = ApplicationId.newInstance(0, 1)
    val app1Data = makeAppInfo("user", app1Id)
    s1.initializeApplication(app1Data)
    val app2Id = ApplicationId.newInstance(0, 2)
    val app2Data = makeAppInfo("user", app2Id)
    s1.initializeApplication(app2Data)
    val app3Id = ApplicationId.newInstance(0, 3)
    val app3Data = makeAppInfo("user", app3Id)
    s1.initializeApplication(app3Data)
    val app4Id = ApplicationId.newInstance(0, 4)
    val app4Data = makeAppInfo("user", app4Id)
    s1.initializeApplication(app4Data)

    val execStateFile = s1.registeredExecutorFile
    execStateFile should not be (null)
    val shuffleInfo1 = new ExecutorShuffleInfo(Array("/foo", "/bar"), 3, SORT_MANAGER)
    val shuffleInfo2 = new ExecutorShuffleInfo(Array("/bippy"), 5, SORT_MANAGER)
    val mergedShuffleInfo3 =
      new ExecutorShuffleInfo(
        Array(new File(tempDir, "foo/foo").getAbsolutePath,
          new File(tempDir, "bar/bar").getAbsolutePath),
      3, SORT_MANAGER_WITH_MERGE_SHUFFLE_META_WithAttemptID1)
    val mergedShuffleInfo4 =
      new ExecutorShuffleInfo(Array(new File(tempDir, "bippy/bippy").getAbsolutePath),
        5, SORT_MANAGER_WITH_MERGE_SHUFFLE_META_WithAttemptID1)

    val blockHandler = s1.blockHandler
    val blockResolver = ShuffleTestAccessor.getBlockResolver(blockHandler)
    ShuffleTestAccessor.registeredExecutorFile(blockResolver) should be (execStateFile)

    val mergeMgrFile = s1.mergeManagerFile
    val mergeManager = s1.shuffleMergeManager.asInstanceOf[RemoteBlockPushResolver]
    ShuffleTestAccessor.recoveryFile(mergeManager) should be (mergeMgrFile)

    blockResolver.registerExecutor(app1Id.toString, "exec-1", shuffleInfo1)
    blockResolver.registerExecutor(app2Id.toString, "exec-2", shuffleInfo2)
    blockResolver.registerExecutor(app3Id.toString, "exec-3", mergedShuffleInfo3)
    blockResolver.registerExecutor(app4Id.toString, "exec-4", mergedShuffleInfo4)
    mergeManager.registerExecutor(app3Id.toString, mergedShuffleInfo3)
    mergeManager.registerExecutor(app4Id.toString, mergedShuffleInfo4)

    val partitionId3 = new AppAttemptShuffleMergeId(app3Id.toString, 1, 1, 1)
    val partitionId4 = new AppAttemptShuffleMergeId(app4Id.toString, 1, 2, 1)
    prepareAppShufflePartition(mergeManager, partitionId3, 1, "3")
    prepareAppShufflePartition(mergeManager, partitionId4, 2, "4")

    val blockResolverDB = ShuffleTestAccessor.shuffleServiceDB(blockResolver)
    ShuffleTestAccessor.reloadRegisteredExecutors(blockResolverDB) should not be empty
    val mergeManagerDB = ShuffleTestAccessor.mergeManagerDB(mergeManager)
    ShuffleTestAccessor.reloadAppShuffleInfo(mergeManager, mergeManagerDB) should not be empty

    s1.stopApplication(new ApplicationTerminationContext(app1Id))
    ShuffleTestAccessor.reloadRegisteredExecutors(blockResolverDB) should not be empty
    ShuffleTestAccessor.reloadAppShuffleInfo(mergeManager, mergeManagerDB) should not be empty
    s1.stopApplication(new ApplicationTerminationContext(app2Id))
    s1.stopApplication(new ApplicationTerminationContext(app3Id))
    s1.stopApplication(new ApplicationTerminationContext(app4Id))
    ShuffleTestAccessor.reloadRegisteredExecutors(blockResolverDB) shouldBe empty
    ShuffleTestAccessor.reloadAppShuffleInfo(mergeManager, mergeManagerDB) shouldBe empty
  }

  test("shuffle service should be robust to corrupt registered executor file") {
    s1 = createYarnShuffleService()
    val app1Id = ApplicationId.newInstance(0, 1)
    val app1Data = makeAppInfo("user", app1Id)
    s1.initializeApplication(app1Data)

    val execStateFile = s1.registeredExecutorFile
    val shuffleInfo1 = new ExecutorShuffleInfo(Array("/foo", "/bar"), 3, SORT_MANAGER)

    val blockHandler = s1.blockHandler
    val blockResolver = ShuffleTestAccessor.getBlockResolver(blockHandler)
    ShuffleTestAccessor.registeredExecutorFile(blockResolver) should be (execStateFile)

    blockResolver.registerExecutor(app1Id.toString, "exec-1", shuffleInfo1)

    // now we pretend the shuffle service goes down, and comes back up.  But we'll also
    // make a corrupt registeredExecutor File
    s1.stop()

    execStateFile.listFiles().foreach{_.delete()}

    val out = new DataOutputStream(new FileOutputStream(s"$execStateFile/CURRENT"))
    out.writeInt(42)
    out.close()

    s2 = createYarnShuffleService()
    s2.registeredExecutorFile should be (execStateFile)

    val handler2 = s2.blockHandler
    val resolver2 = ShuffleTestAccessor.getBlockResolver(handler2)

    // we re-initialize app1, but since the file was corrupt there is nothing we can do about it ...
    s2.initializeApplication(app1Data)
    // however, when we initialize a totally new app2, everything is still happy
    val app2Id = ApplicationId.newInstance(0, 2)
    val app2Data = makeAppInfo("user", app2Id)
    s2.initializeApplication(app2Data)
    val shuffleInfo2 = new ExecutorShuffleInfo(Array("/bippy"), 5, SORT_MANAGER)
    resolver2.registerExecutor(app2Id.toString, "exec-2", shuffleInfo2)
    ShuffleTestAccessor.getExecutorInfo(app2Id, "exec-2", resolver2) should be (Some(shuffleInfo2))
    s2.stop()

    // another stop & restart should be fine though (e.g., we recover from previous corruption)
    s3 = createYarnShuffleService()
    s3.registeredExecutorFile should be (execStateFile)
    val handler3 = s3.blockHandler
    val resolver3 = ShuffleTestAccessor.getBlockResolver(handler3)

    s3.initializeApplication(app2Data)
    ShuffleTestAccessor.getExecutorInfo(app2Id, "exec-2", resolver3) should be (Some(shuffleInfo2))
    s3.stop()
  }

  test("get correct recovery path") {
    // Test recovery path is set outside the shuffle service, this is to simulate NM recovery
    // enabled scenario, where recovery path will be set by yarn.
    s1 = new YarnShuffleService
    val recoveryPath = new Path(Utils.createTempDir().toURI)
    s1.setRecoveryPath(recoveryPath)

    s1.init(yarnConfig)
    s1._recoveryPath should be (recoveryPath)
    s1.stop()
  }

  test("moving recovery file from NM local dir to recovery path") {
    // This is to test when Hadoop is upgrade to 2.5+ and NM recovery is enabled, we should move
    // old recovery file to the new path to keep compatibility

    // Simulate s1 is running on old version of Hadoop in which recovery file is in the NM local
    // dir.
    s1 = new YarnShuffleService
    s1.setRecoveryPath(new Path(yarnConfig.getTrimmedStrings(YarnConfiguration.NM_LOCAL_DIRS)(0)))
    // set auth to true to test the secrets recovery
    yarnConfig.setBoolean(SecurityManager.SPARK_AUTH_CONF, true)
    s1.init(yarnConfig)
    val app1Id = ApplicationId.newInstance(0, 1)
    val app1Data = makeAppInfo("user", app1Id)
    s1.initializeApplication(app1Data)
    val app2Id = ApplicationId.newInstance(0, 2)
    val app2Data = makeAppInfo("user", app2Id)
    s1.initializeApplication(app2Data)

    assert(s1.secretManager.getSecretKey(app1Id.toString()) != null)
    assert(s1.secretManager.getSecretKey(app2Id.toString()) != null)

    val execStateFile = s1.registeredExecutorFile
    execStateFile should not be (null)
    val secretsFile = s1.secretsFile
    secretsFile should not be (null)
    val shuffleInfo1 = new ExecutorShuffleInfo(Array("/foo", "/bar"), 3, SORT_MANAGER)
    val shuffleInfo2 = new ExecutorShuffleInfo(Array("/bippy"), 5, SORT_MANAGER)

    val blockHandler = s1.blockHandler
    val blockResolver = ShuffleTestAccessor.getBlockResolver(blockHandler)
    ShuffleTestAccessor.registeredExecutorFile(blockResolver) should be (execStateFile)

    blockResolver.registerExecutor(app1Id.toString, "exec-1", shuffleInfo1)
    blockResolver.registerExecutor(app2Id.toString, "exec-2", shuffleInfo2)
    ShuffleTestAccessor.getExecutorInfo(app1Id, "exec-1", blockResolver) should
      be (Some(shuffleInfo1))
    ShuffleTestAccessor.getExecutorInfo(app2Id, "exec-2", blockResolver) should
      be (Some(shuffleInfo2))

    assert(execStateFile.exists(), s"$execStateFile did not exist")

    s1.stop()

    // Simulate s2 is running on Hadoop 2.5+ with NM recovery is enabled.
    assert(execStateFile.exists())
    val recoveryPath = new Path(recoveryLocalDir.toURI)
    s2 = new YarnShuffleService
    s2.setRecoveryPath(recoveryPath)
    s2.init(yarnConfig)

    // Ensure that s2 has loaded known apps from the secrets db.
    assert(s2.secretManager.getSecretKey(app1Id.toString()) != null)
    assert(s2.secretManager.getSecretKey(app2Id.toString()) != null)

    val execStateFile2 = s2.registeredExecutorFile
    val secretsFile2 = s2.secretsFile

    recoveryPath.toString should be (new Path(execStateFile2.getParentFile.toURI).toString)
    recoveryPath.toString should be (new Path(secretsFile2.getParentFile.toURI).toString)
    eventually(timeout(10.seconds), interval(5.milliseconds)) {
      assert(!execStateFile.exists())
    }
    eventually(timeout(10.seconds), interval(5.milliseconds)) {
      assert(!secretsFile.exists())
    }

    val handler2 = s2.blockHandler
    val resolver2 = ShuffleTestAccessor.getBlockResolver(handler2)

    // now we reinitialize only one of the apps, and expect yarn to tell us that app2 was stopped
    // during the restart
    // Since recovery file is got from old path, so the previous state should be stored.
    s2.initializeApplication(app1Data)
    s2.stopApplication(new ApplicationTerminationContext(app2Id))
    ShuffleTestAccessor.getExecutorInfo(app1Id, "exec-1", resolver2) should be (Some(shuffleInfo1))
    ShuffleTestAccessor.getExecutorInfo(app2Id, "exec-2", resolver2) should be (None)

    s2.stop()
  }

  test("service throws error if cannot start") {
    // Set up a read-only local dir.
    val roDir = Utils.createTempDir()
    Files.setPosixFilePermissions(roDir.toPath(), EnumSet.of(OWNER_READ, OWNER_EXECUTE))

    // Try to start the shuffle service, it should fail.
    val service = new YarnShuffleService()
    service.setRecoveryPath(new Path(roDir.toURI))

    try {
      val error = intercept[ServiceStateException] {
        service.init(yarnConfig)
      }
      assert(error.getCause().isInstanceOf[IOException])
    } finally {
      service.stop()
      Files.setPosixFilePermissions(roDir.toPath(),
        EnumSet.of(OWNER_READ, OWNER_WRITE, OWNER_EXECUTE))
    }
  }

  test("Consistency in AppPathInfo between in-memory hashmap and the DB") {
    s1 = createYarnShuffleService()

    val app1Id = ApplicationId.newInstance(0, 1)
    val app1Data = makeAppInfo("user", app1Id)
    s1.initializeApplication(app1Data)
    val app2Attempt1Id = ApplicationId.newInstance(0, 2)
    val app2Attempt1Data = makeAppInfo("user", app2Attempt1Id)
    s1.initializeApplication(app2Attempt1Data)
    val app2Attempt2Id = ApplicationId.newInstance(0, 2)
    val app2Attempt2Data = makeAppInfo("user", app2Attempt2Id)
    s1.initializeApplication(app2Attempt2Data)
    val app3IdNoAttemptId = ApplicationId.newInstance(0, 3)
    val app3NoAttemptIdData = makeAppInfo("user", app3IdNoAttemptId)
    s1.initializeApplication(app3NoAttemptIdData)

    val mergeMgrFile = s1.mergeManagerFile
    mergeMgrFile should not be (null)
    val mergedShuffleInfo1 =
      new ExecutorShuffleInfo(
        Array(new File(tempDir, "foo/foo").getAbsolutePath,
          new File(tempDir, "bar/bar").getAbsolutePath), 3,
        SORT_MANAGER_WITH_MERGE_SHUFFLE_META_WithAttemptID1)
    val mergedShuffleInfo2Attempt1 =
      new ExecutorShuffleInfo(Array(new File(tempDir, "bippy1/bippy1").getAbsolutePath),
        5, SORT_MANAGER_WITH_MERGE_SHUFFLE_META_WithAttemptID1)
    val mergedShuffleInfo2Attempt2 =
      new ExecutorShuffleInfo(Array(new File(tempDir, "bippy2/bippy2").getAbsolutePath),
        5, SORT_MANAGER_WITH_MERGE_SHUFFLE_META_WithAttemptID2)
    val mergedShuffleInfo3NoAttemptId =
      new ExecutorShuffleInfo(
        Array(new File(tempDir, "foo/foo").getAbsolutePath,
          new File(tempDir, "bar/bar").getAbsolutePath),
      4, SORT_MANAGER_WITH_MERGE_SHUFFLE_META_WithNoAttemptID)

    val localDirs1 = Array(new File(tempDir, "foo/merge_manager_1").getAbsolutePath,
      new File(tempDir, "bar/merge_manager_1").getAbsolutePath)
    val localDirs2Attempt1 = Array(new File(tempDir, "bippy1/merge_manager_1").getAbsolutePath)
    val localDirs2Attempt2 = Array(new File(tempDir, "bippy2/merge_manager_2").getAbsolutePath)
    val localDirs3NoAttempt = Array(new File(tempDir, "foo/merge_manager").getAbsolutePath,
      new File(tempDir, "bar/merge_manager").getAbsolutePath)
    val appPathsInfo1 = new AppPathsInfo(localDirs1, 3)
    val appPathsInfo2Attempt1 = new AppPathsInfo(localDirs2Attempt1, 5)
    val appPathsInfo2Attempt2 = new AppPathsInfo(localDirs2Attempt2, 5)
    val appPathsInfo3NoAttempt = new AppPathsInfo(localDirs3NoAttempt, 4)

    val mergeManager1 = s1.shuffleMergeManager.asInstanceOf[RemoteBlockPushResolver]
    val mergeManager1DB = ShuffleTestAccessor.mergeManagerDB(mergeManager1)
    ShuffleTestAccessor.recoveryFile(mergeManager1) should be (mergeMgrFile)

    ShuffleTestAccessor.getAppsShuffleInfo(mergeManager1).size() equals 0
    ShuffleTestAccessor.reloadAppShuffleInfo(
      mergeManager1, mergeManager1DB).size() equals 0

    mergeManager1.registerExecutor(app1Id.toString, mergedShuffleInfo1)
    var appShuffleInfo = ShuffleTestAccessor.getAppsShuffleInfo(mergeManager1)
    appShuffleInfo.size() equals 1
    appShuffleInfo.get(app1Id.toString).getAppPathsInfo should be (appPathsInfo1)
    var appShuffleInfoAfterReload =
      ShuffleTestAccessor.reloadAppShuffleInfo(mergeManager1, mergeManager1DB)
    appShuffleInfoAfterReload.size() equals 1
    appShuffleInfoAfterReload.get(app1Id.toString).getAppPathsInfo should be (appPathsInfo1)

    mergeManager1.registerExecutor(app2Attempt1Id.toString, mergedShuffleInfo2Attempt1)
    appShuffleInfo = ShuffleTestAccessor.getAppsShuffleInfo(mergeManager1)
    appShuffleInfo.size() equals 2
    appShuffleInfo.get(app1Id.toString).getAppPathsInfo should be (appPathsInfo1)
    appShuffleInfo.get(
      app2Attempt1Id.toString).getAppPathsInfo should be (appPathsInfo2Attempt1)
    appShuffleInfoAfterReload =
      ShuffleTestAccessor.reloadAppShuffleInfo(mergeManager1, mergeManager1DB)
    appShuffleInfoAfterReload.size() equals 2
    appShuffleInfoAfterReload.get(app1Id.toString).getAppPathsInfo should be (appPathsInfo1)
    appShuffleInfoAfterReload.get(
      app2Attempt1Id.toString).getAppPathsInfo should be (appPathsInfo2Attempt1)

    mergeManager1.registerExecutor(app3IdNoAttemptId.toString, mergedShuffleInfo3NoAttemptId)
    appShuffleInfo = ShuffleTestAccessor.getAppsShuffleInfo(mergeManager1)
    appShuffleInfo.size() equals 3
    appShuffleInfo.get(app1Id.toString).getAppPathsInfo should be (appPathsInfo1)
    appShuffleInfo.get(
      app2Attempt1Id.toString).getAppPathsInfo should be (appPathsInfo2Attempt1)
    appShuffleInfo.get(
      app3IdNoAttemptId.toString).getAppPathsInfo should be (appPathsInfo3NoAttempt)
    appShuffleInfoAfterReload =
      ShuffleTestAccessor.reloadAppShuffleInfo(mergeManager1, mergeManager1DB)
    appShuffleInfoAfterReload.size() equals 3
    appShuffleInfoAfterReload.get(app1Id.toString).getAppPathsInfo should be (appPathsInfo1)
    appShuffleInfoAfterReload.get(
      app2Attempt1Id.toString).getAppPathsInfo should be (appPathsInfo2Attempt1)
    appShuffleInfoAfterReload.get(
      app3IdNoAttemptId.toString).getAppPathsInfo should be (appPathsInfo3NoAttempt)

    mergeManager1.registerExecutor(app2Attempt2Id.toString, mergedShuffleInfo2Attempt2)
    appShuffleInfo = ShuffleTestAccessor.getAppsShuffleInfo(mergeManager1)
    appShuffleInfo.size() equals 3
    appShuffleInfo.get(app1Id.toString).getAppPathsInfo should be (appPathsInfo1)
    appShuffleInfo.get(
      app2Attempt2Id.toString).getAppPathsInfo should be (appPathsInfo2Attempt2)
    appShuffleInfo.get(
      app3IdNoAttemptId.toString).getAppPathsInfo should be (appPathsInfo3NoAttempt)
    appShuffleInfoAfterReload =
      ShuffleTestAccessor.reloadAppShuffleInfo(mergeManager1, mergeManager1DB)
    appShuffleInfoAfterReload.size() equals 3
    appShuffleInfoAfterReload.get(app1Id.toString).getAppPathsInfo should be (appPathsInfo1)
    appShuffleInfoAfterReload.get(
      app2Attempt2Id.toString).getAppPathsInfo should be (appPathsInfo2Attempt2)
    appShuffleInfoAfterReload.get(
      app3IdNoAttemptId.toString).getAppPathsInfo should be (appPathsInfo3NoAttempt)

    mergeManager1.applicationRemoved(app2Attempt2Id.toString, true)
    appShuffleInfo = ShuffleTestAccessor.getAppsShuffleInfo(mergeManager1)
    appShuffleInfo.size() equals 2
    appShuffleInfo.get(app1Id.toString).getAppPathsInfo should be (appPathsInfo1)
    assert(!appShuffleInfo.containsKey(app2Attempt2Id.toString))
    appShuffleInfo.get(
      app3IdNoAttemptId.toString).getAppPathsInfo should be (appPathsInfo3NoAttempt)
    appShuffleInfoAfterReload =
      ShuffleTestAccessor.reloadAppShuffleInfo(mergeManager1, mergeManager1DB)
    appShuffleInfoAfterReload.size() equals 2
    appShuffleInfoAfterReload.get(app1Id.toString).getAppPathsInfo should be (appPathsInfo1)
    assert(!appShuffleInfoAfterReload.containsKey(app2Attempt2Id.toString))
    appShuffleInfoAfterReload.get(
      app3IdNoAttemptId.toString).getAppPathsInfo should be (appPathsInfo3NoAttempt)

    s1.stop()
  }

  test("Finalized merged shuffle are written into DB and cleaned up after application stopped") {
    s1 = createYarnShuffleService()

    val app1Id = ApplicationId.newInstance(0, 1)
    val app1Data = makeAppInfo("user", app1Id)
    s1.initializeApplication(app1Data)
    val app2Attempt1Id = ApplicationId.newInstance(0, 2)
    val app2Attempt1Data = makeAppInfo("user", app2Attempt1Id)
    s1.initializeApplication(app2Attempt1Data)

    val mergeMgrFile = s1.mergeManagerFile
    mergeMgrFile should not be (null)
    val mergedShuffleInfo1 =
      new ExecutorShuffleInfo(
        Array(new File(tempDir, "foo/foo").getAbsolutePath,
          new File(tempDir, "bar/bar").getAbsolutePath), 3,
        SORT_MANAGER_WITH_MERGE_SHUFFLE_META_WithAttemptID1)
    val mergedShuffleInfo2Attempt1 =
      new ExecutorShuffleInfo(Array(new File(tempDir, "bippy1/bippy1").getAbsolutePath),
        5, SORT_MANAGER_WITH_MERGE_SHUFFLE_META_WithAttemptID1)

    val localDirs1 = Array(new File(tempDir, "foo/merge_manager_1").getAbsolutePath,
      new File(tempDir, "bar/merge_manager_1").getAbsolutePath)
    val localDirs2Attempt1 = Array(new File(tempDir, "bippy1/merge_manager_1").getAbsolutePath)
    val appPathsInfo1 = new AppPathsInfo(localDirs1, 3)
    val appPathsInfo2Attempt1 = new AppPathsInfo(localDirs2Attempt1, 5)

    val mergeManager1 = s1.shuffleMergeManager.asInstanceOf[RemoteBlockPushResolver]
    val mergeManager1DB = ShuffleTestAccessor.mergeManagerDB(mergeManager1)
    ShuffleTestAccessor.recoveryFile(mergeManager1) should be (mergeMgrFile)

    ShuffleTestAccessor.getAppsShuffleInfo(mergeManager1).size() equals 0
    ShuffleTestAccessor.reloadAppShuffleInfo(
      mergeManager1, mergeManager1DB).size() equals 0

    mergeManager1.registerExecutor(app1Id.toString, mergedShuffleInfo1)
    mergeManager1.registerExecutor(app2Attempt1Id.toString, mergedShuffleInfo2Attempt1)
    val partitionId1 = new AppAttemptShuffleMergeId(app1Id.toString, 1, 1, 1)
    val partitionId2 = new AppAttemptShuffleMergeId(app2Attempt1Id.toString, 1, 2, 1)
    prepareAppShufflePartition(mergeManager1, partitionId1, 1, "3")
    prepareAppShufflePartition(mergeManager1, partitionId2, 2, "4")

    var appShuffleInfo = ShuffleTestAccessor.getAppsShuffleInfo(mergeManager1)
    appShuffleInfo.size() equals 2
    appShuffleInfo.get(app1Id.toString).getAppPathsInfo should be (appPathsInfo1)
    appShuffleInfo.get(
      app2Attempt1Id.toString).getAppPathsInfo should be (appPathsInfo2Attempt1)
    assert(!appShuffleInfo.get(app1Id.toString).getShuffles.get(1).isFinalized)
    assert(!appShuffleInfo.get(app2Attempt1Id.toString).getShuffles.get(2).isFinalized)
    var appShuffleInfoAfterReload =
      ShuffleTestAccessor.reloadAppShuffleInfo(mergeManager1, mergeManager1DB)
    appShuffleInfoAfterReload.size() equals 2
    appShuffleInfoAfterReload.get(app1Id.toString).getAppPathsInfo should be (appPathsInfo1)
    appShuffleInfoAfterReload.get(
      app2Attempt1Id.toString).getAppPathsInfo should be (appPathsInfo2Attempt1)
    assert(appShuffleInfoAfterReload.get(app1Id.toString).getShuffles.isEmpty)
    assert(appShuffleInfoAfterReload.get(app2Attempt1Id.toString).getShuffles.isEmpty)

    ShuffleTestAccessor.finalizeShuffleMerge(mergeManager1, partitionId1)
    ShuffleTestAccessor.finalizeShuffleMerge(mergeManager1, partitionId2)

    appShuffleInfo = ShuffleTestAccessor.getAppsShuffleInfo(mergeManager1)
    assert(appShuffleInfo.get(app1Id.toString).getShuffles.get(1).isFinalized)
    assert(appShuffleInfo.get(app2Attempt1Id.toString).getShuffles.get(2).isFinalized)
    appShuffleInfoAfterReload =
      ShuffleTestAccessor.reloadAppShuffleInfo(mergeManager1, mergeManager1DB)
    assert(appShuffleInfoAfterReload.get(app1Id.toString).getShuffles.get(1).isFinalized)
    assert(appShuffleInfoAfterReload.get(app2Attempt1Id.toString).getShuffles.get(2).isFinalized)

    mergeManager1.applicationRemoved(app1Id.toString, true)
    appShuffleInfo = ShuffleTestAccessor.getAppsShuffleInfo(mergeManager1)
    appShuffleInfo.size() equals 1
    assert(!appShuffleInfo.containsKey(app1Id.toString))
    assert(appShuffleInfo.get(app2Attempt1Id.toString).getShuffles.get(2).isFinalized)
    appShuffleInfoAfterReload =
      ShuffleTestAccessor.reloadAppShuffleInfo(mergeManager1, mergeManager1DB)
    appShuffleInfoAfterReload.size() equals 1
    assert(!appShuffleInfoAfterReload.containsKey(app1Id.toString))
    assert(appShuffleInfoAfterReload.get(app2Attempt1Id.toString).getShuffles.get(2).isFinalized)

    s1.stop()
  }

  test("SPARK-40186: shuffleMergeManager should have been shutdown before db closed") {
    val maxId = 100
    s1 = createYarnShuffleService()
    val resolver = s1.shuffleMergeManager.asInstanceOf[RemoteBlockPushResolver]
    (0 until maxId).foreach { id =>
      val appId = ApplicationId.newInstance(0, id)
      val appInfo = makeAppInfo("user", appId)
      s1.initializeApplication(appInfo)
      val mergedShuffleInfo =
        new ExecutorShuffleInfo(
          Array(new File(tempDir, "foo/foo").getAbsolutePath,
            new File(tempDir, "bar/bar").getAbsolutePath), 3,
          SORT_MANAGER_WITH_MERGE_SHUFFLE_META_WithAttemptID1)
      resolver.registerExecutor(appId.toString, mergedShuffleInfo)
    }

    (0 until maxId).foreach { id =>
      val appId = ApplicationId.newInstance(0, id)
      resolver.applicationRemoved(appId.toString, true)
    }

    s1.stop()

    assert(ShuffleTestAccessor.isMergedShuffleCleanerShutdown(resolver))
  }

  test("Dangling finalized merged partition info in DB will be removed during restart") {
    s1 = createYarnShuffleServiceWithCustomMergeManager(
      ShuffleTestAccessor.createMergeManagerWithNoOpAppShuffleDBCleanup)

    val app1Id = ApplicationId.newInstance(0, 1)
    val app1Data = makeAppInfo("user", app1Id)
    s1.initializeApplication(app1Data)
    val app2Id = ApplicationId.newInstance(0, 2)
    val app2Attempt1Data = makeAppInfo("user", app2Id)
    s1.initializeApplication(app2Attempt1Data)

    val mergeMgrFile = s1.mergeManagerFile
    mergeMgrFile should not be (null)
    val mergedShuffleInfo1 =
      new ExecutorShuffleInfo(
        Array(new File(tempDir, "foo/foo").getAbsolutePath,
          new File(tempDir, "bar/bar").getAbsolutePath), 3,
        SORT_MANAGER_WITH_MERGE_SHUFFLE_META_WithAttemptID1)
    val mergedShuffleInfo2Attempt1 =
      new ExecutorShuffleInfo(Array(new File(tempDir, "bippy1/bippy1").getAbsolutePath),
        5, SORT_MANAGER_WITH_MERGE_SHUFFLE_META_WithAttemptID1)

    val localDirs1 = Array(new File(tempDir, "foo/merge_manager_1").getAbsolutePath,
      new File(tempDir, "bar/merge_manager_1").getAbsolutePath)
    val localDirs2Attempt1 = Array(new File(tempDir, "bippy1/merge_manager_1").getAbsolutePath)
    val localDirs2Attempt2 = Array(new File(tempDir, "bippy2/merge_manager_2").getAbsolutePath)
    val appPathsInfo1 = new AppPathsInfo(localDirs1, 3)
    val appPathsInfo2Attempt1 = new AppPathsInfo(localDirs2Attempt1, 5)

    val mergeManager1 = s1.shuffleMergeManager.asInstanceOf[RemoteBlockPushResolver]
    val mergeManager1DB = ShuffleTestAccessor.mergeManagerDB(mergeManager1)
    ShuffleTestAccessor.recoveryFile(mergeManager1) should be (mergeMgrFile)

    mergeManager1.registerExecutor(app1Id.toString, mergedShuffleInfo1)
    mergeManager1.registerExecutor(app2Id.toString, mergedShuffleInfo2Attempt1)
    val partitionId1 = new AppAttemptShuffleMergeId(app1Id.toString, 1, 1, 1)
    val partitionId2 = new AppAttemptShuffleMergeId(app2Id.toString, 1, 2, 1)
    prepareAppShufflePartition(mergeManager1, partitionId1, 1, "3")
    prepareAppShufflePartition(mergeManager1, partitionId2, 2, "4")

    var appShuffleInfo = ShuffleTestAccessor.getAppsShuffleInfo(mergeManager1)
    appShuffleInfo.size() equals 2
    appShuffleInfo.get(app1Id.toString).getAppPathsInfo should be (appPathsInfo1)
    appShuffleInfo.get(
      app2Id.toString).getAppPathsInfo should be (appPathsInfo2Attempt1)
    assert(!appShuffleInfo.get(app1Id.toString).getShuffles.get(1).isFinalized)
    assert(!appShuffleInfo.get(app2Id.toString).getShuffles.get(2).isFinalized)

    ShuffleTestAccessor.finalizeShuffleMerge(mergeManager1, partitionId1)
    ShuffleTestAccessor.finalizeShuffleMerge(mergeManager1, partitionId2)

    appShuffleInfo = ShuffleTestAccessor.getAppsShuffleInfo(mergeManager1)
    assert(appShuffleInfo.get(app1Id.toString).getShuffles.get(1).isFinalized)
    assert(appShuffleInfo.get(app2Id.toString).getShuffles.get(2).isFinalized)
    var appShuffleInfoAfterReload =
      ShuffleTestAccessor.reloadAppShuffleInfo(mergeManager1, mergeManager1DB)
    assert(appShuffleInfoAfterReload.get(app1Id.toString).getShuffles.get(1).isFinalized)
    assert(appShuffleInfoAfterReload.get(app2Id.toString).getShuffles.get(2).isFinalized)

    // The applicationRemove will not clean up the finalized merged shuffle partition in DB
    // as of the NoOp mergedShuffleFileManager removeAppShuffleInfoFromDB method
    mergeManager1.applicationRemoved(app1Id.toString, true)

    appShuffleInfo = ShuffleTestAccessor.getAppsShuffleInfo(mergeManager1)
    appShuffleInfo.size() equals 1
    assert(!appShuffleInfo.containsKey(app1Id.toString))
    assert(appShuffleInfo.get(app2Id.toString).getShuffles.get(2).isFinalized)
    // Clear the AppsShuffleInfo hashmap and reload the hashmap from DB
    appShuffleInfoAfterReload =
      ShuffleTestAccessor.reloadAppShuffleInfo(mergeManager1, mergeManager1DB)
    appShuffleInfoAfterReload.size() equals 1
    assert(!appShuffleInfoAfterReload.containsKey(app1Id.toString))
    assert(appShuffleInfoAfterReload.get(app2Id.toString).getShuffles.get(2).isFinalized)

    // Register application app1Id again and reload the DB again
    mergeManager1.registerExecutor(app1Id.toString, mergedShuffleInfo1)
    appShuffleInfo = ShuffleTestAccessor.getAppsShuffleInfo(mergeManager1)
    appShuffleInfo.size() equals 2
    appShuffleInfo.get(app1Id.toString).getAppPathsInfo should be (appPathsInfo1)
    assert(appShuffleInfo.get(app1Id.toString).getShuffles.isEmpty)
    assert(appShuffleInfo.get(app2Id.toString).getShuffles.get(2).isFinalized)
    appShuffleInfoAfterReload =
      ShuffleTestAccessor.reloadAppShuffleInfo(mergeManager1, mergeManager1DB)
    // The merged partition information for App1 should be empty as they have been removed from DB
    assert(appShuffleInfoAfterReload.get(app1Id.toString).getShuffles.isEmpty)
    assert(appShuffleInfoAfterReload.get(app2Id.toString).getShuffles.get(2).isFinalized)

    s1.stop()
  }

  test("Dangling application path or shuffle information in DB will be removed during restart") {
    s1 = createYarnShuffleServiceWithCustomMergeManager(
      ShuffleTestAccessor.createMergeManagerWithNoDBCleanup)

    val app1Id = ApplicationId.newInstance(0, 2)
    val app1Attempt1Data = makeAppInfo("user", app1Id)
    s1.initializeApplication(app1Attempt1Data)

    val mergeMgrFile = s1.mergeManagerFile
    mergeMgrFile should not be (null)

    val mergedShuffleInfo1Attempt1 =
      new ExecutorShuffleInfo(Array(new File(tempDir, "bippy1/bippy1").getAbsolutePath),
        5, SORT_MANAGER_WITH_MERGE_SHUFFLE_META_WithAttemptID1)
    val mergedShuffleInfo1Attempt2 =
      new ExecutorShuffleInfo(Array(new File(tempDir, "bippy2/bippy2").getAbsolutePath),
        5, SORT_MANAGER_WITH_MERGE_SHUFFLE_META_WithAttemptID2)

    val localDirs1Attempt1 = Array(new File(tempDir, "bippy1/merge_manager_1").getAbsolutePath)
    val localDirs1Attempt2 = Array(new File(tempDir, "bippy2/merge_manager_2").getAbsolutePath)
    val appPathsInfo1Attempt1 = new AppPathsInfo(localDirs1Attempt1, 5)
    val appPathsInfo1Attempt2 = new AppPathsInfo(localDirs1Attempt2, 5)

    val mergeManager1 = s1.shuffleMergeManager.asInstanceOf[RemoteBlockPushResolver]
    ShuffleTestAccessor.recoveryFile(mergeManager1) should be (mergeMgrFile)

    mergeManager1.registerExecutor(app1Id.toString, mergedShuffleInfo1Attempt1)
    val partitionId1 = new AppAttemptShuffleMergeId(app1Id.toString, 1, 2, 1)
    prepareAppShufflePartition(mergeManager1, partitionId1, 2, "4")

    var appShuffleInfo = ShuffleTestAccessor.getAppsShuffleInfo(mergeManager1)
    appShuffleInfo.size() equals 1
    appShuffleInfo.get(
      app1Id.toString).getAppPathsInfo should be (appPathsInfo1Attempt1)
    assert(!appShuffleInfo.get(app1Id.toString).getShuffles.get(2).isFinalized)
    ShuffleTestAccessor.finalizeShuffleMerge(mergeManager1, partitionId1)
    appShuffleInfo = ShuffleTestAccessor.getAppsShuffleInfo(mergeManager1)
    assert(appShuffleInfo.get(app1Id.toString).getShuffles.get(2).isFinalized)

    // Register Attempt 2
    mergeManager1.registerExecutor(app1Id.toString, mergedShuffleInfo1Attempt2)
    val partitionId2 = new AppAttemptShuffleMergeId(app1Id.toString, 2, 2, 1)
    prepareAppShufflePartition(mergeManager1, partitionId2, 2, "4")

    appShuffleInfo = ShuffleTestAccessor.getAppsShuffleInfo(mergeManager1)
    appShuffleInfo.size() equals 1
    appShuffleInfo.get(
      app1Id.toString).getAppPathsInfo should be (appPathsInfo1Attempt2)
    assert(!appShuffleInfo.get(app1Id.toString).getShuffles.get(2).isFinalized)
    ShuffleTestAccessor.finalizeShuffleMerge(mergeManager1, partitionId2)
    assert(appShuffleInfo.get(app1Id.toString).getShuffles.get(2).isFinalized)

    val partitionId2Attempt2 = new AppAttemptShuffleMergeId(app1Id.toString, 2, 2, 2)
    prepareAppShufflePartition(mergeManager1, partitionId2Attempt2, 2, "4")
    assert(!appShuffleInfo.get(app1Id.toString).getShuffles.get(2).isFinalized)
    ShuffleTestAccessor.finalizeShuffleMerge(mergeManager1, partitionId2Attempt2)
    assert(appShuffleInfo.get(app1Id.toString).getShuffles.get(2).isFinalized)

    // now we pretend the shuffle service goes down, since the DB deletion are NoOp,
    // it should have multiple app attempt local paths info and finalized merge info
    s1.stop()
    // Yarn shuffle service with custom mergeManager to confirm that DB has outdated data
    s2 = createYarnShuffleServiceWithCustomMergeManager(
      ShuffleTestAccessor.createMergeManagerWithNoCleanupAfterReload)
    val mergeManager2 = s2.shuffleMergeManager.asInstanceOf[RemoteBlockPushResolver]
    val mergeManager2DB = ShuffleTestAccessor.mergeManagerDB(mergeManager2)
    ShuffleTestAccessor.clearAppShuffleInfo(mergeManager2)
    assert(ShuffleTestAccessor.getOutdatedAppPathInfoCountDuringDBReload(
      mergeManager2, mergeManager2DB) == 1)
    assert(ShuffleTestAccessor.getOutdatedFinalizedShuffleCountDuringDBReload(
      mergeManager2, mergeManager2DB) == 1)
    s2.stop()

    // Yarn Shuffle service comes back up without custom mergeManager
    s3 = createYarnShuffleService()
    s3.mergeManagerFile should be (mergeMgrFile)

    val mergeManager3 = s3.shuffleMergeManager.asInstanceOf[RemoteBlockPushResolver]
    val mergeManager3DB = ShuffleTestAccessor.mergeManagerDB(mergeManager3)
    appShuffleInfo = ShuffleTestAccessor.getAppsShuffleInfo(mergeManager3)
    appShuffleInfo.size() equals 1
    appShuffleInfo.get(
      app1Id.toString).getAppPathsInfo should be (appPathsInfo1Attempt2)
    assert(appShuffleInfo.get(app1Id.toString).getShuffles.get(2).isFinalized)
    ShuffleTestAccessor.clearAppShuffleInfo(mergeManager3)
    assert(ShuffleTestAccessor.getOutdatedAppPathInfoCountDuringDBReload(
      mergeManager3, mergeManager3DB) == 0)
    assert(ShuffleTestAccessor.getOutdatedFinalizedShuffleCountDuringDBReload(
      mergeManager3, mergeManager3DB) == 0)

    s3.stop()
  }

  test("Cleanup for former attempts local path info should be triggered in applicationRemoved") {
    s1 = createYarnShuffleServiceWithCustomMergeManager(
      ShuffleTestAccessor.createMergeManagerWithNoDBCleanup)

    val app1Id = ApplicationId.newInstance(0, 1)
    val app1Attempt1Data = makeAppInfo("user", app1Id)
    s1.initializeApplication(app1Attempt1Data)

    val mergeMgrFile = s1.mergeManagerFile
    mergeMgrFile should not be (null)

    val mergedShuffleInfo1Attempt1 =
      new ExecutorShuffleInfo(Array(new File(tempDir, "bippy1/bippy1").getAbsolutePath),
        5, SORT_MANAGER_WITH_MERGE_SHUFFLE_META_WithAttemptID1)
    val mergedShuffleInfo1Attempt2 =
      new ExecutorShuffleInfo(Array(new File(tempDir, "bippy2/bippy2").getAbsolutePath),
        5, SORT_MANAGER_WITH_MERGE_SHUFFLE_META_WithAttemptID2)

    val localDirs1Attempt2 = Array(new File(tempDir, "bippy2/merge_manager_2").getAbsolutePath)
    val appPathsInfo1Attempt2 = new AppPathsInfo(localDirs1Attempt2, 5)

    val mergeManager1 = s1.shuffleMergeManager.asInstanceOf[RemoteBlockPushResolver]
    mergeManager1.registerExecutor(app1Id.toString, mergedShuffleInfo1Attempt1)

    // Register Attempt 2
    mergeManager1.registerExecutor(app1Id.toString, mergedShuffleInfo1Attempt2)

    val appShuffleInfo = ShuffleTestAccessor.getAppsShuffleInfo(mergeManager1)
    appShuffleInfo.size() equals 1
    appShuffleInfo.get(
      app1Id.toString).getAppPathsInfo should be (appPathsInfo1Attempt2)

    // now we pretend the shuffle service goes down, since the DB deletion are NoOp,
    // it should have multiple app attempt local paths info
    s1.stop()
    // Yarn Shuffle service comes back up without custom mergeManager
    s2 = createYarnShuffleServiceWithCustomMergeManager(
      ShuffleTestAccessor.createMergeManagerWithNoCleanupAfterReload)

    val mergeManager2 = s2.shuffleMergeManager.asInstanceOf[RemoteBlockPushResolver]
    val mergeManager2DB = ShuffleTestAccessor.mergeManagerDB(mergeManager2)
    ShuffleTestAccessor.clearAppShuffleInfo(mergeManager2)
    assert(ShuffleTestAccessor.getOutdatedAppPathInfoCountDuringDBReload(
      mergeManager2, mergeManager2DB) == 1)

    // ApplicationRemove should trigger DB cleanup
    mergeManager2.applicationRemoved(app1Id.toString, true)
    assert(ShuffleTestAccessor.getOutdatedAppPathInfoCountDuringDBReload(
      mergeManager2, mergeManager2DB) == 0)

    s2.stop()
  }

  private def makeAppInfo(user: String, appId: ApplicationId,
      metadataStorageDisabled: Boolean = false,
      authEnabled: Boolean = true,
      password: String = EMPTY_PASSWORD): ApplicationInitializationContext = {
    if (!metadataStorageDisabled) {
      new ApplicationInitializationContext(user, appId, JavaUtils.stringToBytes(password))
    } else {
      val payload = new mutable.HashMap[String, Object]()
      payload.put(YarnShuffleService.SPARK_SHUFFLE_SERVER_RECOVERY_DISABLED, java.lang.Boolean.TRUE)
      if (authEnabled) {
        payload.put(YarnShuffleService.SECRET_KEY, password)
      }
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      val jsonString = mapper.writeValueAsString(payload)
      new ApplicationInitializationContext(user, appId, JavaUtils.stringToBytes(jsonString))
    }
  }

  test("recovery db should not be created if NM recovery is not enabled") {
    s1 = new YarnShuffleService
    s1.init(yarnConfig)
    s1._recoveryPath should be (null)
    s1.registeredExecutorFile should be (null)
    s1.secretsFile should be (null)
  }

  test("SPARK-31646: metrics should be registered into Node Manager's metrics system") {
    s1 = new YarnShuffleService
    s1.init(yarnConfig)

    val metricsSource = DefaultMetricsSystem.instance.asInstanceOf[MetricsSystemImpl]
      .getSource("sparkShuffleService").asInstanceOf[YarnShuffleServiceMetrics]
    val metricSetRef = classOf[YarnShuffleServiceMetrics].getDeclaredField("metricSet")
    metricSetRef.setAccessible(true)
    val metrics = metricSetRef.get(metricsSource).asInstanceOf[MetricSet].getMetrics

    // Use sorted Seq instead of Set for easier comparison when there is a mismatch
    assert(metrics.keySet().asScala.toSeq.sorted == Seq(
      "blockTransferRate",
      "blockTransferMessageRate",
      "blockTransferRateBytes",
      "blockTransferAvgSize_1min",
      "numActiveConnections",
      "numCaughtExceptions",
      "numRegisteredConnections",
      "openBlockRequestLatencyMillis",
      "registeredExecutorsSize",
      "registerExecutorRequestLatencyMillis",
      "finalizeShuffleMergeLatencyMillis",
      "shuffle-server.usedDirectMemory",
      "shuffle-server.usedHeapMemory",
      "fetchMergedBlocksMetaLatencyMillis"
    ).sorted)
  }

  test("SPARK-34828: metrics should be registered with configured name") {
    s1 = new YarnShuffleService
    yarnConfig.set(YarnShuffleService.SPARK_SHUFFLE_SERVICE_METRICS_NAMESPACE_KEY, "fooMetrics")
    s1.init(yarnConfig)

    assert(DefaultMetricsSystem.instance.getSource("sparkShuffleService") === null)
    assert(DefaultMetricsSystem.instance.getSource("fooMetrics")
        .isInstanceOf[YarnShuffleServiceMetrics])
  }

  test("create default merged shuffle file manager instance") {
    val mockConf = mock(classOf[TransportConf])
    when(mockConf.mergedShuffleFileManagerImpl).thenReturn(
      "org.apache.spark.network.shuffle.NoOpMergedShuffleFileManager")
    val mergeMgr = YarnShuffleService.newMergedShuffleFileManagerInstance(mockConf, null)
    assert(mergeMgr.isInstanceOf[NoOpMergedShuffleFileManager])
  }

  test("create remote block push resolver instance") {
    val mockConf = mock(classOf[TransportConf])
    when(mockConf.get(Constants.SHUFFLE_SERVICE_DB_BACKEND, DBBackend.ROCKSDB.name()))
      .thenReturn(shuffleDBBackend().name())
    when(mockConf.mergedShuffleFileManagerImpl).thenReturn(
      "org.apache.spark.network.shuffle.RemoteBlockPushResolver")
    val mergeMgr = YarnShuffleService.newMergedShuffleFileManagerInstance(mockConf, null)
    assert(mergeMgr.isInstanceOf[RemoteBlockPushResolver])
  }

  test("invalid class name of merge manager will use noop instance") {
    val mockConf = mock(classOf[TransportConf])
    when(mockConf.mergedShuffleFileManagerImpl).thenReturn(
      "org.apache.spark.network.shuffle.NotExistent")
    val mergeMgr = YarnShuffleService.newMergedShuffleFileManagerInstance(mockConf, null)
    assert(mergeMgr.isInstanceOf[NoOpMergedShuffleFileManager])
  }

  test("secret of applications should not be stored in db if they want to be excluded") {
    // set auth to true to test the secrets recovery
    yarnConfig.setBoolean(SecurityManager.SPARK_AUTH_CONF, true)
    s1 = createYarnShuffleService()
    val app1Id = ApplicationId.newInstance(1681252509, 1)
    val app1Data = makeAppInfo("user", app1Id, metadataStorageDisabled = true,
        authEnabled = true, EMPTY_PASSWORD)
    s1.initializeApplication(app1Data)
    val app2Id = ApplicationId.newInstance(1681252509, 2)
    val app2Data = makeAppInfo("user", app2Id, metadataStorageDisabled = false,
        authEnabled = true, DUMMY_PASSWORD)
    s1.initializeApplication(app2Data)
    assert(s1.secretManager.getSecretKey(app1Id.toString()) == EMPTY_PASSWORD)
    assert(s1.secretManager.getSecretKey(app2Id.toString()) == DUMMY_PASSWORD)

    val execShuffleInfo1 =
      new ExecutorShuffleInfo(
        Array(new File(tempDir, "foo/foo").getAbsolutePath,
          new File(tempDir, "bar/bar").getAbsolutePath), 3,
        SORT_MANAGER_WITH_MERGE_SHUFFLE_META_WithAttemptID1)
    val execShuffleInfo2 =
      new ExecutorShuffleInfo(Array(new File(tempDir, "bippy/bippy").getAbsolutePath),
        3, SORT_MANAGER_WITH_MERGE_SHUFFLE_META_WithAttemptID1)

    val blockHandler = s1.blockHandler
    val blockResolver = ShuffleTestAccessor.getBlockResolver(blockHandler)
    blockResolver.registerExecutor(app1Id.toString, "exec-1", execShuffleInfo1)
    blockResolver.registerExecutor(app2Id.toString, "exec-2", execShuffleInfo2)
    ShuffleTestAccessor.getExecutorInfo(app1Id, "exec-1", blockResolver) should
      be(Some(execShuffleInfo1))
    ShuffleTestAccessor.getExecutorInfo(app2Id, "exec-2", blockResolver) should
      be(Some(execShuffleInfo2))

    val mergeManager = s1.shuffleMergeManager.asInstanceOf[RemoteBlockPushResolver]
    mergeManager.registerExecutor(app1Id.toString, execShuffleInfo1)
    mergeManager.registerExecutor(app2Id.toString, execShuffleInfo2)
    val localDirsApp1 = Array(new File(tempDir, "foo/merge_manager_1").getAbsolutePath,
      new File(tempDir, "bar/merge_manager_1").getAbsolutePath)
    val localDirsApp2 = Array(new File(tempDir, "bippy/merge_manager_1").getAbsolutePath)
    val appPathsInfo1 = new AppPathsInfo(localDirsApp1, 3)
    val appPathsInfo2 = new AppPathsInfo(localDirsApp2, 3)

    ShuffleTestAccessor.getAppPathsInfo(app1Id.toString, mergeManager) should
      be(Some(appPathsInfo1))
    ShuffleTestAccessor.getAppPathsInfo(app2Id.toString, mergeManager) should
      be(Some(appPathsInfo2))

    val partitionIdApp1 = new AppAttemptShuffleMergeId(app1Id.toString, 1, 1, 1)
    val partitionIdApp2 = new AppAttemptShuffleMergeId(app2Id.toString, 1, 2, 1)
    prepareAppShufflePartition(mergeManager, partitionIdApp1, 1, "3")
    prepareAppShufflePartition(mergeManager, partitionIdApp2, 2, "4")
    ShuffleTestAccessor.finalizeShuffleMerge(mergeManager, partitionIdApp1)
    ShuffleTestAccessor.finalizeShuffleMerge(mergeManager, partitionIdApp2)

    val execStateFile = s1.registeredExecutorFile
    assert(execStateFile.exists(), s"$execStateFile did not exist")
    val mergeMgrFile = s1.mergeManagerFile
    assert(mergeMgrFile.exists(), s"$mergeMgrFile did not exist")

    // shuffle service goes down
    s1.stop()
    // Yarn Shuffle service comes back up without custom mergeManager
    s2 = createYarnShuffleService()
    // Since secret of app1 is not saved in the db, it isn't recovered
    assert(s2.secretManager.getSecretKey(app1Id.toString()) == null)
    assert(s2.secretManager.getSecretKey(app2Id.toString()) == DUMMY_PASSWORD)

    val resolver2 = ShuffleTestAccessor.getBlockResolver(s2.blockHandler)
    val mergeManager2 = s2.shuffleMergeManager.asInstanceOf[RemoteBlockPushResolver]

    // App1 executor information should not have been saved in the db.
    ShuffleTestAccessor.getExecutorInfo(app1Id, "exec-1", resolver2) should be(None)
    ShuffleTestAccessor.getExecutorInfo(app2Id, "exec-2", resolver2) should be(
      Some(execShuffleInfo2))
    // App1 should not have any merge related metadata stored in the db.
    ShuffleTestAccessor
      .getAppPathsInfo(app1Id.toString, mergeManager2) should be(None)
    ShuffleTestAccessor.getAppPathsInfo(app2Id.toString, mergeManager2) should be(
      Some(appPathsInfo2))

    // Even though App1-partition1 was finalized before the restart, merge manager will recreate
    // the partition since it didn't have any metadata saved for that app.
    mergeManager2.registerExecutor(app1Id.toString, execShuffleInfo1)
    prepareAppShufflePartition(mergeManager2, partitionIdApp1, 1, "3")
    val dataFileApp1 =
      ShuffleTestAccessor.getMergedShuffleDataFile(mergeManager2, partitionIdApp1, 1)
    dataFileApp1.length() should be((4 * 5 + 1) * DUMMY_BLOCK_DATA.length)
    // Since app2-partition2 was metadata was saved, it cannot be re-opened.
    val error = intercept[BlockPushNonFatalFailure] {
      ShuffleTestAccessor.getOrCreateAppShufflePartitionInfo(
        mergeManager2, partitionIdApp2, 2, "3")
    }
    assert(error.getMessage.contains("is finalized"))

    s2.stopApplication(new ApplicationTerminationContext(app1Id))
    s2.stopApplication(new ApplicationTerminationContext(app2Id))
    s2.stop()
  }

  test("executor info of apps should not be stored in db if they want to be excluded. " +
    "Authentication is turned off") {
    s1 = createYarnShuffleService()
    val app1Id = ApplicationId.newInstance(1681252509, 1)
    val app1Data = makeAppInfo("user", app1Id, metadataStorageDisabled = true, authEnabled = false)
    s1.initializeApplication(app1Data)
    val execShuffleInfo1 =
      new ExecutorShuffleInfo(
        Array(new File(tempDir, "foo/foo").getAbsolutePath,
          new File(tempDir, "bar/bar").getAbsolutePath), 3, SORT_MANAGER)
    val blockHandler = s1.blockHandler
    val blockResolver = ShuffleTestAccessor.getBlockResolver(blockHandler)
    blockResolver.registerExecutor(app1Id.toString, "exec-1", execShuffleInfo1)
    ShuffleTestAccessor.getExecutorInfo(app1Id, "exec-1", blockResolver) should
      be(Some(execShuffleInfo1))
    // shuffle service goes down
    s1.stop()
    // Yarn Shuffle service comes back up without custom mergeManager
    s2 = createYarnShuffleService()
    val resolver2 = ShuffleTestAccessor.getBlockResolver(s2.blockHandler)
    // App1 executor information should not have been saved in the db.
    ShuffleTestAccessor.getExecutorInfo(app1Id, "exec-1", resolver2) should be(None)
    s2.stopApplication(new ApplicationTerminationContext(app1Id))
    s2.stop()
  }
}

@ExtendedLevelDBTest
class YarnShuffleServiceWithLevelDBBackendSuite extends YarnShuffleServiceSuite {
  override protected def shuffleDBBackend(): DBBackend = DBBackend.LEVELDB
}

class YarnShuffleServiceWithRocksDBBackendSuite extends YarnShuffleServiceSuite {
  override protected def shuffleDBBackend(): DBBackend = DBBackend.ROCKSDB
}
