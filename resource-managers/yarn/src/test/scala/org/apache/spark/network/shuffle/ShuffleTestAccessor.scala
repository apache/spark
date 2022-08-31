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
package org.apache.spark.network.shuffle

import java.io.File
import java.nio.channels.FileChannel
import java.util.List
import java.util.concurrent.ConcurrentMap

import org.apache.hadoop.yarn.api.records.ApplicationId

import org.apache.spark.network.shuffle.ExternalShuffleBlockResolver.AppExecId
import org.apache.spark.network.shuffle.RemoteBlockPushResolver._
import org.apache.spark.network.shuffle.protocol.{ExecutorShuffleInfo, FinalizeShuffleMerge}
import org.apache.spark.network.shuffledb.DB
import org.apache.spark.network.shuffledb.DBBackend
import org.apache.spark.network.util.{DBProvider, TransportConf}

/**
 * just a cheat to get package-visible members in tests
 */
object ShuffleTestAccessor {

  def getBlockResolver(handler: ExternalBlockHandler): ExternalShuffleBlockResolver = {
    handler.blockManager
  }

  def getExecutorInfo(
      appId: ApplicationId,
      execId: String,
      resolver: ExternalShuffleBlockResolver
  ): Option[ExecutorShuffleInfo] = {
    val id = new AppExecId(appId.toString, execId)
    Option(resolver.executors.get(id))
  }

  def getAppPathsInfo(
      appId: String,
      mergeManager: RemoteBlockPushResolver): Option[AppPathsInfo] = {
    Option(mergeManager.appsShuffleInfo.get(appId)).flatMap(v => Option(v.getAppPathsInfo))
  }

  def getAppsShuffleInfo(
    mergeManager: RemoteBlockPushResolver
  ): ConcurrentMap[String, RemoteBlockPushResolver.AppShuffleInfo] = {
    mergeManager.appsShuffleInfo
  }

  def registeredExecutorFile(resolver: ExternalShuffleBlockResolver): File = {
    resolver.registeredExecutorFile
  }

  def recoveryFile(mergeManager: RemoteBlockPushResolver): File = {
    mergeManager.recoveryFile
  }

  def shuffleServiceDB(resolver: ExternalShuffleBlockResolver): DB = {
    resolver.db
  }

  def mergeManagerLevelDB(mergeManager: RemoteBlockPushResolver): DB = {
    mergeManager.db
  }

  def createMergeManagerWithSynchronizedCleanup(
      transportConf: TransportConf,
      file: File): MergedShuffleFileManager = {
    new RemoteBlockPushResolver(transportConf, file) {
      override private[shuffle] def submitCleanupTask(task: Runnable): Unit = {
        task.run()
      }
    }
  }

  def createMergeManagerWithNoOpAppShuffleDBCleanup(
      transportConf: TransportConf,
      file: File): MergedShuffleFileManager = {
    new RemoteBlockPushResolver(transportConf, file) {
      override private[shuffle] def removeAppShuffleInfoFromDB(
          appShuffleInfo: RemoteBlockPushResolver.AppShuffleInfo): Unit = {
        // NoOp
      }
      override private[shuffle] def submitCleanupTask(task: Runnable): Unit = {
        task.run()
      }
    }
  }

  def createMergeManagerWithNoDBCleanup(
      transportConf: TransportConf,
      file: File): MergedShuffleFileManager = {
    new RemoteBlockPushResolver(transportConf, file) {
      override private[shuffle] def removeAppAttemptPathInfoFromDB(
        appId: String, attemptId: Int): Unit = {
        // NoOp
      }
      override private[shuffle] def removeAppShuffleInfoFromDB(
        appShuffleInfo: RemoteBlockPushResolver.AppShuffleInfo): Unit = {
        // NoOp
      }
      override private[shuffle] def submitCleanupTask(task: Runnable): Unit = {
        task.run()
      }
    }
  }

  def createMergeManagerWithNoCleanupAfterReload(
      transportConf: TransportConf,
      file: File): MergedShuffleFileManager = {
    new RemoteBlockPushResolver(transportConf, file) {
      override private[shuffle] def removeOutdatedKeyValuesInDB(
          dbKeysToBeRemoved: List[Array[Byte]]): Unit = {
        // NoOp
      }
    }
  }

  def getOrCreateAppShufflePartitionInfo(
      mergeManager: RemoteBlockPushResolver,
      appShufflePartitionId: AppAttemptShuffleMergeId,
      reduceId: Int,
      blockId: String): AppShufflePartitionInfo = {
    mergeManager.getOrCreateAppShufflePartitionInfo(
      mergeManager.appsShuffleInfo.get(appShufflePartitionId.appId),
      appShufflePartitionId.shuffleId, appShufflePartitionId.shuffleMergeId,
      reduceId, blockId)
  }

  def finalizeShuffleMerge(
      mergeManager: RemoteBlockPushResolver,
      appAttemptShuffleMergeId: AppAttemptShuffleMergeId): Unit = {
    mergeManager.finalizeShuffleMerge(
      new FinalizeShuffleMerge(
        appAttemptShuffleMergeId.appId, appAttemptShuffleMergeId.attemptId,
        appAttemptShuffleMergeId.shuffleId, appAttemptShuffleMergeId.shuffleMergeId))
  }

  def getMergedShuffleDataFile(
      mergeManager: RemoteBlockPushResolver,
      appShufflePartitionId: AppAttemptShuffleMergeId,
      reduceId: Int): File = {
    mergeManager.appsShuffleInfo.get(appShufflePartitionId.appId)
      .getMergedShuffleDataFile(appShufflePartitionId.shuffleId,
        appShufflePartitionId.shuffleMergeId, reduceId)
  }

  def getMergedShuffleIndexFile(
      mergeManager: RemoteBlockPushResolver,
      appShufflePartitionId: AppAttemptShuffleMergeId,
      reduceId: Int): File = {
    new File(mergeManager.appsShuffleInfo.get(appShufflePartitionId.appId)
      .getMergedShuffleIndexFilePath(appShufflePartitionId.shuffleId,
        appShufflePartitionId.shuffleMergeId, reduceId))
  }

  def getMergedShuffleMetaFile(
      mergeManager: RemoteBlockPushResolver,
      appShufflePartitionId: AppAttemptShuffleMergeId,
      reduceId: Int): File = {
    mergeManager.appsShuffleInfo.get(appShufflePartitionId.appId)
      .getMergedShuffleMetaFile(appShufflePartitionId.shuffleId,
        appShufflePartitionId.shuffleMergeId, reduceId)
  }

  def getPartitionFileHandlers(
      partitionInfo: AppShufflePartitionInfo):
      (FileChannel, MergeShuffleFile, MergeShuffleFile) = {
    (partitionInfo.getDataChannel, partitionInfo.getMetaFile, partitionInfo.getIndexFile)
  }

  def closePartitionFiles(partitionInfo: AppShufflePartitionInfo): Unit = {
    partitionInfo.closeAllFilesAndDeleteIfNeeded(false)
  }

  def clearAppShuffleInfo(mergeMgr: RemoteBlockPushResolver): Unit = {
    mergeMgr.appsShuffleInfo.clear()
  }

  def reloadAppShuffleInfo(
      mergeMgr: RemoteBlockPushResolver, db: DB): ConcurrentMap[String, AppShuffleInfo] = {
    mergeMgr.appsShuffleInfo.clear()
    mergeMgr.reloadAndCleanUpAppShuffleInfo(db)
    mergeMgr.appsShuffleInfo
  }

  def getOutdatedAppPathInfoCountDuringDBReload(
      mergeMgr: RemoteBlockPushResolver, db: DB): Int = {
    mergeMgr.reloadActiveAppAttemptsPathInfo(db).size()
  }

  def getOutdatedFinalizedShuffleCountDuringDBReload(
    mergeMgr: RemoteBlockPushResolver, db: DB): Int = {
    mergeMgr.reloadFinalizedAppAttemptsShuffleMergeInfo(db).size()
  }

  def reloadRegisteredExecutors(
    dbBackend: DBBackend,
    file: File): ConcurrentMap[ExternalShuffleBlockResolver.AppExecId, ExecutorShuffleInfo] = {
    val db = DBProvider.initDB(dbBackend, file)
    val result = ExternalShuffleBlockResolver.reloadRegisteredExecutors(db)
    db.close()
    result
  }

  def reloadRegisteredExecutors(
      db: DB): ConcurrentMap[ExternalShuffleBlockResolver.AppExecId, ExecutorShuffleInfo] = {
    ExternalShuffleBlockResolver.reloadRegisteredExecutors(db)
  }
}
