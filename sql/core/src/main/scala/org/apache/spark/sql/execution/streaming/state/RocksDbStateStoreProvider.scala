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

package org.apache.spark.sql.execution.streaming.state;

import java.io._
import java.util
import java.util.Locale

import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.control.NonFatal

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.io.FileUtility
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

 /*
  * An implementation of [[StateStoreProvider]] and [[StateStore]] using RocksDB as the storage
  * engine. In RocksDB, new writes are inserted into a memtable which is flushed into local storage
  * when the memtable fills up. It improves scalability as compared to
  * [[HDFSBackedStateStoreProvider]] since now the state data which was large enough to fit in the
  * executor memory can be written into the combination of memtable and local storage.The data is
  * backed in a HDFS-compatible file system just like [[HDFSBackedStateStoreProvider]]
  *
  * Fault-tolerance model:
  * - Every set of updates is written to a delta file before committing.
  * - The state store is responsible for managing, collapsing and cleaning up of delta files.
  * - Updates are committed in the db atomically
  *
  * Backup Model:
  * - Delta file is written in a HDFS-compatible file system on batch commit
  * - RocksDB state is check-pointed into a separate folder on batch commit
  * - Maintenance thread periodically takes a snapshot of the latest check-pointed version of
  *   rocksDB state which is written to a HDFS-compatible file system.
  *
  * Isolation Guarantee:
  * - writes are committed in the transaction.
  * - writer thread which started the transaction can read all un-committed updates
  * - any other reader thread cannot read any un-committed updates
  */
private[sql] class RocksDbStateStoreProvider extends StateStoreProvider with Logging {

  /* Internal fields and methods */
  @volatile private var stateStoreId_ : StateStoreId = _
  @volatile private var keySchema: StructType = _
  @volatile private var valueSchema: StructType = _
  @volatile private var storeConf: StateStoreConf = _
  @volatile private var hadoopConf: Configuration = _
  @volatile private var numberOfVersionsToRetain: Int = _
  @volatile private var localDir: String = _
  @volatile private var rocksDbConf: RocksDbStateStoreConf = _

  private lazy val baseDir: Path = stateStoreId.storeCheckpointLocation()
  private lazy val fm = CheckpointFileManager.create(baseDir, hadoopConf)
  private lazy val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf)

  private case class StoreFile(version: Long, path: Path, isSnapshot: Boolean)

  import WALUtils._

  /** Implementation of [[StateStore]] API which is backed by RocksDB and HDFS */
  class RocksDbStateStore(val version: Long) extends StateStore with Logging {

    /** Trait and classes representing the internal state of the store */
    trait STATE

    case object LOADED extends STATE

    case object UPDATING extends STATE

    case object COMMITTED extends STATE

    case object ABORTED extends STATE

    private val newVersion = version + 1
    @volatile private var state: STATE = LOADED
    private val finalDeltaFile: Path = deltaFile(baseDir, newVersion)
    private lazy val deltaFileStream = fm.createAtomic(finalDeltaFile, overwriteIfPossible = true)
    private lazy val compressedStream = compressStream(deltaFileStream, sparkConf)

    override def id: StateStoreId = RocksDbStateStoreProvider.this.stateStoreId

    var rocksDbWriteInstance: OptimisticTransactionDbInstance = null

    /*
     * numEntriesInDb and bytesUsedByDb are estimated value
     * due to the nature of RocksDB implementation.
     * see https://github.com/facebook/rocksdb/wiki/RocksDB-FAQ for more details
     */
    var numEntriesInDb: Long = 0L
    var bytesUsedByDb: Long = 0L

    private def initTransaction(): Unit = {
      if (state == LOADED && rocksDbWriteInstance == null) {
        logDebug(s"Creating Transactional DB for batch $version")
        rocksDbWriteInstance = new OptimisticTransactionDbInstance(
          keySchema,
          valueSchema,
          newVersion.toString,
          rocksDbConf)
        rocksDbWriteInstance.open(rocksDbPath)
        rocksDbWriteInstance.startTransactions()
        state = UPDATING
      }
    }

    override def get(key: UnsafeRow): UnsafeRow = {
      initTransaction()
      rocksDbWriteInstance.get(key)
    }

    override def put(key: UnsafeRow, value: UnsafeRow): Unit = {
      initTransaction()
      require(state == UPDATING, s"Cannot put after already committed or aborted")
      val keyCopy = key.copy()
      val valueCopy = value.copy()
      rocksDbWriteInstance.put(keyCopy, valueCopy)
      writeUpdateToDeltaFile(compressedStream, keyCopy, valueCopy)
    }

    override def remove(key: UnsafeRow): Unit = {
      initTransaction()
      require(state == UPDATING, "Cannot remove after already committed or aborted")
      rocksDbWriteInstance.remove(key)
      writeRemoveToDeltaFile(compressedStream, key)
    }

    override def getRange(
                           start: Option[UnsafeRow],
                           end: Option[UnsafeRow]): Iterator[UnsafeRowPair] = {
      require(state == UPDATING, "Cannot getRange after already committed or aborted")
      iterator()
    }

    /** Commit all the updates that have been made to the store, and return the new version. */
    override def commit(): Long = {
      initTransaction()
      require(state == UPDATING, s"Cannot commit after already committed or aborted")
      try {
        synchronized {
          rocksDbWriteInstance.commit(Some(getCheckpointPath(newVersion)))
          finalizeDeltaFile(compressedStream)
        }
        state = COMMITTED
        numEntriesInDb = rocksDbWriteInstance.getApproxEntriesInDb()
        bytesUsedByDb = numEntriesInDb * (keySchema.defaultSize + valueSchema.defaultSize)
        newVersion
      } catch {
        case NonFatal(e) =>
          throw new IllegalStateException(s"Error committing version $newVersion into $this", e)
      } finally {
        storeMap.remove(version)
        close()
      }
    }

    /*
     * Abort all the updates made on this store. This store will not be usable any more.
     */
    override def abort(): Unit = {
      // This if statement is to ensure that files are deleted only if there are changes to the
      // StateStore. We have two StateStores for each task, one which is used only for reading, and
      // the other used for read+write. We don't want the read-only to delete state files.
      try {
        if (state == UPDATING) {
          state = ABORTED
          synchronized {
            rocksDbWriteInstance.abort()
            cancelDeltaFile(compressedStream, deltaFileStream)
          }
          logInfo(s"Aborted version $newVersion for $this")
        } else {
          state = ABORTED
        }
      } catch {
        case NonFatal(e) =>
          throw new IllegalStateException(s"Error aborting version $newVersion into $this", e)
      } finally {
        storeMap.remove(version)
        close()
      }
    }

    def close(): Unit = {
      if (rocksDbWriteInstance != null) {
        rocksDbWriteInstance.close()
        rocksDbWriteInstance = null
      }
    }

    /*
     * Get an iterator of all the store data.
     * This can be called only after committing all the updates made in the current thread.
     */
    override def iterator(): Iterator[UnsafeRowPair] = {
      state match {
        case UPDATING =>
          logDebug("state = updating using transaction DB")
          // We need to use current db to read uncommitted transactions
          rocksDbWriteInstance.iterator(closeDbOnCompletion = false)

        case LOADED | ABORTED =>
          // use check-pointed db for previous version
          logDebug(s"state = loaded/aborted using check-pointed DB with version $version")
          if (version == 0) {
            Iterator.empty
          } else {
            val path = getCheckpointPath(version)
            val r: RocksDbInstance =
              new RocksDbInstance(
                keySchema,
                valueSchema,
                version.toString,
                rocksDbConf)
            r.open(path, readOnly = true)
            r.iterator(closeDbOnCompletion = true)
          }
        case COMMITTED =>
          logDebug(s"state = committed using check-pointed DB with version $newVersion")
          // use check-pointed db for current updated version
          val path = getCheckpointPath(newVersion)
          val r: RocksDbInstance =
            new RocksDbInstance(
              keySchema,
              valueSchema,
              newVersion.toString,
              rocksDbConf)
          r.open(path, readOnly = true)
          r.iterator(closeDbOnCompletion = true)

        case _ => Iterator.empty
      }
    }

    override def metrics: StateStoreMetrics = {
      val metricsFromProvider: Map[String, Long] = getMetricsForProvider()
      val customMetrics = metricsFromProvider.flatMap {
        case (name, value) =>
          // just allow searching from list cause the list is small enough
          supportedCustomMetrics.find(_.name == name).map(_ -> value)
      }
      StateStoreMetrics(Math.max(numEntriesInDb, 0), Math.max(bytesUsedByDb, 0), customMetrics)
    }

    /*
     * Whether all updates have been committed
     */
    override def hasCommitted: Boolean = {
      state == COMMITTED
    }

    override def toString(): String = {
      s"RocksDbStateStore[id=(op=${id.operatorId},part=${id.partitionId}),dir=$baseDir]"
    }

  }

  /*
   * Initialize the provider with more contextual information from the SQL operator.
   * This method will be called first after creating an instance of the StateStoreProvider by
   * reflection.
   *
   * @param stateStoreId    Id of the versioned StateStores that this provider will generate
   * @param keySchema       Schema of keys to be stored
   * @param valueSchema     Schema of value to be stored
   * @param keyIndexOrdinal Optional column (represent as the ordinal of the field in keySchema) by
   *                        which the StateStore implementation could index the data.
   * @param storeConfs      Configurations used by the StateStores
   * @param hadoopConf      Hadoop configuration that could be used by StateStore
   *                        to save state data
   */
  override def init(
                     stateStoreId: StateStoreId,
                     keySchema: StructType,
                     valueSchema: StructType,
                     keyIndexOrdinal: Option[Int], // for sorting the data by their keys
                     storeConfs: StateStoreConf,
                     hadoopConf: Configuration): Unit = {
    this.stateStoreId_ = stateStoreId
    this.keySchema = keySchema
    this.valueSchema = valueSchema
    this.storeConf = storeConfs
    this.hadoopConf = hadoopConf
    this.numberOfVersionsToRetain = storeConfs.maxVersionsToRetainInMemory
    fm.mkdirs(baseDir)
    this.rocksDbConf = new RocksDbStateStoreConf(storeConfs)
    this.localDir = rocksDbConf.localDir
  }

  /*
   * Return the id of the StateStores this provider will generate.
   * Should be the same as the one passed in init().
   */
  override def stateStoreId: StateStoreId = stateStoreId_

  /*
   * Called when the provider instance is unloaded from the executor
   */
  override def close(): Unit = {
    storeMap.values.asScala.foreach(_.close)
    storeMap.clear()
  }

  private val storeMap = new util.HashMap[Long, RocksDbStateStore]()

  /*
   * Optional custom metrics that the implementation may want to report.
   *
   * @note The StateStore objects created by this provider must report the same custom metrics
   *       (specifically, same names) through `StateStore.metrics`.
   */
  // TODO
  override def supportedCustomMetrics: Seq[StateStoreCustomMetric] = {
    Nil
  }

  override def toString(): String = {
    s"RocksDbStateStoreProvider[" +
      s"id = (op=${stateStoreId.operatorId},part=${stateStoreId.partitionId}),dir = $baseDir]"
  }

  def getMetricsForProvider(): Map[String, Long] = synchronized {
    Map.empty[String, Long]
  }

  /*
   * Return an instance of [[StateStore]] representing state data of the given version
   */
  override def getStore(version: Long): StateStore = synchronized {
    logInfo(s"get Store for version $version")
    require(version >= 0, "Version cannot be less than 0")
    if (storeMap.containsKey(version)) {
      storeMap.get(version)
    } else {
      val store = createStore(version)
      storeMap.put(version, store)
      store
    }
  }

  private def createStore(version: Long): RocksDbStateStore = {
    val newStore = new RocksDbStateStore(version)
    if (version > 0) {
      // load the data into the rocksDB
      logInfo(
        s"Loading state into the db for $version and partition ${stateStoreId_.partitionId}")
      loadIntoRocksDB(version)
    }
    newStore
  }

  private def loadIntoRocksDB(version: Long): Unit = {
    /*
       1. Get last available/committed Rocksdb version in local folder
       2. If last committed version = version, we already have loaded rocksdb state.
       3. If last committed version = version - 1,
          we have to apply delta for version in the existing rocksdb
       4. Otherwise we have to recreate a new rocksDB store by using Snapshots/Delta
     */
    val (_, elapsedMs) = Utils.timeTakenMs {
      var lastAvailableVersion = getLastCommittedVersion()
      if (lastAvailableVersion == -1L || lastAvailableVersion <= version - 2) {
        // Destroy existing DB so that we can reconstruct it using snapshot and delta files
        RocksDbInstance.destroyDB(rocksDbPath)
        var lastAvailableSnapShotVersion: Long = version + 1
        // load from snapshot
        var found = false
        while (!found && lastAvailableSnapShotVersion > 0) {
          try {
            lastAvailableSnapShotVersion = lastAvailableSnapShotVersion - 1
            found = loadSnapshotFile(lastAvailableSnapShotVersion)
            logDebug(
              s"Snapshot for version $lastAvailableSnapShotVersion " +
                "and partition ${stateStoreId_.partitionId}:  found = $found")
          } catch {
            case e: Exception =>
              logError(s"$e while reading snapshot file")
              throw e
          }
        }
        lastAvailableVersion = lastAvailableSnapShotVersion
      }
      if (lastAvailableVersion < version) {
        applyDelta(version, lastAvailableVersion)
      }
    }
    logInfo(
      s"Loading state for $version and partition ${stateStoreId_.partitionId} took $elapsedMs ms.")
  }

  private def getLastCommittedVersion(): Long = {
    val f = new File(rocksDbPath, RocksDbInstance.COMMIT_FILE_NAME)
    if (f.exists()) {
      try {
        val fileContents = Source.fromFile(f.getAbsolutePath).getLines.mkString
        return fileContents.toLong
      } catch {
        case e: Exception =>
          logWarning("Exception while reading committed file")
      }
    }
    return -1L
  }

  private def loadSnapshotFile(version: Long): Boolean = {
    val fileToRead = snapshotFile(baseDir, version)
    if (version == 0 || !fm.exists(fileToRead)) {
      return false
    }
    val versionTempPath = getTempPath(version)
    val tmpLocDir: File = new File(versionTempPath)
    val tmpLocFile: File = new File(s"${versionTempPath}.tar")
    try {
      logInfo(s"Will download $fileToRead at location ${tmpLocFile.toString()}")
      if (downloadFile(fm, fileToRead, new Path(tmpLocFile.getAbsolutePath), sparkConf)) {
        FileUtility.extractTarFile(tmpLocFile.getAbsolutePath, versionTempPath)
        if (!tmpLocDir.list().exists(_.endsWith(".sst"))) {
          logWarning("Snapshot files are corrupted")
          throw new IOException(
            s"Error reading snapshot file $fileToRead of $this:" +
              s" No SST files found")
        }
        FileUtils.moveDirectory(tmpLocDir, new File(rocksDbPath))
        true
      } else {
        false
      }
    } catch {
      case e: Exception =>
        logError(s"Exception while loading snapshot file $e")
        throw e
    } finally {
      if (tmpLocFile.exists()) {
        tmpLocFile.delete()
      }
      FileUtils.deleteDirectory(tmpLocDir)
    }
  }

  private def applyDelta(version: Long, lastAvailableVersion: Long): Unit = {
    var rocksDbWriteInstance: OptimisticTransactionDbInstance = null
    try {
      rocksDbWriteInstance = new OptimisticTransactionDbInstance(
        keySchema,
        valueSchema,
        version.toString,
        rocksDbConf)
      rocksDbWriteInstance.open(rocksDbPath)
      rocksDbWriteInstance.startTransactions()
      // Load all the deltas from the version after the last available
      // one up to the target version.
      // The last available version is the one with a full snapshot, so it doesn't need deltas.
      for (deltaVersion <- (lastAvailableVersion + 1) to version) {
        val fileToRead = deltaFile(baseDir, deltaVersion)
        updateFromDeltaFile(
          fm,
          fileToRead,
          keySchema,
          valueSchema,
          rocksDbWriteInstance,
          sparkConf)
        logInfo(s"Read delta file for version $version of $this from $fileToRead")
      }
      rocksDbWriteInstance.commit(Some(getCheckpointPath(version)))
    } catch {
      case e: Exception =>
        logError(s"Exception while loading state ${e.getMessage}")
        if (rocksDbWriteInstance != null) {
          rocksDbWriteInstance.abort()
        }
        throw e
    } finally {
      if (rocksDbWriteInstance != null) {
        rocksDbWriteInstance.close()
      }
    }
  }

  /** Optional method for providers to allow for background maintenance (e.g. compactions) */
  override def doMaintenance(): Unit = {
    try {
      val (files: Seq[WALUtils.StoreFile], e1) = Utils.timeTakenMs(fetchFiles(fm, baseDir))
      logDebug(s"fetchFiles() took $e1 ms.")
      doSnapshot(files)
      cleanup(files)
      cleanRocksDBCheckpoints(files)
    } catch {
      case NonFatal(e) =>
        logWarning(s"Error performing snapshot and cleaning up $this")
    }
  }

  private def doSnapshot(files: Seq[WALUtils.StoreFile]): Unit = {
    if (files.nonEmpty) {
      val lastVersion = files.last.version
      val deltaFilesForLastVersion =
        filesForVersion(files, lastVersion).filter(_.isSnapshot == false)
      if (deltaFilesForLastVersion.size > storeConf.minDeltasForSnapshot) {
        val dbPath = getCheckpointPath(lastVersion)
        val snapShotFileName = s"{getTempPath(lastVersion)}.snapshot"
        val f = new File(snapShotFileName)
        try {
          val (_, t1) = Utils.timeTakenMs {
            FileUtility.createTarFile(dbPath, snapShotFileName)
            val targetFile = snapshotFile(baseDir, lastVersion)
            uploadFile(fm, new Path(snapShotFileName), targetFile, sparkConf)
          }
          logInfo(s"Creating snapshot file for ${stateStoreId_.partitionId} took $t1 ms.")
        } catch {
          case e: Exception =>
            logError(s"Exception while creating snapshot $e")
            throw e
        } finally {
          f.delete() // delete the tarball
        }
      }
    }
  }

  /*
   * Clean up old snapshots and delta files that are not needed any more. It ensures that last
   * few versions of the store can be recovered from the files, so re-executed RDD operations
   * can re-apply updates on the past versions of the store.
   */
  private[state] def cleanup(files: Seq[WALUtils.StoreFile]): Unit = {
    try {
      if (files.nonEmpty) {
        val earliestVersionToRetain = files.last.version - storeConf.minVersionsToRetain
        if (earliestVersionToRetain > 0) {
          val earliestFileToRetain = filesForVersion(files, earliestVersionToRetain).head
          val filesToDelete = files.filter(_.version < earliestFileToRetain.version)
          val (_, e2) = Utils.timeTakenMs {
            filesToDelete.foreach { f =>
              fm.delete(f.path)
              val file = new File(rocksDbPath, f.version.toString)
              if (file.exists()) {
                file.delete()
              }
            }
          }
          logDebug(s"deleting files took $e2 ms.")
          logInfo(
            s"Deleted files older than ${earliestFileToRetain.version} for $this: " +
              filesToDelete.mkString(", "))
        }
      }
    } catch {
      case NonFatal(e) =>
        logWarning(s"Error cleaning up files for $this", e)
    }
  }

  private def cleanRocksDBCheckpoints(files: Seq[WALUtils.StoreFile]): Unit = {
    try {
      val (_, e2) = Utils.timeTakenMs {
        if (files.nonEmpty) {
          val earliestVersionToRetain = files.last.version - storeConf.minVersionsToRetain
          if (earliestVersionToRetain > 0) {
            new File(getCheckpointPath(earliestVersionToRetain)).getParentFile
              .listFiles(new FileFilter {
                def accept(f: File): Boolean = {
                  try {
                    f.getName.toLong < earliestVersionToRetain
                  } catch {
                    case _: NumberFormatException => false
                  }
                }
              })
              .foreach(p => RocksDbInstance.destroyDB(p.getAbsolutePath))
            logInfo(
              s"Deleted rocksDB checkpoints older than ${earliestVersionToRetain} for $this: ")
          }
        }
      }
      logDebug(s"deleting rocksDB checkpoints took $e2 ms.")
    } catch {
      case NonFatal(e) => logWarning(s"Error cleaning up files for $this", e)
    }
  }

  // Used only for unit tests
  private[sql] def latestIterator(): Iterator[UnsafeRowPair] = synchronized {
    val versionsInFiles = fetchFiles(fm, baseDir).map(_.version).toSet
    if (versionsInFiles.nonEmpty) {
      val maxVersion = versionsInFiles.max
      if (maxVersion > 0) {
        loadIntoRocksDB(maxVersion)
        val r: RocksDbInstance =
          new RocksDbInstance(
            keySchema,
            valueSchema,
            maxVersion.toString,
            rocksDbConf)
        try {
          r.open(rocksDbPath, readOnly = true)
          return r.iterator(false)
        } catch {
          case e: Exception =>
            logWarning(s"Exception ${e.getMessage} while getting latest Iterator")
        }
      }
    }
    Iterator.empty
  }

  private[sql] def getLocalDir: String = localDir

  private[sql] lazy val rocksDbPath: String = {
    getPath("db")
  }

  private def getCheckpointPath(version: Long): String = {
    getPath("checkpoint", Some(version.toString))
  }

  private def getTempPath(version: Long): String = {
    getPath("tmp", Some(version.toString))
  }

  private def getPath(subFolderName: String, version: Option[String] = None): String = {
    val checkpointRootLocationPath = new Path(stateStoreId.checkpointRootLocation)

    val dirPath = new Path(
      localDir,
      new Path(
        new Path(
          subFolderName,
          checkpointRootLocationPath.getName + "_" + checkpointRootLocationPath.hashCode()),
        new Path(stateStoreId_.operatorId.toString, stateStoreId_.partitionId.toString)))

    val f: File = new File(dirPath.toString)
    if (!f.exists() && !f.mkdirs()) {
      throw new IllegalStateException(s"Couldn't create directory ${dirPath.toString}")
    }

    if (version.isEmpty) {
      dirPath.toString
    } else {
      new Path(dirPath, version.get).toString
    }
  }
}
