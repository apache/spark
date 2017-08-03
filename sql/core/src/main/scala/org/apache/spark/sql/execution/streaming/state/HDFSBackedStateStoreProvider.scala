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

import java.io.{DataInputStream, DataOutputStream, FileNotFoundException, IOException}
import java.nio.channels.ClosedChannelException
import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random
import scala.util.control.NonFatal

import com.google.common.io.ByteStreams
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.io.LZ4CompressionCodec
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{SizeEstimator, Utils}


/**
 * An implementation of [[StateStoreProvider]] and [[StateStore]] in which all the data is backed
 * by files in a HDFS-compatible file system. All updates to the store has to be done in sets
 * transactionally, and each set of updates increments the store's version. These versions can
 * be used to re-execute the updates (by retries in RDD operations) on the correct version of
 * the store, and regenerate the store version.
 *
 * Usage:
 * To update the data in the state store, the following order of operations are needed.
 *
 * // get the right store
 * - val store = StateStore.get(
 *      StateStoreId(checkpointLocation, operatorId, partitionId), ..., version, ...)
 * - store.put(...)
 * - store.remove(...)
 * - store.commit()    // commits all the updates to made; the new version will be returned
 * - store.iterator()  // key-value data after last commit as an iterator
 * - store.updates()   // updates made in the last commit as an iterator
 *
 * Fault-tolerance model:
 * - Every set of updates is written to a delta file before committing.
 * - The state store is responsible for managing, collapsing and cleaning up of delta files.
 * - Multiple attempts to commit the same version of updates may overwrite each other.
 *   Consistency guarantees depend on whether multiple attempts have the same updates and
 *   the overwrite semantics of underlying file system.
 * - Background maintenance of files ensures that last versions of the store is always recoverable
 * to ensure re-executed RDD operations re-apply updates on the correct past version of the
 * store.
 */
private[state] class HDFSBackedStateStoreProvider extends StateStoreProvider with Logging {

  // ConcurrentHashMap is used because it generates fail-safe iterators on filtering
  // - The iterator is weakly consistent with the map, i.e., iterator's data reflect the values in
  //   the map when the iterator was created
  // - Any updates to the map while iterating through the filtered iterator does not throw
  //   java.util.ConcurrentModificationException
  type MapType = java.util.concurrent.ConcurrentHashMap[UnsafeRow, UnsafeRow]

  /** Implementation of [[StateStore]] API which is backed by a HDFS-compatible file system */
  class HDFSBackedStateStore(val version: Long, mapToUpdate: MapType)
    extends StateStore {

    /** Trait and classes representing the internal state of the store */
    trait STATE
    case object UPDATING extends STATE
    case object COMMITTED extends STATE
    case object ABORTED extends STATE

    private val newVersion = version + 1
    private val tempDeltaFile = new Path(baseDir, s"temp-${Random.nextLong}")
    private lazy val tempDeltaFileStream = compressStream(fs.create(tempDeltaFile, true))
    @volatile private var state: STATE = UPDATING
    @volatile private var finalDeltaFile: Path = null

    override def id: StateStoreId = HDFSBackedStateStoreProvider.this.stateStoreId

    override def get(key: UnsafeRow): UnsafeRow = {
      mapToUpdate.get(key)
    }

    override def put(key: UnsafeRow, value: UnsafeRow): Unit = {
      verify(state == UPDATING, "Cannot put after already committed or aborted")
      val keyCopy = key.copy()
      val valueCopy = value.copy()
      mapToUpdate.put(keyCopy, valueCopy)
      writeUpdateToDeltaFile(tempDeltaFileStream, keyCopy, valueCopy)
    }

    override def remove(key: UnsafeRow): Unit = {
      verify(state == UPDATING, "Cannot remove after already committed or aborted")
      val prevValue = mapToUpdate.remove(key)
      if (prevValue != null) {
        writeRemoveToDeltaFile(tempDeltaFileStream, key)
      }
    }

    override def getRange(
        start: Option[UnsafeRow],
        end: Option[UnsafeRow]): Iterator[UnsafeRowPair] = {
      verify(state == UPDATING, "Cannot getRange after already committed or aborted")
      iterator()
    }

    /** Commit all the updates that have been made to the store, and return the new version. */
    override def commit(): Long = {
      verify(state == UPDATING, "Cannot commit after already committed or aborted")

      try {
        finalizeDeltaFile(tempDeltaFileStream)
        finalDeltaFile = commitUpdates(newVersion, mapToUpdate, tempDeltaFile)
        state = COMMITTED
        logInfo(s"Committed version $newVersion for $this to file $finalDeltaFile")
        newVersion
      } catch {
        case NonFatal(e) =>
          throw new IllegalStateException(
            s"Error committing version $newVersion into $this", e)
      }
    }

    /** Abort all the updates made on this store. This store will not be usable any more. */
    override def abort(): Unit = {
      verify(state == UPDATING || state == ABORTED, "Cannot abort after already committed")
      try {
        state = ABORTED
        if (tempDeltaFileStream != null) {
          tempDeltaFileStream.close()
        }
        if (tempDeltaFile != null) {
          fs.delete(tempDeltaFile, true)
        }
      } catch {
        case c: ClosedChannelException =>
          // This can happen when underlying file output stream has been closed before the
          // compression stream.
          logDebug(s"Error aborting version $newVersion into $this", c)

        case e: Exception =>
          logWarning(s"Error aborting version $newVersion into $this", e)
      }
      logInfo(s"Aborted version $newVersion for $this")
    }

    /**
     * Get an iterator of all the store data.
     * This can be called only after committing all the updates made in the current thread.
     */
    override def iterator(): Iterator[UnsafeRowPair] = {
      val unsafeRowPair = new UnsafeRowPair()
      mapToUpdate.entrySet.asScala.iterator.map { entry =>
        unsafeRowPair.withRows(entry.getKey, entry.getValue)
      }
    }

    override def metrics: StateStoreMetrics = {
      StateStoreMetrics(mapToUpdate.size(), SizeEstimator.estimate(mapToUpdate), Map.empty)
    }

    /**
     * Whether all updates have been committed
     */
    override def hasCommitted: Boolean = {
      state == COMMITTED
    }

    override def toString(): String = {
      s"HDFSStateStore[id=(op=${id.operatorId},part=${id.partitionId}),dir=$baseDir]"
    }
  }

  /** Get the state store for making updates to create a new `version` of the store. */
  override def getStore(version: Long): StateStore = synchronized {
    require(version >= 0, "Version cannot be less than 0")
    val newMap = new MapType()
    if (version > 0) {
      newMap.putAll(loadMap(version))
    }
    val store = new HDFSBackedStateStore(version, newMap)
    logInfo(s"Retrieved version $version of ${HDFSBackedStateStoreProvider.this} for update")
    store
  }

  override def init(
      stateStoreId: StateStoreId,
      keySchema: StructType,
      valueSchema: StructType,
      indexOrdinal: Option[Int], // for sorting the data
      storeConf: StateStoreConf,
      hadoopConf: Configuration): Unit = {
    this.stateStoreId_ = stateStoreId
    this.keySchema = keySchema
    this.valueSchema = valueSchema
    this.storeConf = storeConf
    this.hadoopConf = hadoopConf
    fs.mkdirs(baseDir)
  }

  override def stateStoreId: StateStoreId = stateStoreId_

  /** Do maintenance backing data files, including creating snapshots and cleaning up old files */
  override def doMaintenance(): Unit = {
    try {
      doSnapshot()
      cleanup()
    } catch {
      case NonFatal(e) =>
        logWarning(s"Error performing snapshot and cleaning up $this")
    }
  }

  override def close(): Unit = {
    loadedMaps.values.foreach(_.clear())
  }

  override def supportedCustomMetrics: Seq[StateStoreCustomMetric] = {
    Nil
  }

  override def toString(): String = {
    s"HDFSStateStoreProvider[" +
      s"id = (op=${stateStoreId.operatorId},part=${stateStoreId.partitionId}),dir = $baseDir]"
  }

  /* Internal fields and methods */

  @volatile private var stateStoreId_ : StateStoreId = _
  @volatile private var keySchema: StructType = _
  @volatile private var valueSchema: StructType = _
  @volatile private var storeConf: StateStoreConf = _
  @volatile private var hadoopConf: Configuration = _

  private lazy val loadedMaps = new mutable.HashMap[Long, MapType]
  private lazy val baseDir = stateStoreId.storeCheckpointLocation()
  private lazy val fs = baseDir.getFileSystem(hadoopConf)
  private lazy val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf)

  private case class StoreFile(version: Long, path: Path, isSnapshot: Boolean)

  /** Commit a set of updates to the store with the given new version */
  private def commitUpdates(newVersion: Long, map: MapType, tempDeltaFile: Path): Path = {
    synchronized {
      val finalDeltaFile = deltaFile(newVersion)

      // scalastyle:off
      // Renaming a file atop an existing one fails on HDFS
      // (http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/filesystem/filesystem.html).
      // Hence we should either skip the rename step or delete the target file. Because deleting the
      // target file will break speculation, skipping the rename step is the only choice. It's still
      // semantically correct because Structured Streaming requires rerunning a batch should
      // generate the same output. (SPARK-19677)
      // scalastyle:on
      if (fs.exists(finalDeltaFile)) {
        fs.delete(tempDeltaFile, true)
      } else if (!fs.rename(tempDeltaFile, finalDeltaFile)) {
        throw new IOException(s"Failed to rename $tempDeltaFile to $finalDeltaFile")
      }
      loadedMaps.put(newVersion, map)
      finalDeltaFile
    }
  }

  /**
   * Get iterator of all the data of the latest version of the store.
   * Note that this will look up the files to determined the latest known version.
   */
  private[state] def latestIterator(): Iterator[UnsafeRowPair] = synchronized {
    val versionsInFiles = fetchFiles().map(_.version).toSet
    val versionsLoaded = loadedMaps.keySet
    val allKnownVersions = versionsInFiles ++ versionsLoaded
    val unsafeRowTuple = new UnsafeRowPair()
    if (allKnownVersions.nonEmpty) {
      loadMap(allKnownVersions.max).entrySet().iterator().asScala.map { entry =>
        unsafeRowTuple.withRows(entry.getKey, entry.getValue)
      }
    } else Iterator.empty
  }

  /** Load the required version of the map data from the backing files */
  private def loadMap(version: Long): MapType = {
    if (version <= 0) return new MapType
    synchronized { loadedMaps.get(version) }.getOrElse {
      val mapFromFile = readSnapshotFile(version).getOrElse {
        val prevMap = loadMap(version - 1)
        val newMap = new MapType(prevMap)
        updateFromDeltaFile(version, newMap)
        newMap
      }
      loadedMaps.put(version, mapFromFile)
      mapFromFile
    }
  }

  private def writeUpdateToDeltaFile(
      output: DataOutputStream,
      key: UnsafeRow,
      value: UnsafeRow): Unit = {
    val keyBytes = key.getBytes()
    val valueBytes = value.getBytes()
    output.writeInt(keyBytes.size)
    output.write(keyBytes)
    output.writeInt(valueBytes.size)
    output.write(valueBytes)
  }

  private def writeRemoveToDeltaFile(output: DataOutputStream, key: UnsafeRow): Unit = {
    val keyBytes = key.getBytes()
    output.writeInt(keyBytes.size)
    output.write(keyBytes)
    output.writeInt(-1)
  }

  private def finalizeDeltaFile(output: DataOutputStream): Unit = {
    output.writeInt(-1)  // Write this magic number to signify end of file
    output.close()
  }

  private def updateFromDeltaFile(version: Long, map: MapType): Unit = {
    val fileToRead = deltaFile(version)
    var input: DataInputStream = null
    val sourceStream = try {
      fs.open(fileToRead)
    } catch {
      case f: FileNotFoundException =>
        throw new IllegalStateException(
          s"Error reading delta file $fileToRead of $this: $fileToRead does not exist", f)
    }
    try {
      input = decompressStream(sourceStream)
      var eof = false

      while(!eof) {
        val keySize = input.readInt()
        if (keySize == -1) {
          eof = true
        } else if (keySize < 0) {
          throw new IOException(
            s"Error reading delta file $fileToRead of $this: key size cannot be $keySize")
        } else {
          val keyRowBuffer = new Array[Byte](keySize)
          ByteStreams.readFully(input, keyRowBuffer, 0, keySize)

          val keyRow = new UnsafeRow(keySchema.fields.length)
          keyRow.pointTo(keyRowBuffer, keySize)

          val valueSize = input.readInt()
          if (valueSize < 0) {
            map.remove(keyRow)
          } else {
            val valueRowBuffer = new Array[Byte](valueSize)
            ByteStreams.readFully(input, valueRowBuffer, 0, valueSize)
            val valueRow = new UnsafeRow(valueSchema.fields.length)
            // If valueSize in existing file is not multiple of 8, floor it to multiple of 8.
            // This is a workaround for the following:
            // Prior to Spark 2.3 mistakenly append 4 bytes to the value row in
            // `RowBasedKeyValueBatch`, which gets persisted into the checkpoint data
            valueRow.pointTo(valueRowBuffer, (valueSize / 8) * 8)
            map.put(keyRow, valueRow)
          }
        }
      }
    } finally {
      if (input != null) input.close()
    }
    logInfo(s"Read delta file for version $version of $this from $fileToRead")
  }

  private def writeSnapshotFile(version: Long, map: MapType): Unit = {
    val fileToWrite = snapshotFile(version)
    var output: DataOutputStream = null
    Utils.tryWithSafeFinally {
      output = compressStream(fs.create(fileToWrite, false))
      val iter = map.entrySet().iterator()
      while(iter.hasNext) {
        val entry = iter.next()
        val keyBytes = entry.getKey.getBytes()
        val valueBytes = entry.getValue.getBytes()
        output.writeInt(keyBytes.size)
        output.write(keyBytes)
        output.writeInt(valueBytes.size)
        output.write(valueBytes)
      }
      output.writeInt(-1)
    } {
      if (output != null) output.close()
    }
    logInfo(s"Written snapshot file for version $version of $this at $fileToWrite")
  }

  private def readSnapshotFile(version: Long): Option[MapType] = {
    val fileToRead = snapshotFile(version)
    val map = new MapType()
    var input: DataInputStream = null

    try {
      input = decompressStream(fs.open(fileToRead))
      var eof = false

      while (!eof) {
        val keySize = input.readInt()
        if (keySize == -1) {
          eof = true
        } else if (keySize < 0) {
          throw new IOException(
            s"Error reading snapshot file $fileToRead of $this: key size cannot be $keySize")
        } else {
          val keyRowBuffer = new Array[Byte](keySize)
          ByteStreams.readFully(input, keyRowBuffer, 0, keySize)

          val keyRow = new UnsafeRow(keySchema.fields.length)
          keyRow.pointTo(keyRowBuffer, keySize)

          val valueSize = input.readInt()
          if (valueSize < 0) {
            throw new IOException(
              s"Error reading snapshot file $fileToRead of $this: value size cannot be $valueSize")
          } else {
            val valueRowBuffer = new Array[Byte](valueSize)
            ByteStreams.readFully(input, valueRowBuffer, 0, valueSize)
            val valueRow = new UnsafeRow(valueSchema.fields.length)
            // If valueSize in existing file is not multiple of 8, floor it to multiple of 8.
            // This is a workaround for the following:
            // Prior to Spark 2.3 mistakenly append 4 bytes to the value row in
            // `RowBasedKeyValueBatch`, which gets persisted into the checkpoint data
            valueRow.pointTo(valueRowBuffer, (valueSize / 8) * 8)
            map.put(keyRow, valueRow)
          }
        }
      }
      logInfo(s"Read snapshot file for version $version of $this from $fileToRead")
      Some(map)
    } catch {
      case _: FileNotFoundException =>
        None
    } finally {
      if (input != null) input.close()
    }
  }


  /** Perform a snapshot of the store to allow delta files to be consolidated */
  private def doSnapshot(): Unit = {
    try {
      val files = fetchFiles()
      if (files.nonEmpty) {
        val lastVersion = files.last.version
        val deltaFilesForLastVersion =
          filesForVersion(files, lastVersion).filter(_.isSnapshot == false)
        synchronized { loadedMaps.get(lastVersion) } match {
          case Some(map) =>
            if (deltaFilesForLastVersion.size > storeConf.minDeltasForSnapshot) {
              writeSnapshotFile(lastVersion, map)
            }
          case None =>
            // The last map is not loaded, probably some other instance is in charge
        }

      }
    } catch {
      case NonFatal(e) =>
        logWarning(s"Error doing snapshots for $this", e)
    }
  }

  /**
   * Clean up old snapshots and delta files that are not needed any more. It ensures that last
   * few versions of the store can be recovered from the files, so re-executed RDD operations
   * can re-apply updates on the past versions of the store.
   */
  private[state] def cleanup(): Unit = {
    try {
      val files = fetchFiles()
      if (files.nonEmpty) {
        val earliestVersionToRetain = files.last.version - storeConf.minVersionsToRetain
        if (earliestVersionToRetain > 0) {
          val earliestFileToRetain = filesForVersion(files, earliestVersionToRetain).head
          synchronized {
            val mapsToRemove = loadedMaps.keys.filter(_ < earliestVersionToRetain).toSeq
            mapsToRemove.foreach(loadedMaps.remove)
          }
          val filesToDelete = files.filter(_.version < earliestFileToRetain.version)
          filesToDelete.foreach { f =>
            fs.delete(f.path, true)
          }
          logInfo(s"Deleted files older than ${earliestFileToRetain.version} for $this: " +
            filesToDelete.mkString(", "))
        }
      }
    } catch {
      case NonFatal(e) =>
        logWarning(s"Error cleaning up files for $this", e)
    }
  }

  /** Files needed to recover the given version of the store */
  private def filesForVersion(allFiles: Seq[StoreFile], version: Long): Seq[StoreFile] = {
    require(version >= 0)
    require(allFiles.exists(_.version == version))

    val latestSnapshotFileBeforeVersion = allFiles
      .filter(_.isSnapshot == true)
      .takeWhile(_.version <= version)
      .lastOption
    val deltaBatchFiles = latestSnapshotFileBeforeVersion match {
      case Some(snapshotFile) =>

        val deltaFiles = allFiles.filter { file =>
          file.version > snapshotFile.version && file.version <= version
        }.toList
        verify(
          deltaFiles.size == version - snapshotFile.version,
          s"Unexpected list of delta files for version $version for $this: $deltaFiles"
        )
        deltaFiles

      case None =>
        allFiles.takeWhile(_.version <= version)
    }
    latestSnapshotFileBeforeVersion.toSeq ++ deltaBatchFiles
  }

  /** Fetch all the files that back the store */
  private def fetchFiles(): Seq[StoreFile] = {
    val files: Seq[FileStatus] = try {
      fs.listStatus(baseDir)
    } catch {
      case _: java.io.FileNotFoundException =>
        Seq.empty
    }
    val versionToFiles = new mutable.HashMap[Long, StoreFile]
    files.foreach { status =>
      val path = status.getPath
      val nameParts = path.getName.split("\\.")
      if (nameParts.size == 2) {
        val version = nameParts(0).toLong
        nameParts(1).toLowerCase(Locale.ROOT) match {
          case "delta" =>
            // ignore the file otherwise, snapshot file already exists for that batch id
            if (!versionToFiles.contains(version)) {
              versionToFiles.put(version, StoreFile(version, path, isSnapshot = false))
            }
          case "snapshot" =>
            versionToFiles.put(version, StoreFile(version, path, isSnapshot = true))
          case _ =>
            logWarning(s"Could not identify file $path for $this")
        }
      }
    }
    val storeFiles = versionToFiles.values.toSeq.sortBy(_.version)
    logDebug(s"Current set of files for $this: ${storeFiles.mkString(", ")}")
    storeFiles
  }

  private def compressStream(outputStream: DataOutputStream): DataOutputStream = {
    val compressed = new LZ4CompressionCodec(sparkConf).compressedOutputStream(outputStream)
    new DataOutputStream(compressed)
  }

  private def decompressStream(inputStream: DataInputStream): DataInputStream = {
    val compressed = new LZ4CompressionCodec(sparkConf).compressedInputStream(inputStream)
    new DataInputStream(compressed)
  }

  private def deltaFile(version: Long): Path = {
    new Path(baseDir, s"$version.delta")
  }

  private def snapshotFile(version: Long): Path = {
    new Path(baseDir, s"$version.snapshot")
  }

  private def verify(condition: => Boolean, msg: String): Unit = {
    if (!condition) {
      throw new IllegalStateException(msg)
    }
  }
}
