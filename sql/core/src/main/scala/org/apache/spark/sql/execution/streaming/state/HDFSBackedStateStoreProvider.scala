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

import java.io.{DataInputStream, DataOutputStream, IOException}

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
import org.apache.spark.util.Utils


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
private[state] class HDFSBackedStateStoreProvider(
    val id: StateStoreId,
    keySchema: StructType,
    valueSchema: StructType,
    storeConf: StateStoreConf,
    hadoopConf: Configuration
  ) extends StateStoreProvider with Logging {

  type MapType = java.util.HashMap[UnsafeRow, UnsafeRow]

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
    private val tempDeltaFileStream = compressStream(fs.create(tempDeltaFile, true))

    private val allUpdates = new java.util.HashMap[UnsafeRow, StoreUpdate]()

    @volatile private var state: STATE = UPDATING
    @volatile private var finalDeltaFile: Path = null

    override def id: StateStoreId = HDFSBackedStateStoreProvider.this.id

    override def get(key: UnsafeRow): Option[UnsafeRow] = {
      Option(mapToUpdate.get(key))
    }

    override def put(key: UnsafeRow, value: UnsafeRow): Unit = {
      verify(state == UPDATING, "Cannot remove after already committed or aborted")

      val isNewKey = !mapToUpdate.containsKey(key)
      mapToUpdate.put(key, value)

      Option(allUpdates.get(key)) match {
        case Some(ValueAdded(_, _)) =>
          // Value did not exist in previous version and was added already, keep it marked as added
          allUpdates.put(key, ValueAdded(key, value))
        case Some(ValueUpdated(_, _)) | Some(KeyRemoved(_)) =>
          // Value existed in previous version and updated/removed, mark it as updated
          allUpdates.put(key, ValueUpdated(key, value))
        case None =>
          // There was no prior update, so mark this as added or updated according to its presence
          // in previous version.
          val update = if (isNewKey) ValueAdded(key, value) else ValueUpdated(key, value)
          allUpdates.put(key, update)
      }
      writeToDeltaFile(tempDeltaFileStream, ValueUpdated(key, value))
    }

    /** Remove keys that match the following condition */
    override def remove(condition: UnsafeRow => Boolean): Unit = {
      verify(state == UPDATING, "Cannot remove after already committed or aborted")
      val keyIter = mapToUpdate.keySet().iterator()
      while (keyIter.hasNext) {
        val key = keyIter.next
        if (condition(key)) {
          keyIter.remove()

          Option(allUpdates.get(key)) match {
            case Some(ValueUpdated(_, _)) | None =>
              // Value existed in previous version and maybe was updated, mark removed
              allUpdates.put(key, KeyRemoved(key))
            case Some(ValueAdded(_, _)) =>
              // Value did not exist in previous version and was added, should not appear in updates
              allUpdates.remove(key)
            case Some(KeyRemoved(_)) =>
              // Remove already in update map, no need to change
          }
          writeToDeltaFile(tempDeltaFileStream, KeyRemoved(key))
        }
      }
    }

    /** Commit all the updates that have been made to the store, and return the new version. */
    override def commit(): Long = {
      verify(state == UPDATING, "Cannot commit after already committed or aborted")

      try {
        finalizeDeltaFile(tempDeltaFileStream)
        finalDeltaFile = commitUpdates(newVersion, mapToUpdate, tempDeltaFile)
        state = COMMITTED
        logInfo(s"Committed version $newVersion for $this")
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

      state = ABORTED
      if (tempDeltaFileStream != null) {
        tempDeltaFileStream.close()
      }
      if (tempDeltaFile != null && fs.exists(tempDeltaFile)) {
        fs.delete(tempDeltaFile, true)
      }
      logInfo("Aborted")
    }

    /**
     * Get an iterator of all the store data.
     * This can be called only after committing all the updates made in the current thread.
     */
    override def iterator(): Iterator[(UnsafeRow, UnsafeRow)] = {
      verify(state == COMMITTED,
        "Cannot get iterator of store data before committing or after aborting")
      HDFSBackedStateStoreProvider.this.iterator(newVersion)
    }

    /**
     * Get an iterator of all the updates made to the store in the current version.
     * This can be called only after committing all the updates made in the current thread.
     */
    override def updates(): Iterator[StoreUpdate] = {
      verify(state == COMMITTED,
        "Cannot get iterator of updates before committing or after aborting")
      allUpdates.values().asScala.toIterator
    }

    override def numKeys(): Long = mapToUpdate.size()

    /**
     * Whether all updates have been committed
     */
    override private[state] def hasCommitted: Boolean = {
      state == COMMITTED
    }

    override def toString(): String = {
      s"HDFSStateStore[id = (op=${id.operatorId}, part=${id.partitionId}), dir = $baseDir]"
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

  override def toString(): String = {
    s"HDFSStateStoreProvider[id = (op=${id.operatorId}, part=${id.partitionId}), dir = $baseDir]"
  }

  /* Internal classes and methods */

  private val loadedMaps = new mutable.HashMap[Long, MapType]
  private val baseDir =
    new Path(id.checkpointLocation, s"${id.operatorId}/${id.partitionId.toString}")
  private val fs = baseDir.getFileSystem(hadoopConf)
  private val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf)

  initialize()

  private case class StoreFile(version: Long, path: Path, isSnapshot: Boolean)

  /** Commit a set of updates to the store with the given new version */
  private def commitUpdates(newVersion: Long, map: MapType, tempDeltaFile: Path): Path = {
    synchronized {
      val finalDeltaFile = deltaFile(newVersion)
      fs.rename(tempDeltaFile, finalDeltaFile)
      loadedMaps.put(newVersion, map)
      finalDeltaFile
    }
  }

  /**
   * Get iterator of all the data of the latest version of the store.
   * Note that this will look up the files to determined the latest known version.
   */
  private[state] def latestIterator(): Iterator[(UnsafeRow, UnsafeRow)] = synchronized {
    val versionsInFiles = fetchFiles().map(_.version).toSet
    val versionsLoaded = loadedMaps.keySet
    val allKnownVersions = versionsInFiles ++ versionsLoaded
    if (allKnownVersions.nonEmpty) {
      loadMap(allKnownVersions.max).entrySet().iterator().asScala.map { x =>
        (x.getKey, x.getValue)
      }
    } else Iterator.empty
  }

  /** Get iterator of a specific version of the store */
  private[state] def iterator(version: Long): Iterator[(UnsafeRow, UnsafeRow)] = synchronized {
    loadMap(version).entrySet().iterator().asScala.map { x =>
      (x.getKey, x.getValue)
    }
  }

  /** Initialize the store provider */
  private def initialize(): Unit = {
    if (!fs.exists(baseDir)) {
      fs.mkdirs(baseDir)
    } else {
      if (!fs.isDirectory(baseDir)) {
        throw new IllegalStateException(
          s"Cannot use ${id.checkpointLocation} for storing state data for $this as " +
            s"$baseDir already exists and is not a directory")
      }
    }
  }

  /** Load the required version of the map data from the backing files */
  private def loadMap(version: Long): MapType = {
    if (version <= 0) return new MapType
    synchronized { loadedMaps.get(version) }.getOrElse {
      val mapFromFile = readSnapshotFile(version).getOrElse {
        val prevMap = loadMap(version - 1)
        val newMap = new MapType(prevMap)
        newMap.putAll(prevMap)
        updateFromDeltaFile(version, newMap)
        newMap
      }
      loadedMaps.put(version, mapFromFile)
      mapFromFile
    }
  }

  private def writeToDeltaFile(output: DataOutputStream, update: StoreUpdate): Unit = {

    def writeUpdate(key: UnsafeRow, value: UnsafeRow): Unit = {
      val keyBytes = key.getBytes()
      val valueBytes = value.getBytes()
      output.writeInt(keyBytes.size)
      output.write(keyBytes)
      output.writeInt(valueBytes.size)
      output.write(valueBytes)
    }

    def writeRemove(key: UnsafeRow): Unit = {
      val keyBytes = key.getBytes()
      output.writeInt(keyBytes.size)
      output.write(keyBytes)
      output.writeInt(-1)
    }

    update match {
      case ValueAdded(key, value) =>
        writeUpdate(key, value)
      case ValueUpdated(key, value) =>
        writeUpdate(key, value)
      case KeyRemoved(key) =>
        writeRemove(key)
    }
  }

  private def finalizeDeltaFile(output: DataOutputStream): Unit = {
    output.writeInt(-1)  // Write this magic number to signify end of file
    output.close()
  }

  private def updateFromDeltaFile(version: Long, map: MapType): Unit = {
    val fileToRead = deltaFile(version)
    if (!fs.exists(fileToRead)) {
      throw new IllegalStateException(
        s"Error reading delta file $fileToRead of $this: $fileToRead does not exist")
    }
    var input: DataInputStream = null
    try {
      input = decompressStream(fs.open(fileToRead))
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
            valueRow.pointTo(valueRowBuffer, valueSize)
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
    if (!fs.exists(fileToRead)) return None

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
            valueRow.pointTo(valueRowBuffer, valueSize)
            map.put(keyRow, valueRow)
          }
        }
      }
      logInfo(s"Read snapshot file for version $version of $this from $fileToRead")
      Some(map)
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
        }
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
        nameParts(1).toLowerCase match {
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
