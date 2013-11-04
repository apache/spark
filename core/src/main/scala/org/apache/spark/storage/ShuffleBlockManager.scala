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

import java.io.File
import java.util
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.apache.spark.Logging
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.{MetadataCleanerType, MetadataCleaner, TimeStampedHashMap}
import org.apache.spark.util.collection.{PrimitiveKeyOpenHashMap, PrimitiveVector}

private[spark]
class ShuffleWriterGroup(
   val mapId: Int,
   val fileGroup: ShuffleFileGroup,
   val writers: Array[BlockObjectWriter])

private[spark]
trait ShuffleBlocks {
  /** Get a group of writers for this map task. */
  def acquireWriters(mapId: Int): ShuffleWriterGroup

  /** @param success Indicates all writes were successful. If false, no blocks will be recorded. */
  def releaseWriters(group: ShuffleWriterGroup, success: Boolean)
}

/**
 * Manages assigning disk-based block writers to shuffle tasks. Each shuffle task gets one file
 * per reducer (this set of files is called a ShuffleFileGroup).
 *
 * As an optimization to reduce the number of physical shuffle files produced, multiple shuffle
 * blocks are aggregated into the same file. There is one "combined shuffle file" (ShuffleFile) per
 * reducer per concurrently executing shuffle task. As soon as a task finishes writing to its
 * shuffle files, it releases them for another task.
 * Regarding the implementation of this feature, shuffle files are identified by a 3-tuple:
 *   - shuffleId: The unique id given to the entire shuffle stage.
 *   - bucketId: The id of the output partition (i.e., reducer id)
 *   - fileId: The unique id identifying a group of "combined shuffle files." Only one task at a
 *       time owns a particular fileId, and this id is returned to a pool when the task finishes.
 * Each shuffle file is then mapped to a FileSegment, which is a 3-tuple (file, offset, length)
 * that specifies where in a given file the actual block data is located.
 *
 * Shuffle file metadata is stored in a space-efficient manner. Rather than simply mapping
 * ShuffleBlockIds directly to FileSegments, each ShuffleFile maintains a list of offsets for each
 * block stored in that file. In order to find the location of a shuffle block, we search all
 * ShuffleFiles destined for the block's reducer.
 *
 */
private[spark]
class ShuffleBlockManager(blockManager: BlockManager) extends Logging {
  // Turning off shuffle file consolidation causes all shuffle Blocks to get their own file.
  // TODO: Remove this once the shuffle file consolidation feature is stable.
  val consolidateShuffleFiles =
    System.getProperty("spark.shuffle.consolidateFiles", "true").toBoolean

  /**
   * Contains a pool of unused ShuffleFileGroups.
   * One group is needed per concurrent thread (mapper) operating on the same shuffle.
   */
  private class ShuffleFileGroupPool {
    private val nextFileId = new AtomicInteger(0)
    private val unusedFileGroups = new ConcurrentLinkedQueue[ShuffleFileGroup]()

    def getNextFileId() = nextFileId.getAndIncrement()
    def getUnusedFileGroup() = unusedFileGroups.poll()
    def returnFileGroup(group: ShuffleFileGroup) = unusedFileGroups.add(group)
    def returnFileGroups(groups: Seq[ShuffleFileGroup]) = unusedFileGroups.addAll(groups)
  }

  type ShuffleId = Int
  private val shuffleToFileGroupPoolMap = new TimeStampedHashMap[ShuffleId, ShuffleFileGroupPool]

  /**
   * Maps reducers (of a particular shuffle) to the set of files that have blocks destined for them.
   * Each reducer will have one ShuffleFile per concurrent thread that executed during mapping.
   */
  private val shuffleToReducerToFilesMap =
    new TimeStampedHashMap[ShuffleId, Array[ConcurrentLinkedQueue[ShuffleFile]]]

  private
  val metadataCleaner = new MetadataCleaner(MetadataCleanerType.SHUFFLE_BLOCK_MANAGER, this.cleanup)

  def forShuffle(shuffleId: Int, numBuckets: Int, serializer: Serializer): ShuffleBlocks = {
    initializeShuffleMetadata(shuffleId, numBuckets)

    new ShuffleBlocks {
      override def acquireWriters(mapId: Int): ShuffleWriterGroup = {
        val bufferSize = System.getProperty("spark.shuffle.file.buffer.kb", "100").toInt * 1024
        var fileGroup: ShuffleFileGroup = null
        val writers = if (consolidateShuffleFiles) {
          fileGroup = getUnusedFileGroup(shuffleId, mapId, numBuckets)
          Array.tabulate[BlockObjectWriter](numBuckets) { bucketId =>
            val blockId = ShuffleBlockId(shuffleId, mapId, bucketId)
            blockManager.getDiskWriter(blockId, fileGroup(bucketId).file, serializer, bufferSize)
          }
        } else {
          Array.tabulate[BlockObjectWriter](numBuckets) { bucketId =>
            val blockId = ShuffleBlockId(shuffleId, mapId, bucketId)
            val blockFile = blockManager.diskBlockManager.getFile(blockId)
            blockManager.getDiskWriter(blockId, blockFile, serializer, bufferSize)
          }
        }
        new ShuffleWriterGroup(mapId, fileGroup, writers)
      }

      override def releaseWriters(group: ShuffleWriterGroup, success: Boolean) {
        if (consolidateShuffleFiles) {
          val fileGroup = group.fileGroup
          if (success) {
            fileGroup.addMapper(group.mapId)
            for ((writer, shuffleFile) <- group.writers.zip(fileGroup.files)) {
              shuffleFile.recordMapOutput(writer.fileSegment().offset)
            }
          }
          recycleFileGroup(shuffleId, fileGroup)
        }
      }
    }
  }

  private def initializeShuffleMetadata(shuffleId: Int, numBuckets: Int) {
    val prev = shuffleToFileGroupPoolMap.putIfAbsent(shuffleId, new ShuffleFileGroupPool())
    if (!prev.isDefined) {
      val reducerToFilesMap = new Array[ConcurrentLinkedQueue[ShuffleFile]](numBuckets)
      for (reducerId <- 0 until numBuckets) {
        reducerToFilesMap(reducerId) = new ConcurrentLinkedQueue[ShuffleFile]()
      }
      shuffleToReducerToFilesMap.put(shuffleId, reducerToFilesMap)
    }
  }

  private def getUnusedFileGroup(shuffleId: Int, mapId: Int, numBuckets: Int): ShuffleFileGroup = {
    val pool = shuffleToFileGroupPoolMap(shuffleId)
    val fileGroup = pool.getUnusedFileGroup()
    if (fileGroup == null) {
      val fileId = pool.getNextFileId()
      val files = Array.tabulate[ShuffleFile](numBuckets) { bucketId =>
        val filename = physicalFileName(shuffleId, bucketId, fileId)
        val file = blockManager.diskBlockManager.getFile(filename)
        val shuffleFile = new ShuffleFile(file)
        shuffleToReducerToFilesMap(shuffleId)(bucketId).add(shuffleFile)
        shuffleFile
      }
      new ShuffleFileGroup(shuffleId, fileId, files)
    } else {
      fileGroup
    }
  }

  private def recycleFileGroup(shuffleId: Int, fileGroup: ShuffleFileGroup) {
    shuffleToFileGroupPoolMap(shuffleId).returnFileGroup(fileGroup)
  }

  /**
   * Returns the physical file segment in which the given BlockId is located.
   * This function should only be called if shuffle file consolidation is enabled, as it is
   * an error condition if we don't find the expected block.
   */
  def getBlockLocation(id: ShuffleBlockId): FileSegment = {
    // Search all files associated with the given reducer.
    val filesForReducer = shuffleToReducerToFilesMap(id.shuffleId)(id.reduceId)
    for (file <- filesForReducer) {
      val segment = file.getFileSegmentFor(id.mapId)
      if (segment != None) { return segment.get }
    }

    throw new IllegalStateException("Failed to find shuffle block: " + id)
  }

  private def physicalFileName(shuffleId: Int, bucketId: Int, fileId: Int) = {
    "merged_shuffle_%d_%d_%d".format(shuffleId, bucketId, fileId)
  }

  private def cleanup(cleanupTime: Long) {
    shuffleToFileGroupPoolMap.clearOldValues(cleanupTime)
    shuffleToReducerToFilesMap.clearOldValues(cleanupTime)
  }
}

/**
 * A group of shuffle files, one per reducer.
 * A particular mapper will be assigned a single ShuffleFileGroup to write its output to.
 */
private[spark]
class ShuffleFileGroup(val shuffleId: Int, val fileId: Int, val files: Array[ShuffleFile]) {
  /**
   * Stores the absolute index of each mapId in the files of this group. For instance,
   * if mapId 5 is the first block in each file, mapIdToIndex(5) = 0.
   */
  private val mapIdToIndex = new PrimitiveKeyOpenHashMap[Int, Int]()

  files.foreach(_.setShuffleFileGroup(this))

  def apply(bucketId: Int) = files(bucketId)

  def addMapper(mapId: Int) {
    mapIdToIndex(mapId) = mapIdToIndex.size
  }

  def indexOf(mapId: Int): Int = mapIdToIndex.getOrElse(mapId, -1)
}

/**
 * A single, consolidated shuffle file that may contain many actual blocks. All blocks are destined
 * to the same reducer.
 */
private[spark]
class ShuffleFile(val file: File) {
  /**
   * Consecutive offsets of blocks into the file, ordered by position in the file.
   * This ordering allows us to compute block lengths by examining the following block offset.
   * Note: shuffleFileGroup.indexOf(mapId) returns the index of the mapper into this array.
   */
  private val blockOffsets = new PrimitiveVector[Long]()

  /** Back pointer to whichever ShuffleFileGroup this file is a part of. */
  private var shuffleFileGroup : ShuffleFileGroup = _

  // Required due to circular dependency between ShuffleFileGroup and ShuffleFile.
  def setShuffleFileGroup(group: ShuffleFileGroup) {
    assert(shuffleFileGroup == null)
    shuffleFileGroup = group
  }

  def recordMapOutput(offset: Long) {
    blockOffsets += offset
  }

  /**
   * Returns the FileSegment associated with the given map task, or
   * None if this ShuffleFile does not have an entry for it.
   */
  def getFileSegmentFor(mapId: Int): Option[FileSegment] = {
    val index = shuffleFileGroup.indexOf(mapId)
    if (index >= 0) {
      val offset = blockOffsets(index)
      val length =
        if (index + 1 < blockOffsets.length) {
          blockOffsets(index + 1) - offset
        } else {
          file.length() - offset
        }
      assert(length >= 0)
      Some(new FileSegment(file, offset, length))
    } else {
      None
    }
  }
}
