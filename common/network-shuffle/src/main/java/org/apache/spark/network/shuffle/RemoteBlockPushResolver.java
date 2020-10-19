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

package org.apache.spark.network.shuffle;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.StreamCallbackWithID;
import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.network.shuffle.protocol.FinalizeShuffleMerge;
import org.apache.spark.network.shuffle.protocol.MergeStatuses;
import org.apache.spark.network.shuffle.protocol.PushBlockStream;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;

/**
 * An implementation of {@link MergedShuffleFileManager} that provides the most essential shuffle
 * service processing logic to support push based shuffle.
 */
public class RemoteBlockPushResolver implements MergedShuffleFileManager {

  private static final Logger logger = LoggerFactory.getLogger(RemoteBlockPushResolver.class);
  private static final String MERGE_MANAGER_DIR = "merge_manager";

  private final ConcurrentMap<String, AppPathsInfo> appsPathInfo;
  private final ConcurrentMap<AppShufflePartitionId, AppShufflePartitionInfo> partitions;

  private final Executor directoryCleaner;
  private final TransportConf conf;
  private final int minChunkSize;
  private final String relativeMergeDirPathPattern;
  private final ErrorHandler.BlockPushErrorHandler errorHandler;

  @SuppressWarnings("UnstableApiUsage")
  private final LoadingCache<File, ShuffleIndexInformation> indexCache;

  @SuppressWarnings("UnstableApiUsage")
  public RemoteBlockPushResolver(TransportConf conf, String relativeMergeDirPathPattern) {
    this.conf = conf;
    this.partitions = Maps.newConcurrentMap();
    this.appsPathInfo = Maps.newConcurrentMap();
    this.directoryCleaner = Executors.newSingleThreadExecutor(
      // Add `spark` prefix because it will run in NM in Yarn mode.
      NettyUtils.createThreadFactory("spark-shuffle-merged-shuffle-directory-cleaner"));
    this.minChunkSize = conf.minChunkSizeInMergedShuffleFile();
    CacheLoader<File, ShuffleIndexInformation> indexCacheLoader =
      new CacheLoader<File, ShuffleIndexInformation>() {
        public ShuffleIndexInformation load(File file) throws IOException {
          return new ShuffleIndexInformation(file);
        }
      };
    indexCache = CacheBuilder.newBuilder()
      .maximumWeight(conf.mergedIndexCacheSize())
      .weigher((Weigher<File, ShuffleIndexInformation>) (file, indexInfo) -> indexInfo.getSize())
      .build(indexCacheLoader);
    this.relativeMergeDirPathPattern = relativeMergeDirPathPattern;
    this.errorHandler = new ErrorHandler.BlockPushErrorHandler();
  }

  /**
   * Given an ID that uniquely identifies a given shuffle partition of an application, retrieves the
   * associated metadata. If not present and the corresponding merged shuffle does not exist,
   * initializes the metadata.
   */
  private AppShufflePartitionInfo getOrCreateAppShufflePartitionInfo(AppShufflePartitionId id) {
    return partitions.computeIfAbsent(id, key -> {
      // It only gets here when the key is not present in the map. This could either
      // be the first time the merge manager receives a pushed block for a given application
      // shuffle partition, or after the merged shuffle file is finalized. We handle these
      // two cases accordingly by checking if the file already exists.
      File dataFile = getMergedShuffleDataFile(id);
      File indexFile = getMergedShuffleIndexFile(id);
      File metaFile = getMergedShuffleMetaFile(id);
      try {
        if (dataFile.exists()) {
          return null;
        } else {
          return new AppShufflePartitionInfo(id, dataFile, indexFile, metaFile);
        }
      } catch (IOException e) {
        logger.error(
          "Cannot create merged shuffle partition {} with shuffle file {}, index file {}, and "
            + "meta file {}", key, indexFile.getAbsolutePath(),
            indexFile.getAbsolutePath(), metaFile.getAbsolutePath());
        throw new RuntimeException(
          String.format("Cannot initialize merged shuffle partition %s", key.toString()), e);
      }
    });
  }

  @Override
  public MergedBlockMeta getMergedBlockMeta(String appId, int shuffleId, int reduceId) {
    AppShufflePartitionId id = new AppShufflePartitionId(appId, shuffleId, reduceId);
    File indexFile = getMergedShuffleIndexFile(id);
    if (!indexFile.exists()) {
      throw new RuntimeException(
        String.format("Application merged shuffle index file is not found (id=%s)", id.toString()));
    }
    int size = (int) indexFile.length();
    // First entry is the zero offset
    int numChunks = (size / Long.BYTES) - 1;
    File metaFile = getMergedShuffleMetaFile(id);
    if (!metaFile.exists()) {
      throw new RuntimeException(
        String.format("Application merged shuffle meta file is not found (id=%s)", id.toString()));
    }
    FileSegmentManagedBuffer chunkBitMaps =
      new FileSegmentManagedBuffer(conf, metaFile, 0L, metaFile.length());
    logger.trace(
      "{} shuffleId {} reduceId {} num chunks {}", appId, shuffleId, reduceId, numChunks);
    return new MergedBlockMeta(numChunks, chunkBitMaps);
  }

  @SuppressWarnings("UnstableApiUsage")
  @Override
  public ManagedBuffer getMergedBlockData(String appId, int shuffleId, int reduceId, int chunkId) {
    AppShufflePartitionId id = new AppShufflePartitionId(appId, shuffleId, reduceId);
    File mergedShuffleFile = getMergedShuffleDataFile(id);
    if (!mergedShuffleFile.exists()) {
      throw new RuntimeException(String.format("Merged shuffle file %s of %s not found",
        mergedShuffleFile.getPath(), id.toString()));
    }
    File indexFile = getMergedShuffleIndexFile(id);
    try {
      // If we get here, the merged shuffle file should have been properly finalized. Thus we can
      // use the file length to determine the size of the merged shuffle block.
      ShuffleIndexInformation shuffleIndexInformation = indexCache.get(indexFile);
      ShuffleIndexRecord shuffleIndexRecord = shuffleIndexInformation.getIndex(chunkId);
      return new FileSegmentManagedBuffer(
        conf, mergedShuffleFile, shuffleIndexRecord.getOffset(), shuffleIndexRecord.getLength());
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed to open file: " + indexFile, e);
    }
  }

  /**
   * The logic here is consistent with
   * org.apache.spark.storage.DiskBlockManager#getMergedShuffleFile
   */
  // TODO should we use subDirsPerLocalDir to potentially reduce inode size?
  private File getFile(String appId, String filename) {
    int hash = JavaUtils.nonNegativeHash(filename);
    // TODO: Change the message when this service is able to handle NM restart
    AppPathsInfo appPathsInfo = Preconditions.checkNotNull(appsPathInfo.get(appId),
      "application " + appId + " is not registered or NM was restarted.");
    Path[] activeLocalDirs = getActiveLocalDirs(appPathsInfo.activeLocalDirs);
    Path localDir = activeLocalDirs[hash % activeLocalDirs.length];
    String relativePath = getRelativePath(appPathsInfo.user, appId);
    Path filePath = localDir.resolve(relativePath);
    File targetFile = new File(filePath.toFile(), filename);
    logger.debug("Get merged file {}", targetFile.getAbsolutePath());
    return targetFile;
  }

  private Path[] getActiveLocalDirs(String[] activeLocalDirs) {
    Preconditions.checkNotNull(activeLocalDirs,
      "Active local dirs list has not been updated by any executor registration");
    return Arrays.stream(activeLocalDirs).map(localDir -> Paths.get(localDir)).toArray(Path[]::new);
  }

  private String getRelativePath(String user, String appId) {
    return String.format(relativeMergeDirPathPattern + MERGE_MANAGER_DIR, user, appId);
  }

  private File getMergedShuffleDataFile(AppShufflePartitionId id) {
    String fileName = String.format("%s.data", id.generateFileName());
    return getFile(id.appId, fileName);
  }

  private File getMergedShuffleIndexFile(AppShufflePartitionId id) {
    String indexName = String.format("%s.index", id.generateFileName());
    return getFile(id.appId, indexName);
  }

  private File getMergedShuffleMetaFile(AppShufflePartitionId id) {
    String metaName = String.format("%s.meta", id.generateFileName());
    return getFile(id.appId, metaName);
  }

  @Override
  public String[] getMergedBlockDirs(String appId) {
    AppPathsInfo appPathsInfo = Preconditions.checkNotNull(appsPathInfo.get(appId),
      "application " + appId + " is not registered or NM was restarted.");
    String[] activeLocalDirs = Preconditions.checkNotNull(appsPathInfo.get(appId).activeLocalDirs,
      "application " + appId
      + " active local dirs list has not been updated by any executor registration");
    return Arrays.stream(activeLocalDirs)
      .map(dir -> dir + getRelativePath(appPathsInfo.user, appId))
      .toArray(String[]::new);
  }

  @Override
  public void applicationRemoved(String appId, boolean cleanupLocalDirs) {
    logger.info("Application {} removed, cleanupLocalDirs = {}", appId, cleanupLocalDirs);
    // TODO: Change the message when this service is able to handle NM restart
    AppPathsInfo appPathsInfo = Preconditions.checkNotNull(appsPathInfo.remove(appId),
      "application " + appId + " is not registered or NM was restarted.");
    Iterator<Map.Entry<AppShufflePartitionId, AppShufflePartitionInfo>> iterator =
      partitions.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<AppShufflePartitionId, AppShufflePartitionInfo> entry = iterator.next();
      AppShufflePartitionId partitionId = entry.getKey();
      AppShufflePartitionInfo partition = entry.getValue();
      if (appId.equals(partitionId.appId)) {
        iterator.remove();
        try {
          partition.channel.close();
        } catch (IOException e) {
          logger.error("Error closing merged shuffle file for {}", partitionId);
        }
      }
    }

    if (cleanupLocalDirs) {
      Path[] dirs = Arrays.stream(getActiveLocalDirs(appPathsInfo.activeLocalDirs))
        .map(dir -> dir.resolve(getRelativePath(appPathsInfo.user, appId)))
        .toArray(Path[]::new);
      directoryCleaner.execute(() -> deleteExecutorDirs(dirs));
    }
  }

  /**
   * Synchronously delete local dirs, executed in a separate thread.
   */
  private void deleteExecutorDirs(Path[] dirs) {
    for (Path localDir : dirs) {
      try {
        if (Files.exists(localDir)) {
          JavaUtils.deleteRecursively(localDir.toFile());
          logger.debug("Successfully cleaned up directory: {}", localDir);
        }
      } catch (Exception e) {
        logger.error("Failed to delete directory: {}", localDir, e);
      }
    }
  }

  @Override
  public StreamCallbackWithID receiveBlockDataAsStream(PushBlockStream msg) {
    // Retrieve merged shuffle file metadata
    String[] blockIdParts = msg.blockId.split("_");
    if (blockIdParts.length != 4 || !blockIdParts[0].equals("shuffle")) {
      throw new IllegalArgumentException("Unexpected shuffle block id format: " + msg.blockId);
    }
    AppShufflePartitionId partitionId = new AppShufflePartitionId(
      msg.appId, Integer.parseInt(blockIdParts[1]), Integer.parseInt(blockIdParts[3]));
    int mapIndex = Integer.parseInt(blockIdParts[2]);
    AppShufflePartitionInfo partitionInfoBeforeCheck =
      getOrCreateAppShufflePartitionInfo(partitionId);

    // Here partitionInfo will be null in 2 cases:
    // 1) The request is received for a block that has already been merged, this is possible due
    // to the retry logic.
    // 2) The request is received after the merged shuffle is finalized, thus is too late.
    //
    // For case 1, we will drain the data in the channel and just respond success
    // to the client. This is required because the response of the previously merged
    // block will be ignored by the client, per the logic in RetryingBlockFetcher.
    // Note that the netty server should receive data for a given block id only from 1 channel
    // at any time. The block should be pushed only from successful maps, thus there should be
    // only 1 source for a given block at any time. Although the netty client might retry sending
    // this block to the server multiple times, the data of the same block always arrives from the
    // same channel thus the server should have already processed the previous request of this
    // block before seeing it again in the channel. This guarantees that we can simply just
    // check the bitmap to determine if a block is a duplicate or not.
    //
    // For case 2, we will also drain the data in the channel, but throw an exception in
    // {@link org.apache.spark.network.client.StreamCallback#onComplete(String)}. This way,
    // the client will be notified of the failure but the channel will remain active. Keeping
    // the channel alive is important because the same channel could be reused by multiple map
    // tasks in the executor JVM, which belongs to different stages. While one of the shuffles
    // in these stages is finalized, the others might still be active. Tearing down the channel
    // on the server side will disrupt these other on-going shuffle merges. It's also important
    // to notify the client of the failure, so that it can properly halt pushing the remaining
    // blocks upon receiving such failures to preserve resources on the server/client side.
    //
    // Speculative execution would also raise a possible scenario with duplicate blocks. Although
    // speculative execution would kill the slower task attempt, leading to only 1 task attempt
    // succeeding in the end, there is no guarantee that only one copy of the block will be
    // pushed. This is due to our handling of block push process outside of the map task, thus
    // it is possible for the speculative task attempt to initiate the block push process before
    // getting killed. When this happens, we need to distinguish the duplicate blocks as they
    // arrive. More details on this is explained in later comments.

    // Track if the block is received after shuffle merge finalize
    final boolean isTooLate = partitionInfoBeforeCheck == null;
    // Check if the given block is already merged by checking the bitmap against the given mapId
    final AppShufflePartitionInfo partitionInfo = partitionInfoBeforeCheck != null
      && partitionInfoBeforeCheck.mapTracker.contains(mapIndex) ? null : partitionInfoBeforeCheck;

    return new StreamCallbackWithID() {
      private int length = 0;
      // This indicates that this stream got the opportunity to write the blocks to the merged file.
      // Once this is set to true and the stream encounters a failure then it will take necessary
      // action to overwrite any partial written data. This is reset to false when the stream
      // completes without any failures.
      private boolean isWriting = false;
      // Use on-heap instead of direct ByteBuffer since these buffers will be GC'ed very quickly
      private List<ByteBuffer> deferredBufs;

      @Override
      public String getID() {
        return msg.blockId;
      }

      /**
       * Write a ByteBuffer to the merged shuffle file. Here we keep track of the length of the
       * block data written to file. In case of failure during writing block to file, we use the
       * information tracked in partitionInfo to overwrite the corrupt block when writing the new
       * block.
       */
      private void writeBuf(ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
          assert partitionInfo != null;
          if (partitionInfo.isEncounteredFailure()) {
            long updatedPos = partitionInfo.getPosition() + length;
            logger.debug(
              "{} shuffleId {} reduceId {} encountered failure current pos {} updated pos {}",
              partitionId.appId, partitionId.shuffleId, partitionId.reduceId,
              partitionInfo.getPosition(), updatedPos);
            length += partitionInfo.channel.write(buf, updatedPos);
          } else {
            length += partitionInfo.channel.write(buf);
          }
        }
      }

      /**
       * There will be multiple streams of map blocks belonging to the same reduce partition. At any
       * given point of time, only a single map stream can write it's data to the merged file. Until
       * this stream is completed, the other streams defer writing. This prevents corruption of
       * merged data. This returns whether this stream is the active stream that can write to the
       * merged file.
       */
      private boolean allowedToWrite() {
        assert partitionInfo != null;
        return partitionInfo.getCurrentMapId() < 0 || partitionInfo.getCurrentMapId() == mapIndex;
      }

      /**
       * Returns if this is a duplicate block generated by speculative tasks. With speculative
       * tasks, we could receive the same block from 2 different sources at the same time. One of
       * them is going to be the first to set the currentMapId. When that block does so, it's going
       * to see the currentMapId initially as -1. After it sets the currentMapId, it's going to
       * write some data to disk, thus increasing the length counter. The other duplicate block is
       * going to see the currentMapId already set to its mapId. However, it hasn't written any data
       * yet. If the first block gets written completely and resets the currentMapId to -1 before
       * the processing for the second block finishes, we can just check the bitmap to identify the
       * second as a duplicate.
       */
      private boolean isDuplicateBlock() {
        assert partitionInfo != null;
        return (partitionInfo.getCurrentMapId() == mapIndex && length == 0)
          || partitionInfo.mapTracker.contains(mapIndex);
      }

      /**
       * This is only invoked when the stream is able to write. The stream first writes any deferred
       * block parts buffered in memory.
       */
      private void writeAnyDeferredBlocks() throws IOException {
        assert partitionInfo != null;
        if (deferredBufs != null && !deferredBufs.isEmpty()) {
          for (ByteBuffer deferredBuf : deferredBufs) {
            writeBuf(deferredBuf);
          }
          deferredBufs = null;
        }
      }

      @Override
      public void onData(String streamId, ByteBuffer buf) throws IOException {
        // If partition info is null, ignore the requests. It could only be
        // null either when a request is received after the shuffle file is
        // finalized or when a request is for a duplicate block.
        if (partitionInfo == null) {
          return;
        }
        // When handling the block data using StreamInterceptor, it can help to reduce the amount
        // of data that needs to be buffered in memory since it does not wait till the completion
        // of the frame before handling the message, thus releasing the ByteBuf earlier. However,
        // this also means it would chunk a block into multiple buffers. Here, we want to preserve
        // the benefit of handling the block data using StreamInterceptor as much as possible while
        // providing the guarantee that one block would be continuously written to the merged
        // shuffle file before the next block starts. For each shuffle partition, we would track
        // the current map id to make sure only block matching the map id can be written to disk.
        // If one server thread sees the block being handled is the current block, it would
        // directly write the block to disk. Otherwise, it would buffer the block chunks in memory.
        // If the block becomes the current block before we see the end of it, we would then dump
        // all buffered block data to disk and write the remaining portions of the block directly
        // to disk as well. This way, we avoid having to buffer the entirety of every blocks in
        // memory, while still providing the necessary guarantee.
        synchronized (partitionInfo) {
          // If the key is no longer present in the map, it means the shuffle merge has already
          // been finalized. We should thus ignore the data and just drain the remaining bytes of
          // this message. This check should be placed inside the synchronized block to make sure
          // that checking the key is still present and processing the data is atomic.
          if (!partitions.containsKey(partitionId)) {
            // TODO is it necessary to dereference deferredBufs?
            deferredBufs = null;
            return;
          }
          // Check whether we can write to disk
          if (allowedToWrite()) {
            isWriting = true;
            // Identify duplicate block generated by speculative tasks. We respond success to
            // the client in cases of duplicate even though no data is written.
            if (isDuplicateBlock()) {
              deferredBufs = null;
              return;
            }
            logger.trace("{} shuffleId {} reduceId {} onData writable", partitionId.appId,
              partitionId.shuffleId, partitionId.reduceId);
            if (partitionInfo.getCurrentMapId() < 0) {
              partitionInfo.setCurrentMapId(mapIndex);
            }

            // If we got here, it's safe to write the block data to the merged shuffle file. We
            // first write any deferred block.
            writeAnyDeferredBlocks();
            writeBuf(buf);
            // If we got here, it means we successfully write the current chunk of block to merged
            // shuffle file. If we encountered failure while writing the previous block, we should
            // reset the file channel position and the status of partitionInfo to indicate that we
            // have recovered from previous disk write failure. However, we do not update the
            // position tracked by partitionInfo here. That is only updated while the entire block
            // is successfully written to merged shuffle file.
            if (partitionInfo.isEncounteredFailure()) {
              partitionInfo.channel.position(partitionInfo.getPosition() + length);
              partitionInfo.setEncounteredFailure(false);
            }
          } else {
            logger.trace("{} shuffleId {} reduceId {} onData deferred", partitionId.appId,
              partitionId.shuffleId, partitionId.reduceId);
            // If we cannot write to disk, we buffer the current block chunk in memory so it could
            // potentially be written to disk later. We take our best effort without guarantee
            // that the block will be written to disk. If the block data is divided into multiple
            // chunks during TCP transportation, each #onData invocation is an attempt to write
            // the block to disk. If the block is still not written to disk after all #onData
            // invocations, the final #onComplete invocation is the last attempt to write the
            // block to disk. If we still couldn't write this block to disk after this, we give up
            // on this block push request and respond failure to client. We could potentially
            // buffer the block longer or wait for a few iterations inside #onData or #onComplete
            // to increase the chance of writing the block to disk, however this would incur more
            // memory footprint or decrease the server processing throughput for the shuffle
            // service. In addition, during test we observed that by randomizing the order in
            // which clients sends block push requests batches, only ~0.5% blocks failed to be
            // written to disk due to this reason. We thus decide to optimize for server
            // throughput and memory usage.
            if (deferredBufs == null) {
              deferredBufs = new LinkedList<>();
            }
            // Write the buffer to the in-memory deferred cache
            ByteBuffer deferredBuf = ByteBuffer.allocate(buf.remaining());
            deferredBuf.put(buf);
            deferredBuf.flip();
            deferredBufs.add(deferredBuf);
          }
        }
      }

      @Override
      public void onComplete(String streamId) throws IOException {
        logger.trace("{} shuffleId {} reduceId {} onComplete invoked", partitionId.appId,
          partitionId.shuffleId, partitionId.reduceId);
        if (partitionInfo == null) {
          if (isTooLate) {
            // Throw an exception here so the block data is drained from channel and server
            // responds RpcFailure to the client.
            throw new RuntimeException(String.format("Block %s %s", msg.blockId,
              ErrorHandler.BlockPushErrorHandler.TOO_LATE_MESSAGE_SUFFIX));
          } else {
            // For duplicate block that is received before the shuffle merge finalizes, the
            // server should respond success to the client.
            return;
          }
        }
        // TODO should the merge manager check for the merge completion ratio here and finalize
        // TODO shuffle merge if appropriate? So the merge manager can potentially finalize early
        // TODO and the file channel can be closed even if finalize merge request is somehow not
        // TODO received from the driver? If so, then we need to know # maps for this shuffle.

        synchronized (partitionInfo) {
          // When this request initially got to the server, the shuffle merge finalize request
          // was not received yet. By the time we finish reading this message, the shuffle merge
          // however is already finalized. We should thus respond RpcFailure to the client.
          if (!partitions.containsKey(partitionId)) {
            deferredBufs = null;
            throw new RuntimeException(String.format("Block %s %s", msg.blockId,
              ErrorHandler.BlockPushErrorHandler.TOO_LATE_MESSAGE_SUFFIX));
          }
          // Check if we can commit this block
          if (allowedToWrite()) {
            isWriting = true;
            // Identify duplicate block generated by speculative tasks. We respond success to
            // the client in cases of duplicate even though no data is written.
            if (isDuplicateBlock()) {
              deferredBufs = null;
              return;
            }
            if (partitionInfo.getCurrentMapId() < 0) {
              writeAnyDeferredBlocks();
            }
            long updatedPos = partitionInfo.getPosition() + length;
            boolean indexUpdated = false;
            if (updatedPos - partitionInfo.getLastChunkOffset() >= minChunkSize) {
              partitionInfo.updateChunkInfo(updatedPos, mapIndex);
              indexUpdated = true;
            }
            partitionInfo.setPosition(updatedPos);
            partitionInfo.setCurrentMapId(-1);

            // update merged results
            partitionInfo.blockMerged(mapIndex);
            if (indexUpdated) {
              partitionInfo.resetChunkTracker();
            }
          } else {
            deferredBufs = null;
            throw new RuntimeException(String.format("%s %s to merged shuffle",
              ErrorHandler.BlockPushErrorHandler.BLOCK_APPEND_COLLISION_DETECTED_MSG_PREFIX,
              msg.blockId));
          }
        }
        isWriting = false;
      }

      @Override
      public void onFailure(String streamId, Throwable throwable) throws IOException {
        if (errorHandler.shouldLogError(throwable)) {
          logger.error("Encountered issue when merging shuffle partition block {}", msg, throwable);
        } else {
          logger.debug("Encountered issue when merging shuffle partition block {}", msg, throwable);
        }
        // Only update partitionInfo if the failure corresponds to a valid request. If the
        // request is too late, i.e. received after shuffle merge finalize, #onFailure will
        // also be triggered, and we can just ignore. Also, if we couldn't find an opportunity
        // to write the block data to disk, we should also ignore here.
        if (isWriting && partitionInfo != null && partitions.containsKey(partitionId)) {
          synchronized (partitionInfo) {
            logger.debug("{} shuffleId {} reduceId {} set encountered failure", partitionId.appId,
              partitionId.shuffleId, partitionId.reduceId);
            partitionInfo.setCurrentMapId(-1);
            partitionInfo.setEncounteredFailure(true);
          }
        }
      }
    };
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  @Override
  public MergeStatuses finalizeShuffleMerge(FinalizeShuffleMerge msg) throws IOException {
    logger.info("Finalizing shuffle {} from Application {}.", msg.shuffleId, msg.appId);
    List<RoaringBitmap> bitmaps = new LinkedList<>();
    List<Integer> reduceIds = new LinkedList<>();
    List<Long> sizes = new LinkedList<>();
    Iterator<Map.Entry<AppShufflePartitionId, AppShufflePartitionInfo>> iterator =
      partitions.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<AppShufflePartitionId, AppShufflePartitionInfo> entry = iterator.next();
      AppShufflePartitionId partitionId = entry.getKey();
      AppShufflePartitionInfo partition = entry.getValue();
      if (partitionId.compareAppShuffleId(msg.appId, msg.shuffleId)) {
        synchronized (partition) {
          iterator.remove();
          // Get rid of any partial block data at the end of the file. This could either
          // be due to failure or a request still being processed when the shuffle
          // merge gets finalized.
          try {
            partition.channel.truncate(partition.getPosition());
            if (partition.getPosition() != partition.getLastChunkOffset()) {
              partition.updateChunkInfo(partition.getPosition(), partition.lastMergedMapId);
            }
            bitmaps.add(partition.mapTracker);
            reduceIds.add(partitionId.reduceId);
            sizes.add(partition.getPosition());
          } catch (IOException ioe) {
            logger.warn("Exception while finalizing shuffle partition {} {} {}", msg.appId,
              msg.shuffleId, partitionId.reduceId, ioe);
          } finally {
            try {
              partition.channel.close();
              partition.metaChannel.close();
              partition.indexWriteStream.close();
            } catch (IOException closeEx) {
              logger.warn("Exception while closing stream of shuffle partition {} {} {}", msg.appId,
                msg.shuffleId, partitionId.reduceId, closeEx);
            }
          }
        }
      }
    }
    logger.info("Finalized shuffle {} from Application {}.", msg.shuffleId, msg.appId);
    return new MergeStatuses(msg.shuffleId, bitmaps.toArray(new RoaringBitmap[bitmaps.size()]),
      Ints.toArray(reduceIds), Longs.toArray(sizes));
  }

  @Override
  public void registerApplication(String appId, String user) {
    logger.debug("register application with RemoteBlockPushResolver {} {}", appId, user);
    appsPathInfo.putIfAbsent(appId, new AppPathsInfo(user));
  }

  @Override
  public void registerExecutor(String appId, String[] localDirs) {
    if (logger.isDebugEnabled()) {
      logger.debug("register executor with RemoteBlockPushResolver {} {}",
        appId, Arrays.toString(localDirs));
    }
    Preconditions.checkNotNull(appsPathInfo.get(appId),
      "application " + appId + " is not registered or NM was restarted.");
    appsPathInfo.compute(appId, (targetAppId, appPathsInfo) -> {
      assert appPathsInfo != null;
      return appPathsInfo.updateActiveLocalDirs(
        targetAppId, relativeMergeDirPathPattern, localDirs);
    });
  }

  /**
   * ID that uniquely identifies a shuffle partition for an application. This is used to key the
   * metadata tracked for each shuffle partition that's being actively merged.
   */
  public static class AppShufflePartitionId {
    public final String appId;
    public final int shuffleId;
    public final int reduceId;

    AppShufflePartitionId(String appId, int shuffleId, int reduceId) {
      this.appId = appId;
      this.shuffleId = shuffleId;
      this.reduceId = reduceId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      AppShufflePartitionId that = (AppShufflePartitionId) o;
      return shuffleId == that.shuffleId && reduceId == that.reduceId
        && Objects.equal(appId, that.appId);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(appId, shuffleId, reduceId);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("appId", appId)
        .add("shuffleId", shuffleId)
        .add("reduceId", reduceId)
        .toString();
    }

    String generateFileName() {
      return String.format("mergedShuffle_%s_%d_%d", appId, shuffleId, reduceId);
    }

    boolean compareAppShuffleId(String appId, int shuffleId) {
      return Objects.equal(this.appId, appId) && this.shuffleId == shuffleId;
    }
  }

  /** Metadata tracked for an actively merged shuffle partition */
  public static class AppShufflePartitionInfo {

    private final AppShufflePartitionId partitionId;
    // The merged shuffle data file
    final File dataFile;
    public final FileChannel channel;
    // Location offset of the last successfully merged block for this shuffle partition
    private long position;
    // Indicating whether failure was encountered when merging the previous block
    private boolean encounteredFailure;
    // Track the map Id whose block is being merged for this shuffle partition
    private int currentMapId;
    // Bitmap tracking which mapper's blocks have been merged for this shuffle partition
    private RoaringBitmap mapTracker;
    // The index file for a particular merged shuffle contains the chunk offsets.
    private final FileChannel indexChannel;
    // The meta file for a particular merged shuffle contains all the map ids that belong to every
    // chunk. The entry per chunk is a serialized bitmap.
    private final FileChannel metaChannel;
    private final DataOutputStream indexWriteStream;
    // The offset for the last chunk tracked in the index file for this shuffle partition
    private long lastChunkOffset;
    private int lastMergedMapId = -1;

    // Bitmap tracking which mapper's blocks are in the current shuffle chunk
    private RoaringBitmap chunkTracker;
    ByteBuf trackerBuf = null;

    AppShufflePartitionInfo(
        AppShufflePartitionId partitionId,
        File dataFile,
        File indexFile,
        File metaFile) throws IOException {
      this.partitionId = Preconditions.checkNotNull(partitionId, "partition id");
      dataFile.createNewFile();
      this.dataFile = dataFile;
      this.channel = new FileOutputStream(dataFile, true).getChannel();
      indexFile.createNewFile();
      FileOutputStream fos = new FileOutputStream(indexFile, true);
      indexChannel = fos.getChannel();
      this.indexWriteStream = new DataOutputStream(new BufferedOutputStream(fos));
      metaFile.createNewFile();
      metaChannel = new FileOutputStream(metaFile, true).getChannel();
      this.currentMapId = -1;
      // Writing 0 offset so that we can reuse ShuffleIndexInformation.getIndex()
      updateChunkInfo(0L, -1);
      this.position = 0;
      this.encounteredFailure = false;
      this.mapTracker = new RoaringBitmap();
      this.chunkTracker = new RoaringBitmap();
    }

    public long getPosition() {
      return position;
    }

    public void setPosition(long position) {
      logger.trace("{} shuffleId {} reduceId {} current pos {} update pos {}", partitionId.appId,
        partitionId.shuffleId, partitionId.reduceId, this.position, position);
      this.position = position;
    }

    boolean isEncounteredFailure() {
      return encounteredFailure;
    }

    void setEncounteredFailure(boolean encounteredFailure) {
      this.encounteredFailure = encounteredFailure;
    }

    int getCurrentMapId() {
      return currentMapId;
    }

    void setCurrentMapId(int mapId) {
      logger.trace("{} shuffleId {} reduceId {} updated mapId {} current mapId {}",
        partitionId.appId, partitionId.shuffleId, partitionId.reduceId, currentMapId, mapId);
      this.currentMapId = mapId;
    }

    long getLastChunkOffset() {
      return lastChunkOffset;
    }

    void blockMerged(int mapId) {
      logger.debug("{} shuffleId {} reduceId {} updated merging mapId {}", partitionId.appId,
        partitionId.shuffleId, partitionId.reduceId, mapId);
      mapTracker.add(mapId);
      chunkTracker.add(mapId);
      lastMergedMapId = mapId;
    }

    void resetChunkTracker() {
      chunkTracker.clear();
    }

    /**
     * Appends the chunk offset to the index file and adds the mapId to the chunk tracker.
     *
     * @param chunkOffset the offset of the chunk in the data file.
     * @param mapId the mapId to be added to chunk tracker.
     */
    void updateChunkInfo(long chunkOffset, int mapId) throws IOException {
      long idxStartPos = -1;
      try {
        // update the chunk tracker to meta file before index file
        writeChunkTracker(mapId);
        idxStartPos = indexChannel.position();
        logger.trace("{} shuffleId {} reduceId {} updated index current {} updated {}",
          partitionId.appId, partitionId.shuffleId, partitionId.reduceId, this.lastChunkOffset,
          chunkOffset);
        indexWriteStream.writeLong(chunkOffset);
      } catch (IOException ioe) {
        if (idxStartPos != -1) {
          // reset the position to avoid corrupting index files during exception.
          logger.warn("{} reset index to position {}", dataFile.getName(), idxStartPos);
          indexChannel.position(idxStartPos);
        }
        throw ioe;
      }
      this.lastChunkOffset = chunkOffset;
    }

    private void writeChunkTracker(int mapId) throws IOException {
      if (mapId == -1) {
        return;
      }
      chunkTracker.add(mapId);
      if (trackerBuf == null) {
        trackerBuf = Unpooled.buffer(Encoders.Bitmaps.encodedLength(chunkTracker));
      }
      Encoders.Bitmaps.encode(trackerBuf, chunkTracker);
      long metaStartPos = metaChannel.position();
      try {
        logger.trace("{} write chunk to meta file", dataFile.getName());
        metaChannel.write(trackerBuf.nioBuffer());
      } catch (IOException ioe) {
        logger.warn("{} reset position of meta file to {}", dataFile.getName(), metaStartPos);
        metaChannel.position(metaStartPos);
        throw ioe;
      } finally {
        trackerBuf.clear();
      }
    }
  }

  /**
   * Wraps all the information related to the merge directory of an application.
   */
  private static class AppPathsInfo {

    private final String user;
    private String[] activeLocalDirs;

    AppPathsInfo(String user) {
      this.user = Preconditions.checkNotNull(user, "user cannot be null");
    }

    private AppPathsInfo updateActiveLocalDirs(
        String appId, String relativePathPattern, String[] localDirs) {
      if (activeLocalDirs == null) {
        String relativePath = String.format(relativePathPattern, user, appId);
        activeLocalDirs = Arrays.stream(localDirs)
          .map(localDir -> localDir.substring(0, localDir.indexOf(relativePath)))
          .toArray(String[]::new);
        logger.info("Updated the active local dirs {} for application {}",
          Arrays.toString(activeLocalDirs), appId);
      }
      return this;
    }
  }
}
