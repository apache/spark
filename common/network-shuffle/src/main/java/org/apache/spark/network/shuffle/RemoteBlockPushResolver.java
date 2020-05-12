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
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.StreamCallbackWithID;
import org.apache.spark.network.shuffle.protocol.FinalizeShuffleMerge;
import org.apache.spark.network.shuffle.protocol.MergeStatuses;
import org.apache.spark.network.shuffle.protocol.PushBlockStream;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;

/**
 * An implementation of MergedShuffleFileManager that provides the most essential shuffle
 * service processing logic to support push based shuffle.
 */
public class RemoteBlockPushResolver implements MergedShuffleFileManager {

  private static final Logger logger = LoggerFactory.getLogger(RemoteBlockPushResolver.class);

  private final Path[] localDirs;
  private final ConcurrentMap<String, Path> appsRelativePath;
  private final ConcurrentMap<AppShufflePartitionId, AppShufflePartitionInfo> partitions;

  private final Executor directoryCleaner;
  private final TransportConf conf;
  private final int minChunkSize;

  private final LoadingCache<File, ShuffleIndexInformation> indexCache;

  @SuppressWarnings("UnstableApiUsage")
  public RemoteBlockPushResolver(TransportConf conf, String[] localDirs) {
    this.conf = conf;
    this.localDirs = new Path[localDirs.length];
    for (int i = 0; i < localDirs.length; i++) {
      this.localDirs[i] = Paths.get(localDirs[i]);
    }
    this.partitions = Maps.newConcurrentMap();
    this.appsRelativePath = Maps.newConcurrentMap();
    this.directoryCleaner = Executors.newSingleThreadExecutor(
        // Add `spark` prefix because it will run in NM in Yarn mode.
        NettyUtils.createThreadFactory("spark-shuffle-merged-shuffle-directory-cleaner"));
    this.minChunkSize = conf.minChunkSizeInMergedShuffleFile();
    String indexCacheSize = conf.get("spark.shuffle.service.index.cache.size", "100m");
    CacheLoader<File, ShuffleIndexInformation> indexCacheLoader =
        new CacheLoader<File, ShuffleIndexInformation>() {
          public ShuffleIndexInformation load(File file) throws IOException {
            return new ShuffleIndexInformation(file);
          }
        };
    indexCache = CacheBuilder.newBuilder()
        .maximumWeight(JavaUtils.byteStringAsBytes(indexCacheSize))
        .weigher((Weigher<File, ShuffleIndexInformation>) (file, indexInfo) -> indexInfo.getSize())
        .build(indexCacheLoader);
  }

  /**
   * Given an ID that uniquely identifies a given shuffle partition of an application, retrieves
   * the associated metadata. If not present and the corresponding merged shuffle does not exist,
   * initializes the metadata.
   */
  private AppShufflePartitionInfo getOrCreateAppShufflePartitionInfo(
      AppShufflePartitionId id) {
    return partitions.computeIfAbsent(id, key -> {
      // It only gets here when the key is not present in the map. This could either
      // be the first time the merge manager receives a pushed block for a given application
      // shuffle partition, or after the merged shuffle file is finalized. We handle these
      // two cases accordingly by checking if the file already exists.
      try {
        File mergedShuffleFile = getMergedShuffleFile(key);
        if (mergedShuffleFile.exists()) {
          return null;
        } else {
          return new AppShufflePartitionInfo(mergedShuffleFile, getMergedIndexFile(id));
        }
      } catch (IOException e) {
        throw new RuntimeException(String.format(
            "Cannot initialize merged shuffle partition %s", key.toString()), e);
      }
    });
  }

  @Override
  public int getChunkCount(String appId, int shuffleId, int reduceId) {
    AppShufflePartitionId id = new AppShufflePartitionId(appId, shuffleId, reduceId);
    File indexFile = getMergedIndexFile(id);
    if (!indexFile.exists()) {
      throw new RuntimeException(
          String.format("Application merged shuffle index file is not found (id=%s)",
              id.toString()));
    }
    int size = (int) indexFile.length();
    // First entry is the zero offset
    return (size / Long.BYTES) - 1;
  }

  @SuppressWarnings("UnstableApiUsage")
  @Override
  public ManagedBuffer getMergedBlockData(String appId, int shuffleId, int reduceId, int chunkId) {
    AppShufflePartitionId id = new AppShufflePartitionId(appId, shuffleId, reduceId);
    File mergedShuffleFile = getMergedShuffleFile(id);
    if (!mergedShuffleFile.exists()) {
      throw new RuntimeException(
          String.format("Merged shuffle file %s of %s not found", mergedShuffleFile.getPath(),
              id.toString()));
    }
    File indexFile = getMergedIndexFile(id);
    try {
      // If we get here, the merged shuffle file should have been properly finalized. Thus we can
      // use the file length to determine the size of the merged shuffle block.
      ShuffleIndexInformation shuffleIndexInformation = indexCache.get(indexFile);
      ShuffleIndexRecord shuffleIndexRecord = shuffleIndexInformation.getIndex(chunkId);
      return new FileSegmentManagedBuffer(
          conf,
          mergedShuffleFile,
          shuffleIndexRecord.getOffset(),
          shuffleIndexRecord.getLength());
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
    Path localDir = localDirs[hash % localDirs.length];
    Path relativeMergeDir = Preconditions.checkNotNull(
        appsRelativePath.get(appId), "application " + appId + " is not registered.");
    return new File(localDir.resolve(relativeMergeDir).toFile(), filename);
  }

  private File getMergedShuffleFile(AppShufflePartitionId id) {
    String fileName = id.generateFileName();
    return getFile(id.appId, fileName);
  }

  private File getMergedIndexFile(AppShufflePartitionId id) {
    String indexName = id.generateIndexFileName();
    return getFile(id.appId, indexName);
  }

  @Override
  public void applicationRemoved(String appId, boolean cleanupLocalDirs) {
    logger.info("Application {} removed, cleanupLocalDirs = {}", appId, cleanupLocalDirs);
    Path relativeMergeDir = appsRelativePath.remove(appId);
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
      Path[] dirs = Arrays.stream(localDirs)
          .map(dir -> dir.resolve(relativeMergeDir)).toArray(Path[]::new);
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
        logger.error("Failed to delete directory: " + localDir, e);
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
    AppShufflePartitionId partitionId = new AppShufflePartitionId(msg.appId,
        Integer.parseInt(blockIdParts[1]), Integer.parseInt(blockIdParts[3]));
    int mapId = Integer.parseInt(blockIdParts[2]);
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
        && partitionInfoBeforeCheck.mapTracker.contains(mapId) ? null : partitionInfoBeforeCheck;

    return new StreamCallbackWithID() {
      private int length = 0;
      private boolean canWrite = true;
      // Use on-heap instead of direct ByteBuffer since these buffers will be GC'ed very quickly
      private List<ByteBuffer> deferredBufs;

      @Override
      public String getID() {
        return msg.blockId;
      }

      /**
       * Write a ByteBuffer to the merged shuffle file. Here we keep track of the length of
       * the block data written to file. In case of failure during writing block to file,
       * we use the information tracked in partitionInfo to overwrite the corrupt block
       * when writing the new block.
       */
      private void writeBuf(ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
          if (partitionInfo.isEncounteredFailure()) {
            length += partitionInfo.channel.write(buf, partitionInfo.getPosition() + length);
          } else {
            length += partitionInfo.channel.write(buf);
          }
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
          if (partitionInfo.getCurrentMapId() < 0 || partitionInfo.getCurrentMapId() == mapId) {
            // Check if this is a duplicate block generated by speculative tasks. With speculative
            // tasks, we could receive the same block from 2 different sources at the same time.
            // One of them is going to be the first to set the currentMapId. When that block does
            // so, it's going to see the currentMapId initially as -1. After it sets the
            // currentMapId, it's going to write some data to disk, thus increasing the length
            // counter. The other duplicate block is going to see the currentMapId already set to
            // its mapId. However, it hasn't written any data yet. If the first block gets written
            // completely and resets the currentMapId to -1 before the processing for the second
            // block finishes, we can just check the bitmap to identify the second as a duplicate.
            if ((partitionInfo.getCurrentMapId() == mapId && length == 0) ||
                partitionInfo.mapTracker.contains(mapId)) {
              deferredBufs = null;
              return;
            }
            if (partitionInfo.getCurrentMapId() < 0) {
              partitionInfo.setCurrentMapId(mapId);
            }

            // If we got here, it's safe to write the block data to the merged shuffle file. We
            // first write any deferred block chunk buffered in memory, then write the remaining
            // of the block.
            if (deferredBufs != null && !deferredBufs.isEmpty()) {
              for (ByteBuffer deferredBuf : deferredBufs) {
                writeBuf(deferredBuf);
              }
              deferredBufs = null;
            }
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
        if (partitionInfo == null) {
          if (isTooLate) {
            // Throw an exception here so the block data is drained from channel and server
            // responds RpcFailure to the client.
            throw new RuntimeException(String.format("Block %s %s", msg.blockId,
              BlockPushException.TOO_LATE_MESSAGE_SUFFIX));
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
              BlockPushException.TOO_LATE_MESSAGE_SUFFIX));
          }
          // Check if we can commit this block
          if (partitionInfo.getCurrentMapId() < 0 || partitionInfo.getCurrentMapId() == mapId) {
            // Identify duplicate block generated by speculative tasks. We respond success to
            // the client in cases of duplicate even though no data is written.
            if ((partitionInfo.getCurrentMapId() == mapId && length == 0) ||
                partitionInfo.mapTracker.contains(mapId)) {
              deferredBufs = null;
              return;
            }
            if (partitionInfo.getCurrentMapId() < 0 && deferredBufs != null
                && !deferredBufs.isEmpty()) {
              for (ByteBuffer deferredBuf : deferredBufs) {
                writeBuf(deferredBuf);
              }
              deferredBufs = null;
            }
            long updatedPos = partitionInfo.getPosition() + length;
            if (updatedPos - partitionInfo.getLastChunkOffset() >= minChunkSize) {
              partitionInfo.updateLastChunkOffset(updatedPos);
            }
            partitionInfo.setPosition(updatedPos);
            partitionInfo.setCurrentMapId(-1);

            // update merged results
            partitionInfo.blockMerged(mapId);
          } else {
            deferredBufs = null;
            canWrite = false;
            throw new RuntimeException(String.format("Couldn't find an opportunity to write "
                + "block %s to merged shuffle", msg.blockId));
          }
        }
      }

      @Override
      public void onFailure(String streamId, Throwable cause) throws IOException {
        logger.error("Encountered issue when merging shuffle partition block {}", msg, cause);
        // Only update partitionInfo if the failure corresponds to a valid request. If the
        // request is too late, i.e. received after shuffle merge finalize, #onFailure will
        // also be triggered, and we can just ignore. Also, if we couldn't find an opportunity
        // to write the block data to disk, we should also ignore here.
        if (canWrite && partitionInfo != null && partitions.containsKey(partitionId)) {
          synchronized (partitionInfo) {
            partitionInfo.setCurrentMapId(-1);
            partitionInfo.setEncounteredFailure(true);
          }
        }
      }
    };
  }

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
              partition.updateLastChunkOffset(partition.getPosition());
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
  public void registerApplication(String appId, String relativeAppPath) {
    logger.debug("register application with RemoteBlockPushResolver {} {}", appId, relativeAppPath);
    appsRelativePath.put(appId, Paths.get(relativeAppPath));
  }

  /**
   * ID that uniquely identifies a shuffle partition for an application. This is used to key
   * the metadata tracked for each shuffle partition that's being actively merged.
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
      return String.format("mergedShuffle_%s_%d_%d.data", appId, shuffleId, reduceId);
    }

    String generateIndexFileName() {
      return String.format("mergedShuffle_%s_%d_%d.index", appId, shuffleId, reduceId);
    }

    boolean compareAppShuffleId(String appId, int shuffleId) {
      return Objects.equal(this.appId, appId) && this.shuffleId == shuffleId;
    }
  }

  /**
   * Metadata tracked for an actively merged shuffle partition
   */
  public static class AppShufflePartitionInfo {
    // The merged shuffle data file
    final File targetFile;
    public final FileChannel channel;
    // Location offset of the last successfully merged block for this shuffle partition
    private long position;
    // Indicating whether failure was encountered when merging the previous block
    private boolean encounteredFailure;
    // Track the map Id whose block is being merged for this shuffle partition
    private int currentMapId;
    // Bitmap tracking which mapper's blocks have been merged for this shuffle partition
    private RoaringBitmap mapTracker;
    // The merged shuffle index file
    private final FileChannel indexChannel;
    private final DataOutputStream indexWriteStream;
    // The offset for the last chunk tracked in the index file for this shuffle partition
    private long lastChunkOffset;

    AppShufflePartitionInfo(File targetFile, File indexFile) throws IOException {
      targetFile.createNewFile();
      this.targetFile = targetFile;
      this.channel = new FileOutputStream(targetFile, true).getChannel();
      indexFile.createNewFile();
      FileOutputStream fos = new FileOutputStream(indexFile, true);
      indexChannel = fos.getChannel();
      this.indexWriteStream = new DataOutputStream(new BufferedOutputStream(fos));
      // Writing 0 offset so that we can reuse ShuffleIndexInformation.getIndex()
      updateLastChunkOffset(0L);
      this.position = 0;
      this.encounteredFailure = false;
      this.currentMapId = -1;
      this.mapTracker = new RoaringBitmap();
    }

    public long getPosition() {
      return position;
    }

    public void setPosition(long position) {
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
      this.currentMapId = mapId;
    }

    long getLastChunkOffset() {
      return lastChunkOffset;
    }

    void blockMerged(int mapId) {
      mapTracker.add(mapId);
    }

    void updateLastChunkOffset(long lastChunkOffset) throws IOException {
      long startPos = indexChannel.position();
      try {
        indexWriteStream.writeLong(lastChunkOffset);
      } catch(IOException ioe) {
        // reset the position to avoid corrupting index files during exception.
        indexChannel.position(startPos);
        throw ioe;
      }
      this.lastChunkOffset = lastChunkOffset;
    }
  }
}
