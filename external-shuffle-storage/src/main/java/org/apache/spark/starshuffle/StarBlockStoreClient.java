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

package org.apache.spark.starshuffle;

import org.apache.spark.SparkEnv;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.netty.SparkTransportConf;
import org.apache.spark.network.shuffle.*;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.ShuffleBlockId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * This class fetches shuffle blocks from external storage like S3
 */
public class StarBlockStoreClient extends BlockStoreClient {

    private static final Logger logger = LoggerFactory.getLogger(StarBlockStoreClient.class);

    // Fetch shuffle blocks from external shuffle storage.
    // The shuffle location is encoded in the host argument. In the future, we should enhance
    // Spark internal code to support abstraction of shuffle storage location.
    @Override
    public void fetchBlocks(String host, int port, String execId, String[] blockIds, BlockFetchingListener listener, DownloadFileManager downloadFileManager) {
        for (int i = 0; i < blockIds.length; i++) {
            String blockId = blockIds[i];
            CompletableFuture.runAsync(() -> fetchBlock(host, execId, blockId, listener, downloadFileManager));
        }
    }

    private void fetchBlock(String host, String execId, String blockIdStr, BlockFetchingListener listener, DownloadFileManager downloadFileManager) {
        BlockId blockId = BlockId.apply(blockIdStr);
        if (blockId instanceof ShuffleBlockId) {
            ShuffleBlockId shuffleBlockId = (ShuffleBlockId)blockId;
            StarMapResultFileInfo mapResultFileInfo = StarMapResultFileInfo.deserializeFromString(host);
            long offset = 0;
            for (int i = 0; i < shuffleBlockId.reduceId(); i++) {
                offset += mapResultFileInfo.getPartitionLengths()[i];
            }
            long size = mapResultFileInfo.getPartitionLengths()[shuffleBlockId.reduceId()];
            StarShuffleFileManager streamProvider = StarUtils.createShuffleFileManager(SparkEnv.get().conf(),
                    mapResultFileInfo.getLocation());
            if (downloadFileManager != null) {
                try (InputStream inputStream = streamProvider.read(mapResultFileInfo.getLocation(), offset, size)) {
                    TransportConf transportConf = SparkTransportConf.fromSparkConf(
                            SparkEnv.get().conf(), "starShuffle", 1, Option.empty());
                    DownloadFile downloadFile = downloadFileManager.createTempFile(transportConf);
                    downloadFileManager.registerTempFileToClean(downloadFile);
                    DownloadFileWritableChannel downloadFileWritableChannel = downloadFile.openForWriting();

                    int bufferSize = 64 * 1024;
                    byte[] bytes = new byte[bufferSize];
                    int readBytes = 0;
                    while (readBytes < size) {
                        int toReadBytes = Math.min((int)size - readBytes, bufferSize);
                        int n = inputStream.read(bytes, 0, toReadBytes);
                        if (n == -1) {
                            throw new RuntimeException(String.format(
                                    "Failed to read file %s for shuffle block %s, hit end with remaining %s bytes",
                                    mapResultFileInfo.getLocation(),
                                    blockId,
                                    size - readBytes));
                        }
                        readBytes += n;
                        downloadFileWritableChannel.write(ByteBuffer.wrap(bytes, 0, n));
                    }
                    ManagedBuffer managedBuffer = downloadFileWritableChannel.closeAndRead();
                    listener.onBlockFetchSuccess(blockIdStr, managedBuffer);
                } catch (IOException e) {
                    throw new RuntimeException(String.format(
                            "Failed to read file %s for shuffle block %s",
                            mapResultFileInfo.getLocation(),
                            blockId));
                }
            } else {
                try (InputStream inputStream = streamProvider.read(mapResultFileInfo.getLocation(), offset, size)) {
                    ByteBuffer byteBuffer = ByteBuffer.allocate((int)size);
                    int b = inputStream.read();
                    while (b != -1) {
                        byteBuffer.put((byte)b);
                        if (byteBuffer.position() == size) {
                            break;
                        }
                        b = inputStream.read();
                    }
                    byteBuffer.flip();
                    NioManagedBuffer managedBuffer = new NioManagedBuffer(byteBuffer);
                    listener.onBlockFetchSuccess(blockIdStr, managedBuffer);
                } catch (IOException e) {
                    throw new RuntimeException(String.format(
                            "Failed to read file %s for shuffle block %s",
                            mapResultFileInfo.getLocation(),
                            blockId));
                }
            }
            logger.info("Fetch blocks: {}, {}", host, execId);
        } else {
            throw new RuntimeException(String.format(
                    "%s does not support %s: %s",
                    this.getClass().getSimpleName(),
                    blockId.getClass().getSimpleName(),
                    blockId));
        }
    }

    @Override
    public void close() throws IOException {
        logger.info("Close");
    }
}
