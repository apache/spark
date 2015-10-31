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
package org.apache.spark.network;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.client.ChunkReceivedCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.util.ssl.SslSampleConfigs;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class SslChunkFetchIntegrationSuite extends ChunkFetchIntegrationSuite {

  @BeforeClass
  public static void setUp() throws Exception {
    int bufSize = 100000;
    final ByteBuffer buf = ByteBuffer.allocate(bufSize);
    for (int i = 0; i < bufSize; i++) {
      buf.put((byte) i);
    }
    buf.flip();
    bufferChunk = new NioManagedBuffer(buf);

    testFile = File.createTempFile("shuffle-test-file", "txt");
    testFile.deleteOnExit();
    RandomAccessFile fp = new RandomAccessFile(testFile, "rw");
    byte[] fileContent = new byte[1024];
    new Random().nextBytes(fileContent);
    fp.write(fileContent);
    fp.close();

    final TransportConf conf = new TransportConf(SslSampleConfigs.createDefaultConfigProvider());

    fileChunk = new FileSegmentManagedBuffer(conf, testFile, 10, testFile.length() - 25);

    streamManager = new StreamManager() {
      @Override
      public ManagedBuffer getChunk(long streamId, int chunkIndex) {
        assertEquals(STREAM_ID, streamId);
        if (chunkIndex == BUFFER_CHUNK_INDEX) {
          return new NioManagedBuffer(buf);
        } else if (chunkIndex == FILE_CHUNK_INDEX) {
          return new FileSegmentManagedBuffer(conf, testFile, 10, testFile.length() - 25);
        } else {
          throw new IllegalArgumentException("Invalid chunk index: " + chunkIndex);
        }
      }
    };
    RpcHandler handler = new RpcHandler() {
      @Override
      public void receive(TransportClient client, byte[] message, RpcResponseCallback callback) {
        throw new UnsupportedOperationException();
      }

      @Override
      public StreamManager getStreamManager() {
        return streamManager;
      }
    };
    TransportContext context = new TransportContext(conf, handler);
    server = context.createServer();
    clientFactory = context.createClientFactory();
  }
}
