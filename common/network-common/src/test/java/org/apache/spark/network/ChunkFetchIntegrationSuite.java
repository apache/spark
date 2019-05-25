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

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.client.ChunkReceivedCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;

public class ChunkFetchIntegrationSuite {
  static final long STREAM_ID = 1;
  static final int BUFFER_CHUNK_INDEX = 0;
  static final int FILE_CHUNK_INDEX = 1;

  static TransportContext context;
  static TransportServer server;
  static TransportClientFactory clientFactory;
  static StreamManager streamManager;
  static File testFile;

  static ManagedBuffer bufferChunk;
  static ManagedBuffer fileChunk;

  @BeforeClass
  public static void setUp() throws Exception {
    int bufSize = 100000;
    final ByteBuffer buf = ByteBuffer.allocate(bufSize);
    for (int i = 0; i < bufSize; i ++) {
      buf.put((byte) i);
    }
    buf.flip();
    bufferChunk = new NioManagedBuffer(buf);

    testFile = File.createTempFile("shuffle-test-file", "txt");
    testFile.deleteOnExit();
    RandomAccessFile fp = new RandomAccessFile(testFile, "rw");
    boolean shouldSuppressIOException = true;
    try {
      byte[] fileContent = new byte[1024];
      new Random().nextBytes(fileContent);
      fp.write(fileContent);
      shouldSuppressIOException = false;
    } finally {
      Closeables.close(fp, shouldSuppressIOException);
    }

    final TransportConf conf = new TransportConf("shuffle", MapConfigProvider.EMPTY);
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
      public void receive(
          TransportClient client,
          ByteBuffer message,
          RpcResponseCallback callback) {
        throw new UnsupportedOperationException();
      }

      @Override
      public StreamManager getStreamManager() {
        return streamManager;
      }
    };
    context = new TransportContext(conf, handler);
    server = context.createServer();
    clientFactory = context.createClientFactory();
  }

  @AfterClass
  public static void tearDown() {
    bufferChunk.release();
    server.close();
    clientFactory.close();
    context.close();
    testFile.delete();
  }

  static class FetchResult {
    public Set<Integer> successChunks;
    public Set<Integer> failedChunks;
    public List<ManagedBuffer> buffers;

    public void releaseBuffers() {
      for (ManagedBuffer buffer : buffers) {
        buffer.release();
      }
    }
  }

  private FetchResult fetchChunks(List<Integer> chunkIndices) throws Exception {
    final FetchResult res = new FetchResult();

    try (TransportClient client =
      clientFactory.createClient(TestUtils.getLocalHost(), server.getPort())) {
      final Semaphore sem = new Semaphore(0);

      res.successChunks = Collections.synchronizedSet(new HashSet<Integer>());
      res.failedChunks = Collections.synchronizedSet(new HashSet<Integer>());
      res.buffers = Collections.synchronizedList(new LinkedList<ManagedBuffer>());

      ChunkReceivedCallback callback = new ChunkReceivedCallback() {
        @Override
        public void onSuccess(int chunkIndex, ManagedBuffer buffer) {
          buffer.retain();
          res.successChunks.add(chunkIndex);
          res.buffers.add(buffer);
          sem.release();
        }

        @Override
        public void onFailure(int chunkIndex, Throwable e) {
          res.failedChunks.add(chunkIndex);
          sem.release();
        }
      };

      for (int chunkIndex : chunkIndices) {
        client.fetchChunk(STREAM_ID, chunkIndex, callback);
      }
      if (!sem.tryAcquire(chunkIndices.size(), 60, TimeUnit.SECONDS)) {
        fail("Timeout getting response from the server");
      }
    }
    return res;
  }

  @Test
  public void fetchBufferChunk() throws Exception {
    FetchResult res = fetchChunks(Arrays.asList(BUFFER_CHUNK_INDEX));
    assertEquals(Sets.newHashSet(BUFFER_CHUNK_INDEX), res.successChunks);
    assertTrue(res.failedChunks.isEmpty());
    assertBufferListsEqual(Arrays.asList(bufferChunk), res.buffers);
    res.releaseBuffers();
  }

  @Test
  public void fetchFileChunk() throws Exception {
    FetchResult res = fetchChunks(Arrays.asList(FILE_CHUNK_INDEX));
    assertEquals(Sets.newHashSet(FILE_CHUNK_INDEX), res.successChunks);
    assertTrue(res.failedChunks.isEmpty());
    assertBufferListsEqual(Arrays.asList(fileChunk), res.buffers);
    res.releaseBuffers();
  }

  @Test
  public void fetchNonExistentChunk() throws Exception {
    FetchResult res = fetchChunks(Arrays.asList(12345));
    assertTrue(res.successChunks.isEmpty());
    assertEquals(Sets.newHashSet(12345), res.failedChunks);
    assertTrue(res.buffers.isEmpty());
  }

  @Test
  public void fetchBothChunks() throws Exception {
    FetchResult res = fetchChunks(Arrays.asList(BUFFER_CHUNK_INDEX, FILE_CHUNK_INDEX));
    assertEquals(Sets.newHashSet(BUFFER_CHUNK_INDEX, FILE_CHUNK_INDEX), res.successChunks);
    assertTrue(res.failedChunks.isEmpty());
    assertBufferListsEqual(Arrays.asList(bufferChunk, fileChunk), res.buffers);
    res.releaseBuffers();
  }

  @Test
  public void fetchChunkAndNonExistent() throws Exception {
    FetchResult res = fetchChunks(Arrays.asList(BUFFER_CHUNK_INDEX, 12345));
    assertEquals(Sets.newHashSet(BUFFER_CHUNK_INDEX), res.successChunks);
    assertEquals(Sets.newHashSet(12345), res.failedChunks);
    assertBufferListsEqual(Arrays.asList(bufferChunk), res.buffers);
    res.releaseBuffers();
  }

  private static void assertBufferListsEqual(List<ManagedBuffer> list0, List<ManagedBuffer> list1)
      throws Exception {
    assertEquals(list0.size(), list1.size());
    for (int i = 0; i < list0.size(); i ++) {
      assertBuffersEqual(list0.get(i), list1.get(i));
    }
  }

  private static void assertBuffersEqual(ManagedBuffer buffer0, ManagedBuffer buffer1)
      throws Exception {
    ByteBuffer nio0 = buffer0.nioByteBuffer();
    ByteBuffer nio1 = buffer1.nioByteBuffer();

    int len = nio0.remaining();
    assertEquals(nio0.remaining(), nio1.remaining());
    for (int i = 0; i < len; i ++) {
      assertEquals(nio0.get(), nio1.get());
    }
  }
}
