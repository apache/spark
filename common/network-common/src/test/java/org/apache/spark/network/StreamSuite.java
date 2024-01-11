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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.io.Files;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.StreamCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;

public class StreamSuite {
  private static final String[] STREAMS = StreamTestHelper.STREAMS;
  private static StreamTestHelper testData;

  private static TransportContext context;
  private static TransportServer server;
  private static TransportClientFactory clientFactory;

  private static ByteBuffer createBuffer(int bufSize) {
    ByteBuffer buf = ByteBuffer.allocate(bufSize);
    for (int i = 0; i < bufSize; i ++) {
      buf.put((byte) i);
    }
    buf.flip();
    return buf;
  }

  @BeforeAll
  public static void setUp() throws Exception {
    testData = new StreamTestHelper();

    final TransportConf conf = new TransportConf("shuffle", MapConfigProvider.EMPTY);
    final StreamManager streamManager = new StreamManager() {
      @Override
      public ManagedBuffer getChunk(long streamId, int chunkIndex) {
        throw new UnsupportedOperationException();
      }

      @Override
      public ManagedBuffer openStream(String streamId) {
        return testData.openStream(conf, streamId);
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

  @AfterAll
  public static void tearDown() {
    server.close();
    clientFactory.close();
    testData.cleanup();
    context.close();
  }

  @Test
  public void testZeroLengthStream() throws Throwable {
    try (TransportClient client =
        clientFactory.createClient(TestUtils.getLocalHost(), server.getPort())) {
      StreamTask task = new StreamTask(client, "emptyBuffer", TimeUnit.SECONDS.toMillis(5));
      task.run();
      task.check();
    }
  }

  @Test
  public void testSingleStream() throws Throwable {
    try (TransportClient client =
        clientFactory.createClient(TestUtils.getLocalHost(), server.getPort())) {
      StreamTask task = new StreamTask(client, "largeBuffer", TimeUnit.SECONDS.toMillis(5));
      task.run();
      task.check();
    }
  }

  @Test
  public void testMultipleStreams() throws Throwable {
    try (TransportClient client =
        clientFactory.createClient(TestUtils.getLocalHost(), server.getPort())) {
      for (int i = 0; i < 20; i++) {
        StreamTask task = new StreamTask(client, STREAMS[i % STREAMS.length],
          TimeUnit.SECONDS.toMillis(5));
        task.run();
        task.check();
      }
    }
  }

  @Test
  public void testConcurrentStreams() throws Throwable {
    ExecutorService executor = Executors.newFixedThreadPool(20);

    try (TransportClient client =
        clientFactory.createClient(TestUtils.getLocalHost(), server.getPort())) {
      List<StreamTask> tasks = new ArrayList<>();
      for (int i = 0; i < 20; i++) {
        StreamTask task = new StreamTask(client, STREAMS[i % STREAMS.length],
          TimeUnit.SECONDS.toMillis(20));
        tasks.add(task);
        executor.submit(task);
      }

      executor.shutdown();
      assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS), "Timed out waiting for tasks.");
      for (StreamTask task : tasks) {
        task.check();
      }
    } finally {
      executor.shutdownNow();
    }
  }

  private static class StreamTask implements Runnable {

    private final TransportClient client;
    private final String streamId;
    private final long timeoutMs;
    private Throwable error;

    StreamTask(TransportClient client, String streamId, long timeoutMs) {
      this.client = client;
      this.streamId = streamId;
      this.timeoutMs = timeoutMs;
    }

    @Override
    public void run() {
      ByteBuffer srcBuffer = null;
      OutputStream out = null;
      File outFile = null;
      try {
        ByteArrayOutputStream baos = null;

        switch (streamId) {
          case "largeBuffer" -> {
            baos = new ByteArrayOutputStream();
            out = baos;
            srcBuffer = testData.largeBuffer;
          }
          case "smallBuffer" -> {
            baos = new ByteArrayOutputStream();
            out = baos;
            srcBuffer = testData.smallBuffer;
          }
          case "file" -> {
            outFile = File.createTempFile("data", ".tmp", testData.tempDir);
            out = new FileOutputStream(outFile);
          }
          case "emptyBuffer" -> {
            baos = new ByteArrayOutputStream();
            out = baos;
            srcBuffer = testData.emptyBuffer;
          }
          default -> throw new IllegalArgumentException(streamId);
        }

        TestCallback callback = new TestCallback(out);
        client.stream(streamId, callback);
        callback.waitForCompletion(timeoutMs);

        if (srcBuffer == null) {
          assertTrue(Files.equal(testData.testFile, outFile), "File stream did not match.");
        } else {
          ByteBuffer base;
          synchronized (srcBuffer) {
            base = srcBuffer.duplicate();
          }
          byte[] result = baos.toByteArray();
          byte[] expected = new byte[base.remaining()];
          base.get(expected);
          assertEquals(expected.length, result.length);
          assertArrayEquals(expected, result, "buffers don't match");
        }
      } catch (Throwable t) {
        error = t;
      } finally {
        if (out != null) {
          try {
            out.close();
          } catch (Exception e) {
            // ignore.
          }
        }
        if (outFile != null) {
          outFile.delete();
        }
      }
    }

    public void check() throws Throwable {
      if (error != null) {
        throw error;
      }
    }
  }

  static class TestCallback implements StreamCallback {

    private final OutputStream out;
    public volatile boolean completed;
    public volatile Throwable error;

    TestCallback(OutputStream out) {
      this.out = out;
      this.completed = false;
    }

    @Override
    public void onData(String streamId, ByteBuffer buf) throws IOException {
      byte[] tmp = new byte[buf.remaining()];
      buf.get(tmp);
      out.write(tmp);
    }

    @Override
    public void onComplete(String streamId) throws IOException {
      out.close();
      synchronized (this) {
        completed = true;
        notifyAll();
      }
    }

    @Override
    public void onFailure(String streamId, Throwable cause) {
      error = cause;
      synchronized (this) {
        completed = true;
        notifyAll();
      }
    }

    void waitForCompletion(long timeoutMs) {
      long now = System.currentTimeMillis();
      long deadline = now + timeoutMs;
      synchronized (this) {
        while (!completed && now < deadline) {
          try {
            wait(deadline - now);
          } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
          }
          now = System.currentTimeMillis();
        }
      }
      assertTrue(completed, "Timed out waiting for stream.");
      assertNull(error);
    }
  }

}
