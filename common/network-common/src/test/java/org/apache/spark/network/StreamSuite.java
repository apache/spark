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
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.io.Files;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
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
  private static final String[] STREAMS = { "largeBuffer", "smallBuffer", "emptyBuffer", "file" };

  private static TransportServer server;
  private static TransportClientFactory clientFactory;
  private static File testFile;
  private static File tempDir;

  private static ByteBuffer emptyBuffer;
  private static ByteBuffer smallBuffer;
  private static ByteBuffer largeBuffer;

  private static ByteBuffer createBuffer(int bufSize) {
    ByteBuffer buf = ByteBuffer.allocate(bufSize);
    for (int i = 0; i < bufSize; i ++) {
      buf.put((byte) i);
    }
    buf.flip();
    return buf;
  }

  @BeforeClass
  public static void setUp() throws Exception {
    tempDir = Files.createTempDir();
    emptyBuffer = createBuffer(0);
    smallBuffer = createBuffer(100);
    largeBuffer = createBuffer(100000);

    testFile = File.createTempFile("stream-test-file", "txt", tempDir);
    FileOutputStream fp = new FileOutputStream(testFile);
    try {
      Random rnd = new Random();
      for (int i = 0; i < 512; i++) {
        byte[] fileContent = new byte[1024];
        rnd.nextBytes(fileContent);
        fp.write(fileContent);
      }
    } finally {
      fp.close();
    }

    final TransportConf conf = new TransportConf("shuffle", MapConfigProvider.EMPTY);
    final StreamManager streamManager = new StreamManager() {
      @Override
      public ManagedBuffer getChunk(long streamId, int chunkIndex) {
        throw new UnsupportedOperationException();
      }

      @Override
      public ManagedBuffer openStream(String streamId) {
        switch (streamId) {
          case "largeBuffer":
            return new NioManagedBuffer(largeBuffer);
          case "smallBuffer":
            return new NioManagedBuffer(smallBuffer);
          case "emptyBuffer":
            return new NioManagedBuffer(emptyBuffer);
          case "file":
            return new FileSegmentManagedBuffer(conf, testFile, 0, testFile.length());
          default:
            throw new IllegalArgumentException("Invalid stream: " + streamId);
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
    TransportContext context = new TransportContext(conf, handler);
    server = context.createServer();
    clientFactory = context.createClientFactory();
  }

  @AfterClass
  public static void tearDown() {
    server.close();
    clientFactory.close();
    if (tempDir != null) {
      for (File f : tempDir.listFiles()) {
        f.delete();
      }
      tempDir.delete();
    }
  }

  @Test
  public void testZeroLengthStream() throws Throwable {
    TransportClient client = clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
    try {
      StreamTask task = new StreamTask(client, "emptyBuffer", TimeUnit.SECONDS.toMillis(5));
      task.run();
      task.check();
    } finally {
      client.close();
    }
  }

  @Test
  public void testSingleStream() throws Throwable {
    TransportClient client = clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
    try {
      StreamTask task = new StreamTask(client, "largeBuffer", TimeUnit.SECONDS.toMillis(5));
      task.run();
      task.check();
    } finally {
      client.close();
    }
  }

  @Test
  public void testMultipleStreams() throws Throwable {
    TransportClient client = clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
    try {
      for (int i = 0; i < 20; i++) {
        StreamTask task = new StreamTask(client, STREAMS[i % STREAMS.length],
          TimeUnit.SECONDS.toMillis(5));
        task.run();
        task.check();
      }
    } finally {
      client.close();
    }
  }

  @Test
  public void testConcurrentStreams() throws Throwable {
    ExecutorService executor = Executors.newFixedThreadPool(20);
    TransportClient client = clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());

    try {
      List<StreamTask> tasks = new ArrayList<>();
      for (int i = 0; i < 20; i++) {
        StreamTask task = new StreamTask(client, STREAMS[i % STREAMS.length],
          TimeUnit.SECONDS.toMillis(20));
        tasks.add(task);
        executor.submit(task);
      }

      executor.shutdown();
      assertTrue("Timed out waiting for tasks.", executor.awaitTermination(30, TimeUnit.SECONDS));
      for (StreamTask task : tasks) {
        task.check();
      }
    } finally {
      executor.shutdownNow();
      client.close();
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
          case "largeBuffer":
            baos = new ByteArrayOutputStream();
            out = baos;
            srcBuffer = largeBuffer;
            break;
          case "smallBuffer":
            baos = new ByteArrayOutputStream();
            out = baos;
            srcBuffer = smallBuffer;
            break;
          case "file":
            outFile = File.createTempFile("data", ".tmp", tempDir);
            out = new FileOutputStream(outFile);
            break;
          case "emptyBuffer":
            baos = new ByteArrayOutputStream();
            out = baos;
            srcBuffer = emptyBuffer;
            break;
          default:
            throw new IllegalArgumentException(streamId);
        }

        TestCallback callback = new TestCallback(out);
        client.stream(streamId, callback);
        waitForCompletion(callback);

        if (srcBuffer == null) {
          assertTrue("File stream did not match.", Files.equal(testFile, outFile));
        } else {
          ByteBuffer base;
          synchronized (srcBuffer) {
            base = srcBuffer.duplicate();
          }
          byte[] result = baos.toByteArray();
          byte[] expected = new byte[base.remaining()];
          base.get(expected);
          assertEquals(expected.length, result.length);
          assertTrue("buffers don't match", Arrays.equals(expected, result));
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

    private void waitForCompletion(TestCallback callback) throws Exception {
      long now = System.currentTimeMillis();
      long deadline = now + timeoutMs;
      synchronized (callback) {
        while (!callback.completed && now < deadline) {
          callback.wait(deadline - now);
          now = System.currentTimeMillis();
        }
      }
      assertTrue("Timed out waiting for stream.", callback.completed);
      assertNull(callback.error);
    }

  }

  private static class TestCallback implements StreamCallback {

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

  }

}
