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

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.client.*;
import org.apache.spark.network.server.*;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;

public class RpcIntegrationSuite {
  static TransportConf conf;
  static TransportContext context;
  static TransportServer server;
  static TransportClientFactory clientFactory;
  static RpcHandler rpcHandler;
  static List<String> oneWayMsgs;
  static StreamTestHelper testData;

  static ConcurrentHashMap<String, VerifyingStreamCallback> streamCallbacks =
      new ConcurrentHashMap<>();

  @BeforeClass
  public static void setUp() throws Exception {
    conf = new TransportConf("shuffle", MapConfigProvider.EMPTY);
    testData = new StreamTestHelper();
    rpcHandler = new RpcHandler() {
      @Override
      public void receive(
          TransportClient client,
          ByteBuffer message,
          RpcResponseCallback callback) {
        String msg = JavaUtils.bytesToString(message);
        String[] parts = msg.split("/");
        switch (parts[0]) {
          case "hello":
            callback.onSuccess(JavaUtils.stringToBytes("Hello, " + parts[1] + "!"));
            break;
          case "return error":
            callback.onFailure(new RuntimeException("Returned: " + parts[1]));
            break;
          case "throw error":
            throw new RuntimeException("Thrown: " + parts[1]);
        }
      }

      @Override
      public StreamCallbackWithID receiveStream(
          TransportClient client,
          ByteBuffer messageHeader,
          RpcResponseCallback callback) {
        return receiveStreamHelper(JavaUtils.bytesToString(messageHeader));
      }

      @Override
      public void receive(TransportClient client, ByteBuffer message) {
        oneWayMsgs.add(JavaUtils.bytesToString(message));
      }

      @Override
      public StreamManager getStreamManager() { return new OneForOneStreamManager(); }
    };
    context = new TransportContext(conf, rpcHandler);
    server = context.createServer();
    clientFactory = context.createClientFactory();
    oneWayMsgs = new ArrayList<>();
  }

  private static StreamCallbackWithID receiveStreamHelper(String msg) {
    try {
      if (msg.startsWith("fail/")) {
        String[] parts = msg.split("/");
        switch (parts[1]) {
          case "exception-ondata":
            return new StreamCallbackWithID() {
              @Override
              public void onData(String streamId, ByteBuffer buf) throws IOException {
                throw new IOException("failed to read stream data!");
              }

              @Override
              public void onComplete(String streamId) throws IOException {
              }

              @Override
              public void onFailure(String streamId, Throwable cause) throws IOException {
              }

              @Override
              public String getID() {
                return msg;
              }
            };
          case "exception-oncomplete":
            return new StreamCallbackWithID() {
              @Override
              public void onData(String streamId, ByteBuffer buf) throws IOException {
              }

              @Override
              public void onComplete(String streamId) throws IOException {
                throw new IOException("exception in onComplete");
              }

              @Override
              public void onFailure(String streamId, Throwable cause) throws IOException {
              }

              @Override
              public String getID() {
                return msg;
              }
            };
          case "null":
            return null;
          default:
            throw new IllegalArgumentException("unexpected msg: " + msg);
        }
      } else {
        VerifyingStreamCallback streamCallback = new VerifyingStreamCallback(msg);
        streamCallbacks.put(msg, streamCallback);
        return streamCallback;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @AfterClass
  public static void tearDown() {
    server.close();
    clientFactory.close();
    context.close();
    testData.cleanup();
  }

  static class RpcResult {
    public Set<String> successMessages;
    public Set<String> errorMessages;
  }

  private RpcResult sendRPC(String ... commands) throws Exception {
    TransportClient client = clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
    final Semaphore sem = new Semaphore(0);

    final RpcResult res = new RpcResult();
    res.successMessages = Collections.synchronizedSet(new HashSet<>());
    res.errorMessages = Collections.synchronizedSet(new HashSet<>());

    RpcResponseCallback callback = new RpcResponseCallback() {
      @Override
      public void onSuccess(ByteBuffer message) {
        String response = JavaUtils.bytesToString(message);
        res.successMessages.add(response);
        sem.release();
      }

      @Override
      public void onFailure(Throwable e) {
        res.errorMessages.add(e.getMessage());
        sem.release();
      }
    };

    for (String command : commands) {
      client.sendRpc(JavaUtils.stringToBytes(command), callback);
    }

    if (!sem.tryAcquire(commands.length, 5, TimeUnit.SECONDS)) {
      fail("Timeout getting response from the server");
    }
    client.close();
    return res;
  }

  private RpcResult sendRpcWithStream(String... streams) throws Exception {
    TransportClient client = clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
    final Semaphore sem = new Semaphore(0);
    RpcResult res = new RpcResult();
    res.successMessages = Collections.synchronizedSet(new HashSet<>());
    res.errorMessages = Collections.synchronizedSet(new HashSet<>());

    for (String stream : streams) {
      int idx = stream.lastIndexOf('/');
      ManagedBuffer meta = new NioManagedBuffer(JavaUtils.stringToBytes(stream));
      String streamName = (idx == -1) ? stream : stream.substring(idx + 1);
      ManagedBuffer data = testData.openStream(conf, streamName);
      client.uploadStream(meta, data, new RpcStreamCallback(stream, res, sem));
    }

    if (!sem.tryAcquire(streams.length, 5, TimeUnit.SECONDS)) {
      fail("Timeout getting response from the server");
    }
    streamCallbacks.values().forEach(streamCallback -> {
      try {
        streamCallback.verify();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    client.close();
    return res;
  }

  private static class RpcStreamCallback implements RpcResponseCallback {
    final String streamId;
    final RpcResult res;
    final Semaphore sem;

    RpcStreamCallback(String streamId, RpcResult res, Semaphore sem) {
      this.streamId = streamId;
      this.res = res;
      this.sem = sem;
    }

    @Override
    public void onSuccess(ByteBuffer message) {
      res.successMessages.add(streamId);
      sem.release();
    }

    @Override
    public void onFailure(Throwable e) {
      res.errorMessages.add(e.getMessage());
      sem.release();
    }
  }

  @Test
  public void singleRPC() throws Exception {
    RpcResult res = sendRPC("hello/Aaron");
    assertEquals(Sets.newHashSet("Hello, Aaron!"), res.successMessages);
    assertTrue(res.errorMessages.isEmpty());
  }

  @Test
  public void doubleRPC() throws Exception {
    RpcResult res = sendRPC("hello/Aaron", "hello/Reynold");
    assertEquals(Sets.newHashSet("Hello, Aaron!", "Hello, Reynold!"), res.successMessages);
    assertTrue(res.errorMessages.isEmpty());
  }

  @Test
  public void returnErrorRPC() throws Exception {
    RpcResult res = sendRPC("return error/OK");
    assertTrue(res.successMessages.isEmpty());
    assertErrorsContain(res.errorMessages, Sets.newHashSet("Returned: OK"));
  }

  @Test
  public void throwErrorRPC() throws Exception {
    RpcResult res = sendRPC("throw error/uh-oh");
    assertTrue(res.successMessages.isEmpty());
    assertErrorsContain(res.errorMessages, Sets.newHashSet("Thrown: uh-oh"));
  }

  @Test
  public void doubleTrouble() throws Exception {
    RpcResult res = sendRPC("return error/OK", "throw error/uh-oh");
    assertTrue(res.successMessages.isEmpty());
    assertErrorsContain(res.errorMessages, Sets.newHashSet("Returned: OK", "Thrown: uh-oh"));
  }

  @Test
  public void sendSuccessAndFailure() throws Exception {
    RpcResult res = sendRPC("hello/Bob", "throw error/the", "hello/Builder", "return error/!");
    assertEquals(Sets.newHashSet("Hello, Bob!", "Hello, Builder!"), res.successMessages);
    assertErrorsContain(res.errorMessages, Sets.newHashSet("Thrown: the", "Returned: !"));
  }

  @Test
  public void sendOneWayMessage() throws Exception {
    final String message = "no reply";
    try (TransportClient client =
        clientFactory.createClient(TestUtils.getLocalHost(), server.getPort())) {
      client.send(JavaUtils.stringToBytes(message));
      assertEquals(0, client.getHandler().numOutstandingRequests());

      // Make sure the message arrives.
      long deadline = System.nanoTime() + TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
      while (System.nanoTime() < deadline && oneWayMsgs.size() == 0) {
        TimeUnit.MILLISECONDS.sleep(10);
      }

      assertEquals(1, oneWayMsgs.size());
      assertEquals(message, oneWayMsgs.get(0));
    }
  }

  @Test
  public void sendRpcWithStreamOneAtATime() throws Exception {
    for (String stream : StreamTestHelper.STREAMS) {
      RpcResult res = sendRpcWithStream(stream);
      assertTrue("there were error messages!" + res.errorMessages, res.errorMessages.isEmpty());
      assertEquals(Sets.newHashSet(stream), res.successMessages);
    }
  }

  @Test
  public void sendRpcWithStreamConcurrently() throws Exception {
    String[] streams = new String[10];
    for (int i = 0; i < 10; i++) {
      streams[i] = StreamTestHelper.STREAMS[i % StreamTestHelper.STREAMS.length];
    }
    RpcResult res = sendRpcWithStream(streams);
    assertEquals(Sets.newHashSet(StreamTestHelper.STREAMS), res.successMessages);
    assertTrue(res.errorMessages.isEmpty());
  }

  @Test
  public void sendRpcWithStreamFailures() throws Exception {
    // when there is a failure reading stream data, we don't try to keep the channel usable,
    // just send back a decent error msg.
    RpcResult exceptionInCallbackResult =
        sendRpcWithStream("fail/exception-ondata/smallBuffer", "smallBuffer");
    assertErrorAndClosed(exceptionInCallbackResult, "Destination failed while reading stream");

    RpcResult nullStreamHandler =
        sendRpcWithStream("fail/null/smallBuffer", "smallBuffer");
    assertErrorAndClosed(exceptionInCallbackResult, "Destination failed while reading stream");

    // OTOH, if there is a failure during onComplete, the channel should still be fine
    RpcResult exceptionInOnComplete =
        sendRpcWithStream("fail/exception-oncomplete/smallBuffer", "smallBuffer");
    assertErrorsContain(exceptionInOnComplete.errorMessages,
        Sets.newHashSet("Failure post-processing"));
    assertEquals(Sets.newHashSet("smallBuffer"), exceptionInOnComplete.successMessages);
  }

  private void assertErrorsContain(Set<String> errors, Set<String> contains) {
    assertEquals("Expected " + contains.size() + " errors, got " + errors.size() + "errors: " +
        errors, contains.size(), errors.size());

    Pair<Set<String>, Set<String>> r = checkErrorsContain(errors, contains);
    assertTrue("Could not find error containing " + r.getRight() + "; errors: " + errors,
        r.getRight().isEmpty());

    assertTrue(r.getLeft().isEmpty());
  }

  private void assertErrorAndClosed(RpcResult result, String expectedError) {
    assertTrue("unexpected success: " + result.successMessages, result.successMessages.isEmpty());
    Set<String> errors = result.errorMessages;
    assertEquals("Expected 2 errors, got " + errors.size() + "errors: " +
        errors, 2, errors.size());

    // We expect 1 additional error due to closed connection and here are possible keywords in the
    // error message.
    Set<String> possibleClosedErrors = Sets.newHashSet(
        "closed",
        "Connection reset",
        "java.nio.channels.ClosedChannelException",
        "java.io.IOException: Broken pipe"
    );
    Set<String> containsAndClosed = Sets.newHashSet(expectedError);
    containsAndClosed.addAll(possibleClosedErrors);

    Pair<Set<String>, Set<String>> r = checkErrorsContain(errors, containsAndClosed);

    assertTrue("Got a non-empty set " + r.getLeft(), r.getLeft().isEmpty());

    Set<String> errorsNotFound = r.getRight();
    assertEquals(
        "The size of " + errorsNotFound + " was not " + (possibleClosedErrors.size() - 1),
        possibleClosedErrors.size() - 1,
        errorsNotFound.size());
    for (String err: errorsNotFound) {
      assertTrue("Found a wrong error " + err, containsAndClosed.contains(err));
    }
  }

  private Pair<Set<String>, Set<String>> checkErrorsContain(
      Set<String> errors,
      Set<String> contains) {
    Set<String> remainingErrors = Sets.newHashSet(errors);
    Set<String> notFound = Sets.newHashSet();
    for (String contain : contains) {
      Iterator<String> it = remainingErrors.iterator();
      boolean foundMatch = false;
      while (it.hasNext()) {
        if (it.next().contains(contain)) {
          it.remove();
          foundMatch = true;
          break;
        }
      }
      if (!foundMatch) {
        notFound.add(contain);
      }
    }
    return new ImmutablePair<>(remainingErrors, notFound);
  }

  private static class VerifyingStreamCallback implements StreamCallbackWithID {
    final String streamId;
    final StreamSuite.TestCallback helper;
    final OutputStream out;
    final File outFile;

    VerifyingStreamCallback(String streamId) throws IOException {
      if (streamId.equals("file")) {
        outFile = File.createTempFile("data", ".tmp", testData.tempDir);
        out = new FileOutputStream(outFile);
      } else {
        out = new ByteArrayOutputStream();
        outFile = null;
      }
      this.streamId = streamId;
      helper = new StreamSuite.TestCallback(out);
    }

    void verify() throws IOException {
      if (streamId.equals("file")) {
        assertTrue("File stream did not match.", Files.equal(testData.testFile, outFile));
      } else {
        byte[] result = ((ByteArrayOutputStream)out).toByteArray();
        ByteBuffer srcBuffer = testData.srcBuffer(streamId);
        ByteBuffer base;
        synchronized (srcBuffer) {
          base = srcBuffer.duplicate();
        }
        byte[] expected = new byte[base.remaining()];
        base.get(expected);
        assertEquals(expected.length, result.length);
        assertArrayEquals("buffers don't match", expected, result);
      }
    }

    @Override
    public void onData(String streamId, ByteBuffer buf) throws IOException {
      helper.onData(streamId, buf);
    }

    @Override
    public void onComplete(String streamId) throws IOException {
      helper.onComplete(streamId);
    }

    @Override
    public void onFailure(String streamId, Throwable cause) throws IOException {
      helper.onFailure(streamId, cause);
    }

    @Override
    public String getID() {
      return streamId;
    }
  }
}
