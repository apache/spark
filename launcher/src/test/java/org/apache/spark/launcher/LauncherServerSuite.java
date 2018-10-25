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

package org.apache.spark.launcher;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import static org.junit.Assert.*;

import static org.apache.spark.launcher.LauncherProtocol.*;

public class LauncherServerSuite extends BaseSuite {

  @Test
  public void testLauncherServerReuse() throws Exception {
    LauncherServer server1 = LauncherServer.getOrCreateServer();
    ChildProcAppHandle handle = new ChildProcAppHandle(server1);
    handle.kill();

    LauncherServer server2 = LauncherServer.getOrCreateServer();
    try {
      assertNotSame(server1, server2);
    } finally {
      server2.unref();
    }
  }

  @Test
  public void testCommunication() throws Exception {
    LauncherServer server = LauncherServer.getOrCreateServer();
    ChildProcAppHandle handle = new ChildProcAppHandle(server);
    String secret = server.registerHandle(handle);

    TestClient client = null;
    try {
      Socket s = new Socket(InetAddress.getLoopbackAddress(), server.getPort());

      final Semaphore semaphore = new Semaphore(0);
      handle.addListener(new SparkAppHandle.Listener() {
        @Override
        public void stateChanged(SparkAppHandle handle) {
          semaphore.release();
        }
        @Override
        public void infoChanged(SparkAppHandle handle) {
          semaphore.release();
        }
      });

      client = new TestClient(s);
      client.send(new Hello(secret, "1.4.0"));
      assertTrue(semaphore.tryAcquire(30, TimeUnit.SECONDS));

      // Make sure the server matched the client to the handle.
      assertNotNull(handle.getConnection());

      client.send(new SetAppId("app-id"));
      assertTrue(semaphore.tryAcquire(30, TimeUnit.SECONDS));
      assertEquals("app-id", handle.getAppId());

      client.send(new SetState(SparkAppHandle.State.RUNNING));
      assertTrue(semaphore.tryAcquire(1, TimeUnit.SECONDS));
      assertEquals(SparkAppHandle.State.RUNNING, handle.getState());

      handle.stop();
      Message stopMsg = client.inbound.poll(30, TimeUnit.SECONDS);
      assertTrue(stopMsg instanceof Stop);
    } finally {
      close(client);
      handle.kill();
      client.clientThread.join();
    }
  }

  @Test
  public void testTimeout() throws Exception {
    LauncherServer server = LauncherServer.getOrCreateServer();
    ChildProcAppHandle handle = new ChildProcAppHandle(server);
    String secret = server.registerHandle(handle);

    TestClient client = null;
    try {
      // LauncherServer will immediately close the server-side socket when the timeout is set
      // to 0.
      SparkLauncher.setConfig(SparkLauncher.CHILD_CONNECTION_TIMEOUT, "0");

      Socket s = new Socket(InetAddress.getLoopbackAddress(), server.getPort());
      client = new TestClient(s);
      waitForError(client, secret);
    } finally {
      SparkLauncher.launcherConfig.remove(SparkLauncher.CHILD_CONNECTION_TIMEOUT);
      handle.kill();
      close(client);
    }
  }

  @Test
  public void testSparkSubmitVmShutsDown() throws Exception {
    LauncherServer server = LauncherServer.getOrCreateServer();
    ChildProcAppHandle handle = new ChildProcAppHandle(server);
    String secret = server.registerHandle(handle);

    TestClient client = null;
    final Semaphore semaphore = new Semaphore(0);
    try {
      Socket s = new Socket(InetAddress.getLoopbackAddress(), server.getPort());
      handle.addListener(new SparkAppHandle.Listener() {
        public void stateChanged(SparkAppHandle handle) {
          semaphore.release();
        }
        public void infoChanged(SparkAppHandle handle) {
          semaphore.release();
        }
      });
      client = new TestClient(s);
      client.send(new Hello(secret, "1.4.0"));
      assertTrue(semaphore.tryAcquire(30, TimeUnit.SECONDS));
      // Make sure the server matched the client to the handle.
      assertNotNull(handle.getConnection());
      client.close();
      handle.dispose();
      assertTrue(semaphore.tryAcquire(30, TimeUnit.SECONDS));
      assertEquals(SparkAppHandle.State.LOST, handle.getState());
    } finally {
      handle.kill();
      close(client);
      client.clientThread.join();
    }
  }

  @Test
  public void testStreamFiltering() throws Exception {
    LauncherServer server = LauncherServer.getOrCreateServer();
    ChildProcAppHandle handle = new ChildProcAppHandle(server);
    String secret = server.registerHandle(handle);

    TestClient client = null;
    try {
      Socket s = new Socket(InetAddress.getLoopbackAddress(), server.getPort());

      client = new TestClient(s);

      try {
        client.send(new EvilPayload());
      } catch (SocketException se) {
        // SPARK-21522: this can happen if the server closes the socket before the full message has
        // been written, so it's expected. It may cause false positives though (socket errors
        // happening for other reasons).
      }

      waitForError(client, secret);
      assertEquals(0, EvilPayload.EVIL_BIT);
    } finally {
      handle.kill();
      close(client);
      client.clientThread.join();
    }
  }

  @Test
  public void testAppHandleDisconnect() throws Exception {
    LauncherServer server = LauncherServer.getOrCreateServer();
    ChildProcAppHandle handle = new ChildProcAppHandle(server);
    String secret = server.registerHandle(handle);

    TestClient client = null;
    try {
      Socket s = new Socket(InetAddress.getLoopbackAddress(), server.getPort());
      client = new TestClient(s);
      client.send(new Hello(secret, "1.4.0"));
      client.send(new SetAppId("someId"));

      // Wait until we know the server has received the messages and matched the handle to the
      // connection before disconnecting.
      eventually(Duration.ofSeconds(1), Duration.ofMillis(10), () -> {
        assertEquals("someId", handle.getAppId());
      });

      handle.disconnect();
      waitForError(client, secret);
    } finally {
      handle.kill();
      close(client);
      client.clientThread.join();
    }
  }

  private void close(Closeable c) {
    if (c != null) {
      try {
        c.close();
      } catch (Exception e) {
        // no-op.
      }
    }
  }

  /**
   * Try a few times to get a client-side error, since the client-side socket may not reflect the
   * server-side close immediately.
   */
  private void waitForError(TestClient client, String secret) throws Exception {
    final AtomicBoolean helloSent = new AtomicBoolean();
    eventually(Duration.ofSeconds(1), Duration.ofMillis(10), () -> {
      try {
        if (!helloSent.get()) {
          client.send(new Hello(secret, "1.4.0"));
          helloSent.set(true);
        } else {
          client.send(new SetAppId("appId"));
        }
        fail("Expected error but message went through.");
      } catch (IllegalStateException | IOException e) {
        // Expected.
      }
    });
  }

  private static class TestClient extends LauncherConnection {

    final BlockingQueue<Message> inbound;
    final Thread clientThread;

    TestClient(Socket s) throws IOException {
      super(s);
      this.inbound = new LinkedBlockingQueue<>();
      this.clientThread = new Thread(this);
      clientThread.setName("TestClient");
      clientThread.setDaemon(true);
      clientThread.start();
    }

    @Override
    protected void handle(Message msg) throws IOException {
      inbound.offer(msg);
    }

  }

  private static class EvilPayload extends LauncherProtocol.Message {

    static int EVIL_BIT = 0;

    // This field should cause the launcher server to throw an error and not deserialize the
    // message.
    private List<String> notAllowedField = Arrays.asList("disallowed");

    private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
      stream.defaultReadObject();
      EVIL_BIT = 1;
    }

  }

}
