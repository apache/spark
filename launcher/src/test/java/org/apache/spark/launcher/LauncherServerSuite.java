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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.spark.launcher.LauncherProtocol.Hello;
import org.apache.spark.launcher.LauncherProtocol.Message;
import org.apache.spark.launcher.LauncherProtocol.SetAppId;
import org.apache.spark.launcher.LauncherProtocol.SetState;
import org.apache.spark.launcher.LauncherProtocol.Stop;
import org.apache.spark.launcher.SparkAppHandle.State;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;


public class LauncherServerSuite extends BaseSuite {

  @Test
  public void testLauncherServerReuse() throws Exception {
    ChildProcAppHandle handle1 = null;
    ChildProcAppHandle handle2 = null;
    ChildProcAppHandle handle3 = null;

    try {
      handle1 = LauncherServer.newAppHandle();
      handle2 = LauncherServer.newAppHandle();
      LauncherServer server1 = handle1.getServer();
      assertSame(server1, handle2.getServer());

      handle1.kill();
      handle2.kill();

      handle3 = LauncherServer.newAppHandle();
      assertNotSame(server1, handle3.getServer());

      handle3.kill();

      assertNull(LauncherServer.getServerInstance());
    } finally {
      kill(handle1);
      kill(handle2);
      kill(handle3);
    }
  }

  @Test
  public void testCommunication() throws Exception {
    ChildProcAppHandle handle = LauncherServer.newAppHandle();
    TestClient client = null;
    try {
      Socket s = new Socket(InetAddress.getLoopbackAddress(),
        LauncherServer.getServerInstance().getPort());

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
      client.send(new Hello(handle.getSecret(), "1.4.0"));
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
      kill(handle);
      close(client);
      client.clientThread.join();
    }
  }

  @Test
  public void testTimeout() throws Exception {
    ChildProcAppHandle handle = null;
    TestClient client = null;
    try {
      // LauncherServer will immediately close the server-side socket when the timeout is set
      // to 0.
      SparkLauncher.setConfig(SparkLauncher.CHILD_CONNECTION_TIMEOUT, "0");

      handle = LauncherServer.newAppHandle();

      Socket s = new Socket(InetAddress.getLoopbackAddress(),
        LauncherServer.getServerInstance().getPort());
      client = new TestClient(s);

      // Try a few times since the client-side socket may not reflect the server-side close
      // immediately.
      boolean helloSent = false;
      int maxTries = 10;
      for (int i = 0; i < maxTries; i++) {
        try {
          if (!helloSent) {
            client.send(new Hello(handle.getSecret(), "1.4.0"));
            helloSent = true;
          } else {
            client.send(new SetAppId("appId"));
          }
          fail("Expected exception caused by connection timeout.");
        } catch (IllegalStateException | IOException e) {
          // Expected.
          break;
        } catch (AssertionError e) {
          if (i < maxTries - 1) {
            Thread.sleep(100);
          } else {
            throw new AssertionError("Test failed after " + maxTries + " attempts.", e);
          }
        }
      }
    } finally {
      SparkLauncher.launcherConfig.remove(SparkLauncher.CHILD_CONNECTION_TIMEOUT);
      kill(handle);
      close(client);
    }
  }

  @Test
  public void testSparkSubmitVmShutsDown() throws Exception {
	LauncherServer launchServer = (LauncherServer)Mockito.mock(LauncherServer.class);
	Socket socket = (Socket)Mockito.mock(Socket.class);
	Mockito.when(socket.getOutputStream()).thenReturn(new ByteArrayOutputStream());
	Mockito.when(socket.getInputStream()).thenReturn(getMessageInputStream(new LauncherProtocol.Message[] {
	  new LauncherProtocol.Hello("someSecret", "someVersion"),
	  new LauncherProtocol.SetState(SparkAppHandle.State.SUBMITTED), new LauncherProtocol.SetState(
	    SparkAppHandle.State.RUNNING) })).thenThrow(new Throwable[] {
	      new EOFException("Probably Remote JVM Shutdown") });

	ChildProcAppHandle appHandle = new ChildProcAppHandle("someSecret", launchServer);
	final BlockingQueue stateQueue = new ArrayBlockingQueue(10);
	List<State> expectedStateList = Lists.newArrayList(State.CONNECTED,State.SUBMITTED,State.RUNNING, State.FINISHED_UNKNOWN);
	final List<State> realStateList = Lists.newArrayList();
	appHandle.addListener(new SparkAppHandle.Listener() {
	  public void stateChanged(SparkAppHandle handle) {
	    stateQueue.offer(handle.getState());
	  }

	  public void infoChanged(SparkAppHandle handle) {
	  }
	});
	DummyLauncherConnection launcherConnection = new DummyLauncherConnection(
	  socket, appHandle);
	final AtomicBoolean jobFinished = new AtomicBoolean(false);
	Thread clientPollerThread = new Thread() {
	  public void run() {
	    while (!jobFinished.get()) {
	      SparkAppHandle.State state = SparkAppHandle.State.UNKNOWN;
	      try {
	        state = (SparkAppHandle.State)stateQueue.take();
	      } catch (InterruptedException e) {
	        Thread.currentThread().interrupt();
	        throw new RuntimeException(e);
	      }

	      switch (state) {
	        case CONNECTED:
	          realStateList.add(state);
	          break;
	        case FAILED:
              realStateList.add(state);
	          jobFinished.set(true);
	          break;
	        case FINISHED:
	          jobFinished.set(true);
	          realStateList.add(state);
	          break;
	        case KILLED:
	          jobFinished.set(true);
	          realStateList.add(state);
	          break;
	        case RUNNING:
	          realStateList.add(state);
	          break;
	        case SUBMITTED:
	          realStateList.add(state);
	          break;
	        case UNKNOWN:
	          realStateList.add(state);
	          break;
	        case FINISHED_UNKNOWN:
	          jobFinished.set(true);
	          realStateList.add(state);
	          break;
	        default:
	          throw new RuntimeException("some exception");
	      }
	    }
	  }
	};
	clientPollerThread.start();
	Thread launcherThread = new Thread(launcherConnection);
	launcherThread.start();
	launcherThread.join();

	clientPollerThread.join(10000L);
	assertEquals(expectedStateList, realStateList);
	assertTrue(jobFinished.get());
  }

  private static InputStream getMessageInputStream(Message ... messages) {
	ByteArrayOutputStream out = new ByteArrayOutputStream();
	try {
	  ObjectOutputStream os = new ObjectOutputStream(out);
	  for (Message message : messages) {
		os.writeObject(message);
	  }
	} catch (IOException e) {
	  throw new RuntimeException(e);
	}
	return new ByteArrayInputStream(out.toByteArray());
  }


  private void kill(SparkAppHandle handle) {
    if (handle != null) {
      handle.kill();
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

  public class DummyLauncherConnection extends LauncherConnection {
 	private ChildProcAppHandle _appAppHandle;
 	DummyLauncherConnection(Socket socket, ChildProcAppHandle appHandle)
 		throws IOException {
 		super(socket);
 		_appAppHandle = appHandle;
 	}

 	@Override
 	protected void handle(Message msg) throws IOException {
 	  try {
 		if (msg instanceof Hello) {
 		  if (_appAppHandle != null) {
 		    _appAppHandle.setState(SparkAppHandle.State.CONNECTED);
 			_appAppHandle.setConnection(this);
 		  } else {
 			throw new IllegalArgumentException("Received Hello for unknown client.");
 		  }
 		} else {
 		  if (_appAppHandle == null) {
 			throw new IllegalArgumentException("Expected hello, got: " + msg != null ? msg.getClass().getName() : null);
 	      }
 		  if (msg instanceof SetAppId) {
 		    SetAppId set = (SetAppId) msg;
 		    _appAppHandle.setAppId(set.appId);
 		  } else if (msg instanceof SetState) {
 		    _appAppHandle.setState(((SetState) msg).state);
 		  } else {
 			throw new IllegalArgumentException("Invalid message: " + msg != null ? msg.getClass().getName() : null);
 		  }
 	    }
 	  } catch (Exception e) {
 	  close();
 	  }
    }
  }

}
