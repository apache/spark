/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.net;

import org.junit.Assume;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;

public class TestNetUtils {

  /**
   * Some slop around expected times when making sure timeouts behave
   * as expected. We assume that they will be accurate to within
   * this threshold.
   */
  static final long TIME_FUDGE_MILLIS = 200;

  /**
   * Test that we can't accidentally connect back to the connecting socket due
   * to a quirk in the TCP spec.
   *
   * This is a regression test for HADOOP-6722.
   */
  @Test
  public void testAvoidLoopbackTcpSockets() throws Exception {
    Configuration conf = new Configuration();

    Socket socket = NetUtils.getDefaultSocketFactory(conf)
      .createSocket();
    socket.bind(new InetSocketAddress("127.0.0.1", 0));
    System.err.println("local address: " + socket.getLocalAddress());
    System.err.println("local port: " + socket.getLocalPort());
    try {
      NetUtils.connect(socket,
        new InetSocketAddress(socket.getLocalAddress(), socket.getLocalPort()),
        20000);
      socket.close();
      fail("Should not have connected");
    } catch (ConnectException ce) {
      System.err.println("Got exception: " + ce);
      assertTrue(ce.getMessage().contains("resulted in a loopback"));
    }
  }

  @Test
  public void testSocketReadTimeoutWithChannel() throws Exception {
    doSocketReadTimeoutTest(true);
  }
  
  @Test
  public void testSocketReadTimeoutWithoutChannel() throws Exception {
    doSocketReadTimeoutTest(false);
  }
  
  private void doSocketReadTimeoutTest(boolean withChannel)
      throws IOException {
    // Binding a ServerSocket is enough to accept connections.
    // Rely on the backlog to accept for us.
    ServerSocket ss = new ServerSocket(0);
    
    Socket s;
    if (withChannel) {
      s = NetUtils.getDefaultSocketFactory(new Configuration())
          .createSocket();
      Assume.assumeNotNull(s.getChannel());
    } else {
      s = new Socket();
      assertNull(s.getChannel());
    }
    
    SocketInputWrapper stm = null;
    try {
      NetUtils.connect(s, ss.getLocalSocketAddress(), 1000);

      stm = NetUtils.getInputStream(s, 1000);
      assertReadTimeout(stm, 1000);

      // Change timeout, make sure it applies.
      stm.setTimeout(1);
      assertReadTimeout(stm, 1);
      
      // If there is a channel, then setting the socket timeout
      // should not matter. If there is not a channel, it will
      // take effect.
      s.setSoTimeout(1000);
      if (withChannel) {
        assertReadTimeout(stm, 1);
      } else {
        assertReadTimeout(stm, 1000);        
      }
    } finally {
      IOUtils.closeStream(stm);
      IOUtils.closeSocket(s);
      ss.close();
    }
  }
  
  private void assertReadTimeout(SocketInputWrapper stm, int timeoutMillis)
      throws IOException {
    long st = System.nanoTime();
    try {
      stm.read();
      fail("Didn't time out");
    } catch (SocketTimeoutException ste) {
      assertTimeSince(st, timeoutMillis);
    }
  }

  private void assertTimeSince(long startNanos, int expectedMillis) {
    long durationNano = System.nanoTime() - startNanos;
    long millis = TimeUnit.MILLISECONDS.convert(
        durationNano, TimeUnit.NANOSECONDS);
    assertTrue("Expected " + expectedMillis + "ms, but took " + millis,
        Math.abs(millis - expectedMillis) < TIME_FUDGE_MILLIS);
  }
} 
