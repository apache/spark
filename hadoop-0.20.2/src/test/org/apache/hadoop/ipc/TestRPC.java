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

package org.apache.hadoop.ipc;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;

import junit.framework.TestCase;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.spi.NullContext;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;
import org.apache.hadoop.security.AccessControlException;

/** Unit tests for RPC. */
public class TestRPC extends TestCase {
  private static final String ADDRESS = "0.0.0.0";

  public static final Log LOG =
    LogFactory.getLog(TestRPC.class);
  
  private static Configuration conf = new Configuration();

  int datasize = 1024*100;
  int numThreads = 50;

  public TestRPC(String name) { super(name); }
	
  public interface TestProtocol extends VersionedProtocol {
    public static final long versionID = 1L;
    
    void ping() throws IOException;
    void slowPing(boolean shouldSlow) throws IOException;
    String echo(String value) throws IOException;
    String[] echo(String[] value) throws IOException;
    Writable echo(Writable value) throws IOException;
    int add(int v1, int v2) throws IOException;
    int add(int[] values) throws IOException;
    int error() throws IOException;
    void testServerGet() throws IOException;
    int[] exchange(int[] values) throws IOException;
    String toString();
  }

  public static class TestImpl implements TestProtocol {
    int fastPingCounter = 0;
    
    public long getProtocolVersion(String protocol, long clientVersion) {
      return TestProtocol.versionID;
    }
    
    public void ping() {}

    public synchronized void slowPing(boolean shouldSlow) {
      if (shouldSlow) {
        while (fastPingCounter < 2) {
          try {
          wait();  // slow response until two fast pings happened
          } catch (InterruptedException ignored) {}
        }
        fastPingCounter -= 2;
      } else {
        fastPingCounter++;
        notify();
      }
    }
    
    public String echo(String value) throws IOException { return value; }

    public String[] echo(String[] values) throws IOException { return values; }

    public Writable echo(Writable writable) {
      return writable;
    }
    public int add(int v1, int v2) {
      return v1 + v2;
    }

    public int add(int[] values) {
      int sum = 0;
      for (int i = 0; i < values.length; i++) {
        sum += values[i];
      }
      return sum;
    }

    public int error() throws IOException {
      throw new IOException("bobo");
    }

    public void testServerGet() throws IOException {
      if (!(Server.get() instanceof RPC.Server)) {
        throw new IOException("Server.get() failed");
      }
    }

    public int[] exchange(int[] values) {
      for (int i = 0; i < values.length; i++) {
        values[i] = i;
      }
      return values;
    }
  }

  //
  // an object that does a bunch of transactions
  //
  static class Transactions implements Runnable {
    int datasize;
    TestProtocol proxy;

    Transactions(TestProtocol proxy, int datasize) {
      this.proxy = proxy;
      this.datasize = datasize;
    }

    // do two RPC that transfers data.
    public void run() {
      int[] indata = new int[datasize];
      int[] outdata = null;
      int val = 0;
      try {
        outdata = proxy.exchange(indata);
        val = proxy.add(1,2);
      } catch (IOException e) {
        assertTrue("Exception from RPC exchange() "  + e, false);
      }
      assertEquals(indata.length, outdata.length);
      assertEquals(val, 3);
      for (int i = 0; i < outdata.length; i++) {
        assertEquals(outdata[i], i);
      }
    }
  }

  //
  // A class that does an RPC but does not read its response.
  //
  static class SlowRPC implements Runnable {
    private TestProtocol proxy;
    private volatile boolean done;
   
    SlowRPC(TestProtocol proxy) {
      this.proxy = proxy;
      done = false;
    }

    boolean isDone() {
      return done;
    }

    public void run() {
      try {
        proxy.slowPing(true);   // this would hang until two fast pings happened
        done = true;
      } catch (IOException e) {
        assertTrue("SlowRPC ping exception " + e, false);
      }
    }
  }

  public void testSlowRpc() throws Exception {
    System.out.println("Testing Slow RPC");
    // create a server with two handlers
    Server server = RPC.getServer(new TestImpl(), ADDRESS, 0, 2, false, conf);
    TestProtocol proxy = null;
    
    try {
    server.start();

    InetSocketAddress addr = NetUtils.getConnectAddress(server);

    // create a client
    proxy = (TestProtocol)RPC.getProxy(
        TestProtocol.class, TestProtocol.versionID, addr, conf);

    SlowRPC slowrpc = new SlowRPC(proxy);
    Thread thread = new Thread(slowrpc, "SlowRPC");
    thread.start(); // send a slow RPC, which won't return until two fast pings
    assertTrue("Slow RPC should not have finished1.", !slowrpc.isDone());

    proxy.slowPing(false); // first fast ping
    
    // verify that the first RPC is still stuck
    assertTrue("Slow RPC should not have finished2.", !slowrpc.isDone());

    proxy.slowPing(false); // second fast ping
    
    // Now the slow ping should be able to be executed
    while (!slowrpc.isDone()) {
      System.out.println("Waiting for slow RPC to get done.");
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {}
    }
    } finally {
      server.stop();
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
      System.out.println("Down slow rpc testing");
    }
  }

  public void testCalls(Configuration conf) throws Exception {
    Server server = RPC.getServer(new TestImpl(), ADDRESS, 0, conf);
    TestProtocol proxy = null;
    try {
    server.start();

    InetSocketAddress addr = NetUtils.getConnectAddress(server);
    proxy = (TestProtocol)RPC.getProxy(
        TestProtocol.class, TestProtocol.versionID, addr, conf);
      
    proxy.ping();

    String stringResult = proxy.echo("foo");
    assertEquals(stringResult, "foo");

    stringResult = proxy.echo((String)null);
    assertEquals(stringResult, null);
    
    // Check rpcMetrics 
    server.rpcMetrics.doUpdates(new NullContext());
    
    // Number 4 includes getProtocolVersion()
    assertEquals(4, server.rpcMetrics.rpcProcessingTime.getPreviousIntervalNumOps());
    assertTrue(server.rpcMetrics.sentBytes.getPreviousIntervalValue() > 0);
    assertTrue(server.rpcMetrics.receivedBytes.getPreviousIntervalValue() > 0);
    
    // Number of calls to echo method should be 2
    server.rpcDetailedMetrics.doUpdates(new NullContext());
    MetricsTimeVaryingRate metrics = 
      (MetricsTimeVaryingRate)server.rpcDetailedMetrics.registry.get("echo");
    assertEquals(2, metrics.getPreviousIntervalNumOps());
    
    // Number of calls to ping method should be 1
    metrics = 
      (MetricsTimeVaryingRate)server.rpcDetailedMetrics.registry.get("ping");
    assertEquals(1, metrics.getPreviousIntervalNumOps());
    
    String[] stringResults = proxy.echo(new String[]{"foo","bar"});
    assertTrue(Arrays.equals(stringResults, new String[]{"foo","bar"}));

    stringResults = proxy.echo((String[])null);
    assertTrue(Arrays.equals(stringResults, null));

    UTF8 utf8Result = (UTF8)proxy.echo(new UTF8("hello world"));
    assertEquals(utf8Result, new UTF8("hello world"));

    utf8Result = (UTF8)proxy.echo((UTF8)null);
    assertEquals(utf8Result, null);

    int intResult = proxy.add(1, 2);
    assertEquals(intResult, 3);

    intResult = proxy.add(new int[] {1, 2});
    assertEquals(intResult, 3);

    boolean caught = false;
    try {
      proxy.error();
    } catch (IOException e) {
      LOG.debug("Caught " + e);
      caught = true;
    }
    assertTrue(caught);

    proxy.testServerGet();

    // create multiple threads and make them do large data transfers
    System.out.println("Starting multi-threaded RPC test...");
    server.setSocketSendBufSize(1024);
    Thread threadId[] = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      Transactions trans = new Transactions(proxy, datasize);
      threadId[i] = new Thread(trans, "TransactionThread-" + i);
      threadId[i].start();
    }

    // wait for all transactions to get over
    System.out.println("Waiting for all threads to finish RPCs...");
    for (int i = 0; i < numThreads; i++) {
      try {
        threadId[i].join();
      } catch (InterruptedException e) {
        i--;      // retry
      }
    }

    // try some multi-calls
    Method echo =
      TestProtocol.class.getMethod("echo", new Class[] { String.class });
    String[] strings = (String[])RPC.call(echo, new String[][]{{"a"},{"b"}},
                                          new InetSocketAddress[] {addr, addr}, conf);
    assertTrue(Arrays.equals(strings, new String[]{"a","b"}));

    Method ping = TestProtocol.class.getMethod("ping", new Class[] {});
    Object[] voids = (Object[])RPC.call(ping, new Object[][]{{},{}},
                                        new InetSocketAddress[] {addr, addr}, conf);
    assertEquals(voids, null);
    } finally {
      server.stop();
      if(proxy!=null) RPC.stopProxy(proxy);
    }
  }
  
  public void testStandaloneClient() throws IOException {
    try {
      RPC.waitForProxy(TestProtocol.class,
        TestProtocol.versionID, new InetSocketAddress(ADDRESS, 20), conf, 15000L);
      fail("We should not have reached here");
    } catch (ConnectException ioe) {
      //this is what we expected
    }
  }
  
  private static final String ACL_CONFIG = "test.protocol.acl";
  
  private static class TestPolicyProvider extends PolicyProvider {

    @Override
    public Service[] getServices() {
      return new Service[] { new Service(ACL_CONFIG, TestProtocol.class) };
    }
    
  }
  
  private void doRPCs(Configuration conf, boolean expectFailure) throws Exception {
    Server server = RPC.getServer(new TestImpl(), ADDRESS, 0, 5, true, conf);

    server.refreshServiceAcl(conf, new TestPolicyProvider());

    TestProtocol proxy = null;

    server.start();

    InetSocketAddress addr = NetUtils.getConnectAddress(server);
    
    try {
      proxy = (TestProtocol)RPC.getProxy(
          TestProtocol.class, TestProtocol.versionID, addr, conf);
      proxy.ping();

      if (expectFailure) {
        fail("Expect RPC.getProxy to fail with AuthorizationException!");
      }
    } catch (RemoteException e) {
      if (expectFailure) {
        assertTrue(e.unwrapRemoteException() instanceof AuthorizationException);
      } else {
        throw e;
      }
    } finally {
      server.stop();
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
      if (expectFailure) {
        assertEquals("Wrong number of authorizationFailures ", 1,  
            server.getRpcMetrics().authorizationFailures
            .getCurrentIntervalValue());
      } else {
        assertEquals("Wrong number of authorizationSuccesses ", 1, 
            server.getRpcMetrics().authorizationSuccesses
            .getCurrentIntervalValue());
      }
      //since we don't have authentication turned ON, we should see 
      // 0 for the authentication successes and 0 for failure
      assertEquals("Wrong number of authenticationFailures ", 0, 
          server.getRpcMetrics().authenticationFailures
          .getCurrentIntervalValue());
      assertEquals("Wrong number of authenticationSuccesses ", 0, 
          server.getRpcMetrics().authenticationSuccesses
          .getCurrentIntervalValue());
    }
  }
  
  public void testAuthorization() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION,
        true);
    
    // Expect to succeed
    conf.set(ACL_CONFIG, "*");
    doRPCs(conf, false);
    
    // Reset authorization to expect failure
    conf.set(ACL_CONFIG, "invalid invalid");
    doRPCs(conf, true);
  }

  public void testRPCInterrupted3() throws IOException, InterruptedException {
    final Configuration conf = new Configuration();
    Server server = RPC.getServer(
      new TestImpl(), ADDRESS, 0, 5, true, conf);

    server.start();

    try {

    int numConcurrentRPC = 200;
    InetSocketAddress addr = NetUtils.getConnectAddress(server);
    final CyclicBarrier barrier = new CyclicBarrier(numConcurrentRPC);
    final CountDownLatch latch = new CountDownLatch(numConcurrentRPC);
    final AtomicBoolean leaderRunning = new AtomicBoolean(true);
    final AtomicReference<Throwable> error = new AtomicReference<Throwable>(null);
    Thread leaderThread = null;
    
    for (int i = 0; i < numConcurrentRPC; i++) {
      final int num = i;
      final TestProtocol proxy = (TestProtocol) RPC.getProxy(
      TestProtocol.class, TestProtocol.versionID, addr, conf);
      Thread rpcThread = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            barrier.await();
            while (num == 0 || leaderRunning.get()) {
              proxy.slowPing(false);
            }
            
            proxy.slowPing(false);
          } catch (Exception e) {
            if (num == 0) {
              leaderRunning.set(false);
            } else {
              error.set(e);
            }

            LOG.error(e);
          } finally {
            latch.countDown();
          }
        }
      });
      rpcThread.start();

      if (leaderThread == null) {
       leaderThread = rpcThread;
      }
    }
    // let threads get past the barrier
    Thread.sleep(1000);

    // stop a single thread
    while (leaderRunning.get()) {
      leaderThread.interrupt();
    }

    latch.await();
    
    // should not cause any other thread to get an error
    assertNull("rpc got ClosedChannelException", error.get());

    } finally {
      server.stop();
    }
  }

  public void testNoPings() throws Exception {
    Configuration conf = new Configuration();
   
    conf.setBoolean("ipc.client.ping", false);
    new TestRPC("testnoPings").testCalls(conf);
   
    conf.setInt(CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_KEY, 2);
    new TestRPC("testnoPings").testCalls(conf);
  }

  /**
   * Count the number of threads that have a stack frame containing
   * the given string
   */
  private static int countThreads(String search) {
    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

    int count = 0;
    ThreadInfo[] infos = threadBean.getThreadInfo(threadBean.getAllThreadIds(), 20);
    for (ThreadInfo info : infos) {
      if (info == null) continue;
      for (StackTraceElement elem : info.getStackTrace()) {
        if (elem.getClassName().contains(search)) {
          count++;
          break;
        }
      }
    }
    return count;
  }


  /**
   * Test that server.stop() properly stops all threads
   */
  public void testStopsAllThreads() throws Exception {
    int threadsBefore = countThreads("Server$Listener$Reader");
    assertEquals("Expect no Reader threads running before test",
      0, threadsBefore);

    final Server server = RPC.getServer(new TestImpl(), ADDRESS, 0, 2, false, conf);
    server.start();
    try {
      int threadsRunning = countThreads("Server$Listener$Reader");
      assertTrue(threadsRunning > 0);
    } finally {
      server.stop();
    }
    int threadsAfter = countThreads("Server$Listener$Reader");
    assertEquals("Expect no Reader threads left running after test",
      0, threadsAfter);
  }
  
  public void testErrorMsgForInsecureClient() throws Exception {
    final Server server = RPC.getServer(
        new TestImpl(), ADDRESS, 0, 5, true, conf, null);
    server.enableSecurity();
    server.start();
    boolean succeeded = false;
    final InetSocketAddress addr = NetUtils.getConnectAddress(server);
    TestProtocol proxy = null;
    try {
      proxy = (TestProtocol) RPC.getProxy(TestProtocol.class,
          TestProtocol.versionID, addr, conf);
    } catch (RemoteException e) {
      LOG.info("LOGGING MESSAGE: " + e.getLocalizedMessage());
      assertTrue(e.unwrapRemoteException() instanceof AccessControlException);
      succeeded = true;
    } finally {
      server.stop();
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
    }
    assertTrue(succeeded);

    conf.setInt(CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_KEY, 2);

    final Server multiServer = RPC.getServer(new TestImpl(), 
						ADDRESS, 0, 5, true, conf, null);
    multiServer.enableSecurity();
    multiServer.start();
    succeeded = false;
    final InetSocketAddress mulitServerAddr =
                      NetUtils.getConnectAddress(multiServer);
    proxy = null;
    try {
      proxy = (TestProtocol) RPC.getProxy(TestProtocol.class,
          TestProtocol.versionID, mulitServerAddr, conf);
    } catch (RemoteException e) {
      LOG.info("LOGGING MESSAGE: " + e.getLocalizedMessage());
      assertTrue(e.unwrapRemoteException() instanceof AccessControlException);
      succeeded = true;
    } finally {
      multiServer.stop();
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
    }
    assertTrue(succeeded);
  }
 
  public static void main(String[] args) throws Exception {

    new TestRPC("test").testCalls(conf);

  }
}
