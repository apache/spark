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
package org.apache.hadoop.mapred;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer;
import org.junit.Test;


public class TestJettyBugMonitor {
  private final Configuration conf = new Configuration();
  
  /**
   * Test that it can detect a running Jetty selector.
   */
  @Test(timeout=20000)
  public void testGetJettyThreads() throws Exception {
    JettyBugMonitor monitor = new JettyBugMonitor(conf);
    
    new File(System.getProperty("build.webapps", "build/webapps") + "/test"
      ).mkdirs();
    HttpServer server = new HttpServer("test", "0.0.0.0", 0, true);
    server.start();
    try {
      List<Long> threads = monitor.waitForJettyThreads();
      assertEquals(1, threads.size());
    } finally {
      server.stop();
    }
  }
  
  /**
   * Test that the CPU monitoring can detect a spinning
   * thread.
   */
  @Test(timeout=5000)
  public void testMonitoring() throws Exception {
    // Start a thread which sucks up CPU
    BusyThread busyThread = new BusyThread();
    busyThread.start();
    final long tid = busyThread.getId();
    // Latch which will be triggered when the jetty monitor
    // wants to abort
    final CountDownLatch abortLatch = new CountDownLatch(1);

    conf.setLong(JettyBugMonitor.CHECK_INTERVAL_KEY, 1000);
    JettyBugMonitor monitor = null;
    try {
      monitor = new JettyBugMonitor(conf) {
        @Override
        protected List<Long> waitForJettyThreads() {
          return Collections.<Long>singletonList(tid);
        }
        @Override
        protected void doAbort() {
          abortLatch.countDown();
          // signal abort to main thread
        }
      };
      monitor.start();
      
      abortLatch.await();
    } finally {
      busyThread.done = true;
      busyThread.join();
      
      if (monitor != null) {
        monitor.shutdown();
      }
    }
  }
  
  private static class BusyThread extends Thread {
    private volatile boolean done = false;
    
    @Override
    public void run() {
      while (!done) {
        // spin using up CPU
      }
    }
  }
}
