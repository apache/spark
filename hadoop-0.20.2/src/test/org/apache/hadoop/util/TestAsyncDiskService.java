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
package org.apache.hadoop.util;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.AsyncDiskService;

/**
 * A test for AsyncDiskService.
 */
public class TestAsyncDiskService extends TestCase {
  
  public static final Log LOG = LogFactory.getLog(TestAsyncDiskService.class);
  
  // Access by multiple threads from the ThreadPools in AsyncDiskService.
  volatile int count;
  
  /** An example task for incrementing a counter.  
   */
  class ExampleTask implements Runnable {

    ExampleTask() {
    }
    
    @Override
    public void run() {
      synchronized (TestAsyncDiskService.this) {
        count ++;
      }
    }
  };
  
  
  /**
   * This test creates some ExampleTasks and runs them. 
   */
  public void testAsyncDiskService() throws Throwable {
  
    String[] vols = new String[]{"/0", "/1"};
    AsyncDiskService service = new AsyncDiskService(vols);
    
    int total = 100;
    
    for (int i = 0; i < total; i++) {
      service.execute(vols[i%2], new ExampleTask());
    }

    Exception e = null;
    try {
      service.execute("no_such_volume", new ExampleTask());
    } catch (RuntimeException ex) {
      e = ex;
    }
    assertNotNull("Executing a task on a non-existing volume should throw an "
        + "Exception.", e);
    
    service.shutdown();
    if (!service.awaitTermination(5000)) {
      fail("AsyncDiskService didn't shutdown in 5 seconds.");
    }
    
    assertEquals(total, count);
  }
}
