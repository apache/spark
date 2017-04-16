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

import java.util.Iterator;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

public class TestLRUCache extends TestCase {
  private static final Log LOG = 
    LogFactory.getLog(TestLRUCache.class);
  
  public void testPut() {
    TaskTracker.LRUCache<String, Path> cache = new TaskTracker.LRUCache<String, Path>(200);
  
    for(int i=0;i<200;i++) {
       cache.put(i+"", new Path("/foo"+i));
    }
    
    Iterator<Map.Entry<String, Path>> iterator = cache.getIterator();
    int i=0;
    while(iterator.hasNext()) {
      Map.Entry<String, Path> entry = iterator.next();
      String key = entry.getKey();
      Path val = entry.getValue();
      assertEquals(i+"", key);
      i++;
    }
    LOG.info("Completed testPut");
  }
  
  
  public void testGet() {
    TaskTracker.LRUCache<String, Path> cache = new TaskTracker.LRUCache<String, Path>(200);
  
    for(int i=0;i<200;i++) {
      cache.put(i+"", new Path("/foo"+i));
    }
    
    for(int i=0;i<200;i++) {
      Path path = cache.get(i+"");
      assertEquals(path.toString(), (new Path("/foo"+i)).toString());
    }
    LOG.info("Completed testGet");
  }
  
  
  /**
   * Test if cache can be cleared properly
   */
  public void testClear() {
    TaskTracker.LRUCache<String, Path> cache = new TaskTracker.LRUCache<String, Path>(200);
  
    for(int i=0;i<200;i++) {
      cache.put(i+"", new Path("/foo"+i));
    }
    
    cache.clear();
    assertTrue(cache.size() == 0);
    LOG.info("Completed testClear");
  }
  
  /**
   * Test for cache overflow condition
   */
  public void testOverFlow() {
    TaskTracker.LRUCache<String, Path> cache = new TaskTracker.LRUCache<String, Path>(200);
  
    int SIZE = 5000;
  
    for(int i=0;i<SIZE;i++) {
      cache.put(i+"", new Path("/foo"+i));
    }
    
    //Check if the items are removed properly when the cache size is exceeded
    for(int i=SIZE-1;i>=SIZE-200;i--) {
      Path path = cache.get(i+"");
      assertEquals(path.toString(), (new Path("/foo"+i)).toString());
    }
    
    LOG.info("Completed testOverFlow");
  }
}