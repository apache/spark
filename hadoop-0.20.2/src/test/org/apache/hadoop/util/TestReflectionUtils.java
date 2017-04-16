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

import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;

import junit.framework.TestCase;

public class TestReflectionUtils extends TestCase {

  private static Class toConstruct[] = { String.class, TestReflectionUtils.class, HashMap.class };
  private Throwable failure = null;

  public void setUp() {
    ReflectionUtils.clearCache();
  }
    
  public void testCache() throws Exception {
    assertEquals(0, cacheSize());
    doTestCache();
    assertEquals(toConstruct.length, cacheSize());
    ReflectionUtils.clearCache();
    assertEquals(0, cacheSize());
  }
    
    
  @SuppressWarnings("unchecked")
  private void doTestCache() {
    for (int i=0; i<toConstruct.length; i++) {
      Class cl = toConstruct[i];
      Object x = ReflectionUtils.newInstance(cl, null);
      Object y = ReflectionUtils.newInstance(cl, null);
      assertEquals(cl, x.getClass());
      assertEquals(cl, y.getClass());
    }
  }
    
  public void testThreadSafe() throws Exception {
    Thread[] th = new Thread[32];
    for (int i=0; i<th.length; i++) {
      th[i] = new Thread() {
          public void run() {
            try {
              doTestCache();
            } catch (Throwable t) {
              failure = t;
            }
          }
        };
      th[i].start();
    }
    for (int i=0; i<th.length; i++) {
      th[i].join();
    }
    if (failure != null) {
      failure.printStackTrace();
      fail(failure.getMessage());
    }
  }
    
  private int cacheSize() throws Exception {
    return ReflectionUtils.getCacheSize();
  }
    
  public void testCantCreate() {
    try {
      ReflectionUtils.newInstance(NoDefaultCtor.class, null);
      fail("invalid call should fail");
    } catch (RuntimeException rte) {
      assertEquals(NoSuchMethodException.class, rte.getCause().getClass());
    }
  }
    
  @SuppressWarnings("unchecked")
  public void testCacheDoesntLeak() throws Exception {
    int iterations=9999; // very fast, but a bit less reliable - bigger numbers force GC
    for (int i=0; i<iterations; i++) {
      URLClassLoader loader = new URLClassLoader(new URL[0], getClass().getClassLoader());
      Class cl = Class.forName("org.apache.hadoop.util.TestReflectionUtils$LoadedInChild", false, loader);
      Object o = ReflectionUtils.newInstance(cl, null);
      assertEquals(cl, o.getClass());
    }
    System.gc();
    assertTrue(cacheSize()+" too big", cacheSize()<iterations);
  }
    
  private static class LoadedInChild {
  }
    
  public static class NoDefaultCtor {
    public NoDefaultCtor(int x) {}
  }
  
  /**
   * This is to test backward compatibility of ReflectionUtils for 
   * JobConfigurable objects. 
   * This should be made deprecated along with the mapred package HADOOP-1230. 
   * Should be removed when mapred package is removed.
   */
  public void testSetConf() {
    JobConfigurableOb ob = new JobConfigurableOb();
    ReflectionUtils.setConf(ob, new Configuration());
    assertFalse(ob.configured);
    ReflectionUtils.setConf(ob, new JobConf());
    assertTrue(ob.configured);
  }
  
  private static class JobConfigurableOb implements JobConfigurable {
    boolean configured;
    public void configure(JobConf job) {
      configured = true;
    }
  }
}
