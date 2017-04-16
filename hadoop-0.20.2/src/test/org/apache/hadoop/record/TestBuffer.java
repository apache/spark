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

package org.apache.hadoop.record;

import junit.framework.*;

/**
 * A Unit test for Record I/O Buffer class
 */
public class TestBuffer extends TestCase {
  
  public TestBuffer(String testName) {
    super(testName);
  }
  
  /**
   * Test of set method, of class org.apache.hadoop.record.Buffer.
   */
  public void testSet() {
    final byte[] bytes = new byte[10];
    final Buffer instance = new Buffer();
    
    instance.set(bytes);
    
    assertEquals("set failed", bytes, instance.get());
  }
  
  /**
   * Test of copy method, of class org.apache.hadoop.record.Buffer.
   */
  public void testCopy() {
    final byte[] bytes = new byte[10];
    final int offset = 6;
    final int length = 3;
    for (int idx = 0; idx < 10; idx ++) {
      bytes[idx] = (byte) idx;
    }
    final Buffer instance = new Buffer();
    
    instance.copy(bytes, offset, length);
    
    assertEquals("copy failed", 3, instance.getCapacity());
    assertEquals("copy failed", 3, instance.get().length);
    for (int idx = 0; idx < 3; idx++) {
      assertEquals("Buffer content corrupted", idx+6, instance.get()[idx]);
    }
  }
  
  /**
   * Test of getCount method, of class org.apache.hadoop.record.Buffer.
   */
  public void testGetCount() {
    final Buffer instance = new Buffer();
    
    final int expResult = 0;
    final int result = instance.getCount();
    assertEquals("getSize failed", expResult, result);
  }
  
  /**
   * Test of getCapacity method, of class org.apache.hadoop.record.Buffer.
   */
  public void testGetCapacity() {
    final Buffer instance = new Buffer();
    
    final int expResult = 0;
    final int result = instance.getCapacity();
    assertEquals("getCapacity failed", expResult, result);
    
    instance.setCapacity(100);
    assertEquals("setCapacity failed", 100, instance.getCapacity());
  }
  
  /**
   * Test of truncate method, of class org.apache.hadoop.record.Buffer.
   */
  public void testTruncate() {
    final Buffer instance = new Buffer();
    instance.setCapacity(100);
    assertEquals("setCapacity failed", 100, instance.getCapacity());
    
    instance.truncate();
    assertEquals("truncate failed", 0, instance.getCapacity());
  }
  
  /**
   * Test of append method, of class org.apache.hadoop.record.Buffer.
   */
  public void testAppend() {
    final byte[] bytes = new byte[100];
    final int offset = 0;
    final int length = 100;
    for (int idx = 0; idx < 100; idx++) {
      bytes[idx] = (byte) (100-idx);
    }
    
    final Buffer instance = new Buffer();
    
    instance.append(bytes, offset, length);
    
    assertEquals("Buffer size mismatch", 100, instance.getCount());
    
    for (int idx = 0; idx < 100; idx++) {
      assertEquals("Buffer contents corrupted", 100-idx, instance.get()[idx]);
    }
    
  }
}
