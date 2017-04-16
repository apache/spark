/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.io;

import java.util.Map;

import junit.framework.TestCase;

/**
 * Tests SortedMapWritable
 */
public class TestSortedMapWritable extends TestCase {
  /** the test */
  @SuppressWarnings("unchecked")
  public void testSortedMapWritable() {
    Text[] keys = {
        new Text("key1"),
        new Text("key2"),
        new Text("key3"),
    };
    
    BytesWritable[] values = {
        new BytesWritable("value1".getBytes()),
        new BytesWritable("value2".getBytes()),
        new BytesWritable("value3".getBytes())
    };

    SortedMapWritable inMap = new SortedMapWritable();
    for (int i = 0; i < keys.length; i++) {
      inMap.put(keys[i], values[i]);
    }
    
    assertEquals(0, inMap.firstKey().compareTo(keys[0]));
    assertEquals(0, inMap.lastKey().compareTo(keys[2]));

    SortedMapWritable outMap = new SortedMapWritable(inMap);
    assertEquals(inMap.size(), outMap.size());
    
    for (Map.Entry<WritableComparable, Writable> e: inMap.entrySet()) {
      assertTrue(outMap.containsKey(e.getKey()));
      assertEquals(0, ((WritableComparable) outMap.get(e.getKey())).compareTo(
          e.getValue()));
    }
    
    // Now for something a little harder...
    
    Text[] maps = {
        new Text("map1"),
        new Text("map2")
    };
    
    SortedMapWritable mapOfMaps = new SortedMapWritable();
    mapOfMaps.put(maps[0], inMap);
    mapOfMaps.put(maps[1], outMap);
    
    SortedMapWritable copyOfMapOfMaps = new SortedMapWritable(mapOfMaps);
    for (int i = 0; i < maps.length; i++) {
      assertTrue(copyOfMapOfMaps.containsKey(maps[i]));

      SortedMapWritable a = (SortedMapWritable) mapOfMaps.get(maps[i]);
      SortedMapWritable b = (SortedMapWritable) copyOfMapOfMaps.get(maps[i]);
      assertEquals(a.size(), b.size());
      for (Writable key: a.keySet()) {
        assertTrue(b.containsKey(key));
        
        // This will work because we know what we put into each set
        
        WritableComparable aValue = (WritableComparable) a.get(key);
        WritableComparable bValue = (WritableComparable) b.get(key);
        assertEquals(0, aValue.compareTo(bValue));
      }
    }
  }
  
  /**
   * Test that number of "unknown" classes is propagated across multiple copies.
   */
  @SuppressWarnings("deprecation")
  public void testForeignClass() {
    SortedMapWritable inMap = new SortedMapWritable();
    inMap.put(new Text("key"), new UTF8("value"));
    inMap.put(new Text("key2"), new UTF8("value2"));
    SortedMapWritable outMap = new SortedMapWritable(inMap);
    SortedMapWritable copyOfCopy = new SortedMapWritable(outMap);
    assertEquals(1, copyOfCopy.getNewClasses());
  }
}
