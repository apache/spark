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

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.SortedRanges.Range;

public class TestSortedRanges extends TestCase {
  private static final Log LOG = 
    LogFactory.getLog(TestSortedRanges.class);
  
  public void testAdd() {
    SortedRanges sr = new SortedRanges();
    sr.add(new Range(2,9));
    assertEquals(9, sr.getIndicesCount());
    
    sr.add(new SortedRanges.Range(3,5));
    assertEquals(9, sr.getIndicesCount());
    
    sr.add(new SortedRanges.Range(7,1));
    assertEquals(9, sr.getIndicesCount());
    
    sr.add(new Range(1,12));
    assertEquals(12, sr.getIndicesCount());
    
    sr.add(new Range(7,9));
    assertEquals(15, sr.getIndicesCount());
    
    sr.add(new Range(31,10));
    sr.add(new Range(51,10));
    sr.add(new Range(66,10));
    assertEquals(45, sr.getIndicesCount());
    
    sr.add(new Range(21,50));
    assertEquals(70, sr.getIndicesCount());
    
    LOG.debug(sr);
    
    Iterator<Long> it = sr.skipRangeIterator();
    int i = 0;
    assertEquals(i, it.next().longValue());
    for(i=16;i<21;i++) {
      assertEquals(i, it.next().longValue());
    }
    assertEquals(76, it.next().longValue());
    assertEquals(77, it.next().longValue());
    
  }
  
  public void testRemove() {
    SortedRanges sr = new SortedRanges();
    sr.add(new Range(2,19));
    assertEquals(19, sr.getIndicesCount());
    
    sr.remove(new SortedRanges.Range(15,8));
    assertEquals(13, sr.getIndicesCount());
    
    sr.remove(new SortedRanges.Range(6,5));
    assertEquals(8, sr.getIndicesCount());
    
    sr.remove(new SortedRanges.Range(8,4));
    assertEquals(7, sr.getIndicesCount());
    
    sr.add(new Range(18,5));
    assertEquals(12, sr.getIndicesCount());
    
    sr.add(new Range(25,1));
    assertEquals(13, sr.getIndicesCount());
    
    sr.remove(new SortedRanges.Range(7,24));
    assertEquals(4, sr.getIndicesCount());
    
    sr.remove(new SortedRanges.Range(5,1));
    assertEquals(3, sr.getIndicesCount());
    
    LOG.debug(sr);
  }

}
