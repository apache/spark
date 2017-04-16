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

package org.apache.hadoop.mapreduce.lib.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.util.ReflectionUtils;

import junit.framework.TestCase;

public class TestBinaryPartitioner extends TestCase {

  public void testDefaultOffsets() {
    Configuration conf = new Configuration();
    BinaryPartitioner<?> partitioner = 
      ReflectionUtils.newInstance(BinaryPartitioner.class, conf);
    
    BinaryComparable key1 = new BytesWritable(new byte[] { 1, 2, 3, 4, 5 }); 
    BinaryComparable key2 = new BytesWritable(new byte[] { 1, 2, 3, 4, 5 });
    int partition1 = partitioner.getPartition(key1, null, 10);
    int partition2 = partitioner.getPartition(key2, null, 10);
    assertEquals(partition1, partition2);
    
    key1 = new BytesWritable(new byte[] { 1, 2, 3, 4, 5 }); 
    key2 = new BytesWritable(new byte[] { 6, 2, 3, 4, 5 });
    partition1 = partitioner.getPartition(key1, null, 10);
    partition2 = partitioner.getPartition(key2, null, 10);
    assertTrue(partition1 != partition2);
    
    key1 = new BytesWritable(new byte[] { 1, 2, 3, 4, 5 }); 
    key2 = new BytesWritable(new byte[] { 1, 2, 3, 4, 6 });
    partition1 = partitioner.getPartition(key1, null, 10);
    partition2 = partitioner.getPartition(key2, null, 10);
    assertTrue(partition1 != partition2);
  }
  
  public void testCustomOffsets() {
    Configuration conf = new Configuration();
    BinaryComparable key1 = new BytesWritable(new byte[] { 1, 2, 3, 4, 5 }); 
    BinaryComparable key2 = new BytesWritable(new byte[] { 6, 2, 3, 7, 8 });
    
    BinaryPartitioner.setOffsets(conf, 1, -3);
    BinaryPartitioner<?> partitioner = 
      ReflectionUtils.newInstance(BinaryPartitioner.class, conf);
    int partition1 = partitioner.getPartition(key1, null, 10);
    int partition2 = partitioner.getPartition(key2, null, 10);
    assertEquals(partition1, partition2);
    
    BinaryPartitioner.setOffsets(conf, 1, 2);
    partitioner = ReflectionUtils.newInstance(BinaryPartitioner.class, conf);
    partition1 = partitioner.getPartition(key1, null, 10);
    partition2 = partitioner.getPartition(key2, null, 10);
    assertEquals(partition1, partition2);
    
    BinaryPartitioner.setOffsets(conf, -4, -3);
    partitioner = ReflectionUtils.newInstance(BinaryPartitioner.class, conf);
    partition1 = partitioner.getPartition(key1, null, 10);
    partition2 = partitioner.getPartition(key2, null, 10);
    assertEquals(partition1, partition2);
  }
  
  public void testLowerBound() {
    Configuration conf = new Configuration();
    BinaryPartitioner.setLeftOffset(conf, 0);
    BinaryPartitioner<?> partitioner = 
      ReflectionUtils.newInstance(BinaryPartitioner.class, conf);
    BinaryComparable key1 = new BytesWritable(new byte[] { 1, 2, 3, 4, 5 }); 
    BinaryComparable key2 = new BytesWritable(new byte[] { 6, 2, 3, 4, 5 });
    int partition1 = partitioner.getPartition(key1, null, 10);
    int partition2 = partitioner.getPartition(key2, null, 10);
    assertTrue(partition1 != partition2);
  }
  
  public void testUpperBound() {
    Configuration conf = new Configuration();
    BinaryPartitioner.setRightOffset(conf, 4);
    BinaryPartitioner<?> partitioner = 
      ReflectionUtils.newInstance(BinaryPartitioner.class, conf);
    BinaryComparable key1 = new BytesWritable(new byte[] { 1, 2, 3, 4, 5 }); 
    BinaryComparable key2 = new BytesWritable(new byte[] { 1, 2, 3, 4, 6 });
    int partition1 = partitioner.getPartition(key1, null, 10);
    int partition2 = partitioner.getPartition(key2, null, 10);
    assertTrue(partition1 != partition2);
  }
  
}
