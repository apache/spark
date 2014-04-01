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

package org.apache.hadoop.streaming;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.typedbytes.TypedBytesOutput;
import org.apache.hadoop.typedbytes.TypedBytesWritable;

import junit.framework.TestCase;

public class TestLoadTypedBytes extends TestCase {

  public void testLoading() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    FileSystem fs = cluster.getFileSystem();
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    TypedBytesOutput tboutput = new TypedBytesOutput(new DataOutputStream(out));
    for (int i = 0; i < 100; i++) {
      tboutput.write(new Long(i)); // key
      tboutput.write("" + (10 * i)); // value
    }
    InputStream isBackup = System.in;
    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    System.setIn(in);
    LoadTypedBytes loadtb = new LoadTypedBytes(conf);

    try {
      Path root = new Path("/typedbytestest");
      assertTrue(fs.mkdirs(root));
      assertTrue(fs.exists(root));
      
      String[] args = new String[1];
      args[0] = "/typedbytestest/test.seq";
      int ret = loadtb.run(args);
      assertEquals("Return value != 0.", 0, ret);

      Path file = new Path(root, "test.seq");
      assertTrue(fs.exists(file));
      SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);
      int counter = 0;
      TypedBytesWritable key = new TypedBytesWritable();
      TypedBytesWritable value = new TypedBytesWritable();
      while (reader.next(key, value)) {
        assertEquals(Long.class, key.getValue().getClass());
        assertEquals(String.class, value.getValue().getClass());
        assertTrue("Invalid record.",
          Integer.parseInt(value.toString()) % 10 == 0);
        counter++;
      }
      assertEquals("Wrong number of records.", 100, counter);
    } finally {
      try {
        fs.close();
      } catch (Exception e) {
      }
      System.setIn(isBackup);
      cluster.shutdown();
    }
  }
  
}
