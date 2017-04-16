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
package org.apache.hadoop.mapreduce.lib.input;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

public class TestDelegatingInputFormat extends TestCase {

  @SuppressWarnings("unchecked")
  public void testSplitting() throws Exception {
    Job job = new Job();
    MiniDFSCluster dfs = null;
    try {
      dfs = new MiniDFSCluster(job.getConfiguration(), 4, true, new String[] { "/rack0",
         "/rack0", "/rack1", "/rack1" }, new String[] { "host0", "host1",
         "host2", "host3" });
      FileSystem fs = dfs.getFileSystem();

      Path path = getPath("/foo/bar", fs);
      Path path2 = getPath("/foo/baz", fs);
      Path path3 = getPath("/bar/bar", fs);
      Path path4 = getPath("/bar/baz", fs);

      final int numSplits = 100;

      FileInputFormat.setMaxInputSplitSize(job, 
              fs.getFileStatus(path).getLen() / numSplits);
      MultipleInputs.addInputPath(job, path, TextInputFormat.class,
         MapClass.class);
      MultipleInputs.addInputPath(job, path2, TextInputFormat.class,
         MapClass2.class);
      MultipleInputs.addInputPath(job, path3, KeyValueTextInputFormat.class,
         MapClass.class);
      MultipleInputs.addInputPath(job, path4, TextInputFormat.class,
         MapClass2.class);
      DelegatingInputFormat inFormat = new DelegatingInputFormat();

      int[] bins = new int[3];
      for (InputSplit split : (List<InputSplit>)inFormat.getSplits(job)) {
       assertTrue(split instanceof TaggedInputSplit);
       final TaggedInputSplit tis = (TaggedInputSplit) split;
       int index = -1;

       if (tis.getInputFormatClass().equals(KeyValueTextInputFormat.class)) {
         // path3
         index = 0;
       } else if (tis.getMapperClass().equals(MapClass.class)) {
         // path
         index = 1;
       } else {
         // path2 and path4
         index = 2;
       }

       bins[index]++;
      }

      assertEquals("count is not equal to num splits", numSplits, bins[0]);
      assertEquals("count is not equal to num splits", numSplits, bins[1]);
      assertEquals("count is not equal to 2 * num splits",
        numSplits * 2, bins[2]);
    } finally {
      if (dfs != null) {
       dfs.shutdown();
      }
    }
  }

  static Path getPath(final String location, final FileSystem fs)
      throws IOException {
    Path path = new Path(location);

    // create a multi-block file on hdfs
    DataOutputStream out = fs.create(path, true, 4096, (short) 2, 512, null);
    for (int i = 0; i < 1000; ++i) {
      out.writeChars("Hello\n");
    }
    out.close();

    return path;
  }

  static class MapClass extends Mapper<String, String, String, String> {
  }

  static class MapClass2 extends MapClass {
  }

}
