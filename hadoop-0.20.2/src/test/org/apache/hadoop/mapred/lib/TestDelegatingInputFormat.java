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
package org.apache.hadoop.mapred.lib;

import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

public class TestDelegatingInputFormat extends TestCase {

  public void testSplitting() throws Exception {
    JobConf conf = new JobConf();
    conf.set("fs.hdfs.impl",
       "org.apache.hadoop.hdfs.ChecksumDistributedFileSystem");
    MiniDFSCluster dfs = null;
    try {
      dfs = new MiniDFSCluster(conf, 4, true, new String[] { "/rack0",
         "/rack0", "/rack1", "/rack1" }, new String[] { "host0", "host1",
         "host2", "host3" });
      FileSystem fs = dfs.getFileSystem();

      Path path = getPath("/foo/bar", fs);
      Path path2 = getPath("/foo/baz", fs);
      Path path3 = getPath("/bar/bar", fs);
      Path path4 = getPath("/bar/baz", fs);

      final int numSplits = 100;

      MultipleInputs.addInputPath(conf, path, TextInputFormat.class,
         MapClass.class);
      MultipleInputs.addInputPath(conf, path2, TextInputFormat.class,
         MapClass2.class);
      MultipleInputs.addInputPath(conf, path3, KeyValueTextInputFormat.class,
         MapClass.class);
      MultipleInputs.addInputPath(conf, path4, TextInputFormat.class,
         MapClass2.class);
      DelegatingInputFormat inFormat = new DelegatingInputFormat();
      InputSplit[] splits = inFormat.getSplits(conf, numSplits);

      int[] bins = new int[3];
      for (InputSplit split : splits) {
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

      // Each bin is a unique combination of a Mapper and InputFormat, and
      // DelegatingInputFormat should split each bin into numSplits splits,
      // regardless of the number of paths that use that Mapper/InputFormat
      for (int count : bins) {
       assertEquals(numSplits, count);
      }

      assertTrue(true);
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

  static class MapClass implements Mapper<String, String, String, String> {

    public void map(String key, String value,
       OutputCollector<String, String> output, Reporter reporter)
       throws IOException {
    }

    public void configure(JobConf job) {
    }

    public void close() throws IOException {
    }
  }

  static class MapClass2 extends MapClass {
  }

}
