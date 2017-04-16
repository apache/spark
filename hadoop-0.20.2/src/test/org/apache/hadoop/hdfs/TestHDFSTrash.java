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
package org.apache.hadoop.hdfs;

import java.io.IOException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.extensions.TestSetup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.TestTrash;

/**
 * This class tests commands from Trash.
 */
public class TestHDFSTrash extends TestTrash {

  private static MiniDFSCluster cluster = null;
  public static Test suite() {
    TestSetup setup = new TestSetup(new TestSuite(TestHDFSTrash.class)) {
      protected void setUp() throws Exception {
        Configuration conf = new Configuration();
        cluster = new MiniDFSCluster(conf, 2, true, null);
      }
      protected void tearDown() throws Exception {
        if (cluster != null) { cluster.shutdown(); }
      }
    };
    return setup;
  }

  /**
   * Tests Trash on HDFS
   */
  public void testTrash() throws IOException {
    trashShell(cluster.getFileSystem(), new Path("/"));
  }

  public void testNonDefaultFS() throws IOException {
    FileSystem fs = cluster.getFileSystem();
    Configuration conf = fs.getConf();
    conf.set("fs.default.name", fs.getUri().toString());
    trashNonDefaultFS(conf);
  }

}
