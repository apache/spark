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

import junit.framework.TestCase;
import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

/** Test NameNode port defaulting code. */
public class TestDefaultNameNodePort extends TestCase {

  public void testGetAddressFromString() throws Exception {
    assertEquals(NameNode.getAddress("foo").getPort(),
                 NameNode.DEFAULT_PORT);
    assertEquals(NameNode.getAddress("hdfs://foo/").getPort(),
                 NameNode.DEFAULT_PORT);
    assertEquals(NameNode.getAddress("hdfs://foo:555").getPort(),
                 555);
    assertEquals(NameNode.getAddress("foo:555").getPort(),
                 555);
  }

  public void testGetAddressFromConf() throws Exception {
    Configuration conf = new Configuration();
    FileSystem.setDefaultUri(conf, "hdfs://foo/");
    assertEquals(NameNode.getAddress(conf).getPort(), NameNode.DEFAULT_PORT);
    FileSystem.setDefaultUri(conf, "hdfs://foo:555/");
    assertEquals(NameNode.getAddress(conf).getPort(), 555);
    FileSystem.setDefaultUri(conf, "foo");
    assertEquals(NameNode.getAddress(conf).getPort(), NameNode.DEFAULT_PORT);
  }

  public void testGetUri() {
    assertEquals(NameNode.getUri(new InetSocketAddress("foo", 555)),
                 URI.create("hdfs://foo:555"));
    assertEquals(NameNode.getUri(new InetSocketAddress("foo",
                                                       NameNode.DEFAULT_PORT)),
                 URI.create("hdfs://foo"));
  }
}
