/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.io.file.tfile;

import java.io.IOException;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.file.tfile.TFile.Writer;

/**
 * 
 * Byte arrays test case class using GZ compression codec, base class of none
 * and LZO compression classes.
 * 
 */
public class TestTFileComparators extends TestCase {
  private static String ROOT =
      System.getProperty("test.build.data", "/tmp/tfile-test");

  private final static int BLOCK_SIZE = 512;
  private FileSystem fs;
  private Configuration conf;
  private Path path;
  private FSDataOutputStream out;
  private Writer writer;

  private String compression = Compression.Algorithm.GZ.getName();
  private String outputFile = "TFileTestComparators";
  /*
   * pre-sampled numbers of records in one block, based on the given the
   * generated key and value strings
   */
  // private int records1stBlock = 4314;
  // private int records2ndBlock = 4108;
  private int records1stBlock = 4480;
  private int records2ndBlock = 4263;

  @Override
  public void setUp() throws IOException {
    conf = new Configuration();
    path = new Path(ROOT, outputFile);
    fs = path.getFileSystem(conf);
    out = fs.create(path);
  }

  @Override
  public void tearDown() throws IOException {
    fs.delete(path, true);
  }

  // bad comparator format
  public void testFailureBadComparatorNames() throws IOException {
    try {
      writer = new Writer(out, BLOCK_SIZE, compression, "badcmp", conf);
      Assert.fail("Failed to catch unsupported comparator names");
    }
    catch (Exception e) {
      // noop, expecting exceptions
      e.printStackTrace();
    }
  }

  // jclass that doesn't exist
  public void testFailureBadJClassNames() throws IOException {
    try {
      writer =
          new Writer(out, BLOCK_SIZE, compression,
              "jclass: some.non.existence.clazz", conf);
      Assert.fail("Failed to catch unsupported comparator names");
    }
    catch (Exception e) {
      // noop, expecting exceptions
      e.printStackTrace();
    }
  }

  // class exists but not a RawComparator
  public void testFailureBadJClasses() throws IOException {
    try {
      writer =
          new Writer(out, BLOCK_SIZE, compression,
              "jclass:org.apache.hadoop.io.file.tfile.Chunk", conf);
      Assert.fail("Failed to catch unsupported comparator names");
    }
    catch (Exception e) {
      // noop, expecting exceptions
      e.printStackTrace();
    }
  }

  private void closeOutput() throws IOException {
    if (writer != null) {
      writer.close();
      writer = null;
    }
    if (out != null) {
      out.close();
      out = null;
    }
  }
}
