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

package org.apache.hadoop.io.file.tfile;

import java.io.IOException;
import java.util.Random;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestVLong extends TestCase {
  private static String ROOT =
      System.getProperty("test.build.data", "/tmp/tfile-test");
  private Configuration conf;
  private FileSystem fs;
  private Path path;
  private String outputFile = "TestVLong";

  @Override
  public void setUp() throws IOException {
    conf = new Configuration();
    path = new Path(ROOT, outputFile);
    fs = path.getFileSystem(conf);
    if (fs.exists(path)) {
      fs.delete(path, false);
    }
  }

  @Override
  public void tearDown() throws IOException {
    if (fs.exists(path)) {
      fs.delete(path, false);
    }
  }

  public void testVLongByte() throws IOException {
    FSDataOutputStream out = fs.create(path);
    for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; ++i) {
      Utils.writeVLong(out, i);
    }
    out.close();
    Assert.assertEquals("Incorrect encoded size", (1 << Byte.SIZE) + 96, fs
        .getFileStatus(
        path).getLen());

    FSDataInputStream in = fs.open(path);
    for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; ++i) {
      long n = Utils.readVLong(in);
      Assert.assertEquals(n, i);
    }
    in.close();
    fs.delete(path, false);
  }

  private long writeAndVerify(int shift) throws IOException {
    FSDataOutputStream out = fs.create(path);
    for (int i = Short.MIN_VALUE; i <= Short.MAX_VALUE; ++i) {
      Utils.writeVLong(out, ((long) i) << shift);
    }
    out.close();
    FSDataInputStream in = fs.open(path);
    for (int i = Short.MIN_VALUE; i <= Short.MAX_VALUE; ++i) {
      long n = Utils.readVLong(in);
      Assert.assertEquals(n, ((long) i) << shift);
    }
    in.close();
    long ret = fs.getFileStatus(path).getLen();
    fs.delete(path, false);
    return ret;
  }
  
  public void testVLongShort() throws IOException {
    long size = writeAndVerify(0);
    Assert.assertEquals("Incorrect encoded size", (1 << Short.SIZE) * 2
        + ((1 << Byte.SIZE) - 40)
        * (1 << Byte.SIZE) - 128 - 32, size);
  }

  public void testVLong3Bytes() throws IOException {
    long size = writeAndVerify(Byte.SIZE);
    Assert.assertEquals("Incorrect encoded size", (1 << Short.SIZE) * 3
        + ((1 << Byte.SIZE) - 32) * (1 << Byte.SIZE) - 40 - 1, size);
  }

  public void testVLong4Bytes() throws IOException {
    long size = writeAndVerify(Byte.SIZE * 2);
    Assert.assertEquals("Incorrect encoded size", (1 << Short.SIZE) * 4
        + ((1 << Byte.SIZE) - 16) * (1 << Byte.SIZE) - 32 - 2, size);
  }

  public void testVLong5Bytes() throws IOException {
    long size = writeAndVerify(Byte.SIZE * 3);
     Assert.assertEquals("Incorrect encoded size", (1 << Short.SIZE) * 6 - 256
        - 16 - 3, size);
  }

  private void verifySixOrMoreBytes(int bytes) throws IOException {
    long size = writeAndVerify(Byte.SIZE * (bytes - 2));
    Assert.assertEquals("Incorrect encoded size", (1 << Short.SIZE)
        * (bytes + 1) - 256 - bytes + 1, size);
  }
  public void testVLong6Bytes() throws IOException {
    verifySixOrMoreBytes(6);
  }
  
  public void testVLong7Bytes() throws IOException {
    verifySixOrMoreBytes(7);
  }

  public void testVLong8Bytes() throws IOException {
    verifySixOrMoreBytes(8);
  }

  public void testVLongRandom() throws IOException {
    int count = 1024 * 1024;
    long data[] = new long[count];
    Random rng = new Random();
    for (int i = 0; i < data.length; ++i) {
      int shift = rng.nextInt(Long.SIZE) + 1;
      long mask = (1L << shift) - 1;
      long a = rng.nextInt() << 32;
      long b = ((long) rng.nextInt()) & 0xffffffff;
      data[i] = (a + b) & mask;
    }
    
    FSDataOutputStream out = fs.create(path);
    for (int i = 0; i < data.length; ++i) {
      Utils.writeVLong(out, data[i]);
    }
    out.close();

    FSDataInputStream in = fs.open(path);
    for (int i = 0; i < data.length; ++i) {
      Assert.assertEquals(Utils.readVLong(in), data[i]);
    }
    in.close();
    fs.delete(path, false);
  }
}
