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
package org.apache.hadoop.mapred.gridmix;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestFileQueue {

  static final Log LOG = LogFactory.getLog(TestFileQueue.class);
  static final int NFILES = 4;
  static final int BLOCK = 256;
  static final Path[] paths = new Path[NFILES];
  static final String[] loc = new String[NFILES];
  static final long[] start = new long[NFILES];
  static final long[] len = new long[NFILES];

  @BeforeClass
  public static void setup() throws IOException {
    final Configuration conf = new Configuration();
    final FileSystem fs = FileSystem.getLocal(conf).getRaw();
    final Path p = new Path(System.getProperty("test.build.data", "/tmp"),
        "testFileQueue").makeQualified(fs);
    fs.delete(p, true);
    final byte[] b = new byte[BLOCK];
    for (int i = 0; i < NFILES; ++i) {
      Arrays.fill(b, (byte)('A' + i));
      paths[i] = new Path(p, "" + (char)('A' + i));
      OutputStream f = null;
      try {
        f = fs.create(paths[i]);
        f.write(b);
      } finally {
        if (f != null) {
          f.close();
        }
      }
    }
  }

  @AfterClass
  public static void cleanup() throws IOException {
    final Configuration conf = new Configuration();
    final FileSystem fs = FileSystem.getLocal(conf).getRaw();
    final Path p = new Path(System.getProperty("test.build.data", "/tmp"),
        "testFileQueue").makeQualified(fs);
    fs.delete(p, true);
  }

  static ByteArrayOutputStream fillVerif() throws IOException {
    final byte[] b = new byte[BLOCK];
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (int i = 0; i < NFILES; ++i) {
      Arrays.fill(b, (byte)('A' + i));
      out.write(b, 0, (int)len[i]);
    }
    return out;
  }

  @Test
  public void testRepeat() throws Exception {
    final Configuration conf = new Configuration();
    Arrays.fill(loc, "");
    Arrays.fill(start, 0L);
    Arrays.fill(len, BLOCK);

    final ByteArrayOutputStream out = fillVerif();
    final FileQueue q =
      new FileQueue(new CombineFileSplit(paths, start, len, loc), conf);
    final byte[] verif = out.toByteArray();
    final byte[] check = new byte[2 * NFILES * BLOCK];
    q.read(check, 0, NFILES * BLOCK);
    assertArrayEquals(verif, Arrays.copyOf(check, NFILES * BLOCK));

    final byte[] verif2 = new byte[2 * NFILES * BLOCK];
    System.arraycopy(verif, 0, verif2, 0, verif.length);
    System.arraycopy(verif, 0, verif2, verif.length, verif.length);
    q.read(check, 0, 2 * NFILES * BLOCK);
    assertArrayEquals(verif2, check);

  }

  @Test
  public void testUneven() throws Exception {
    final Configuration conf = new Configuration();
    Arrays.fill(loc, "");
    Arrays.fill(start, 0L);
    Arrays.fill(len, BLOCK);

    final int B2 = BLOCK / 2;
    for (int i = 0; i < NFILES; i += 2) {
      start[i] += B2;
      len[i] -= B2;
    }
    final FileQueue q =
      new FileQueue(new CombineFileSplit(paths, start, len, loc), conf);
    final ByteArrayOutputStream out = fillVerif();
    final byte[] verif = out.toByteArray();
    final byte[] check = new byte[NFILES / 2 * BLOCK + NFILES / 2 * B2];
    q.read(check, 0, verif.length);
    assertArrayEquals(verif, Arrays.copyOf(check, verif.length));
    q.read(check, 0, verif.length);
    assertArrayEquals(verif, Arrays.copyOf(check, verif.length));
  }

  @Test
  public void testEmpty() throws Exception {
    final Configuration conf = new Configuration();
    // verify OK if unused
    final FileQueue q = new FileQueue(new CombineFileSplit(
          new Path[0], new long[0], new long[0], new String[0]), conf);
  }

}
