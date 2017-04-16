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

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestFilePool {

  static final Log LOG = LogFactory.getLog(TestFileQueue.class);
  static final int NFILES = 26;
  static final Path base = getBaseDir();

  static Path getBaseDir() {
    try {
      final Configuration conf = new Configuration();
      final FileSystem fs = FileSystem.getLocal(conf).getRaw();
      return new Path(System.getProperty("test.build.data", "/tmp"),
          "testFilePool").makeQualified(fs);
    } catch (IOException e) {
      fail();
    }
    return null;
  }

  @BeforeClass
  public static void setup() throws IOException {
    final Configuration conf = new Configuration();
    final FileSystem fs = FileSystem.getLocal(conf).getRaw();
    fs.delete(base, true);
    final Random r = new Random();
    final long seed = r.nextLong();
    r.setSeed(seed);
    LOG.info("seed: " + seed);
    fs.mkdirs(base);
    for (int i = 0; i < NFILES; ++i) {
      Path file = base;
      for (double d = 0.6; d > 0.0; d *= 0.8) {
        if (r.nextDouble() < d) {
          file = new Path(base, Integer.toString(r.nextInt(3)));
          continue;
        }
        break;
      }
      OutputStream out = null;
      try {
        out = fs.create(new Path(file, "" + (char)('A' + i)));
        final byte[] b = new byte[1024];
        Arrays.fill(b, (byte)('A' + i));
        for (int len = ((i % 13) + 1) * 1024; len > 0; len -= 1024) {
          out.write(b);
        }
      } finally {
        if (out != null) {
          out.close();
        }
      }
    }
  }

  @AfterClass
  public static void cleanup() throws IOException {
    final Configuration conf = new Configuration();
    final FileSystem fs = FileSystem.getLocal(conf).getRaw();
    fs.delete(base, true);
  }

  @Test
  public void testUnsuitable() throws Exception {
    try {
      final Configuration conf = new Configuration();
      // all files 13k or less
      conf.setLong(FilePool.GRIDMIX_MIN_FILE, 14 * 1024);
      final FilePool pool = new FilePool(conf, base);
      pool.refresh();
    } catch (IOException e) {
      return;
    }
    fail();
  }

  @Test
  public void testPool() throws Exception {
    final Random r = new Random();
    final Configuration conf = new Configuration();
    conf.setLong(FilePool.GRIDMIX_MIN_FILE, 3 * 1024);
    final FilePool pool = new FilePool(conf, base);
    pool.refresh();
    final ArrayList<FileStatus> files = new ArrayList<FileStatus>();

    // ensure 1k, 2k files excluded
    final int expectedPoolSize = (NFILES / 2 * (NFILES / 2 + 1) - 6) * 1024;
    assertEquals(expectedPoolSize, pool.getInputFiles(Long.MAX_VALUE, files));
    assertEquals(NFILES - 4, files.size());

    // exact match
    files.clear();
    assertEquals(expectedPoolSize, pool.getInputFiles(expectedPoolSize, files));

    // match random within 12k
    files.clear();
    final long rand = r.nextInt(expectedPoolSize);
    assertTrue("Missed: " + rand,
        (NFILES / 2) * 1024 > rand - pool.getInputFiles(rand, files));

    // all files
    conf.setLong(FilePool.GRIDMIX_MIN_FILE, 0);
    pool.refresh();
    files.clear();
    assertEquals((NFILES / 2 * (NFILES / 2 + 1)) * 1024,
        pool.getInputFiles(Long.MAX_VALUE, files));
  }

  void checkSplitEq(FileSystem fs, CombineFileSplit split, long bytes)
      throws Exception {
    long splitBytes = 0L;
    HashSet<Path> uniq = new HashSet<Path>();
    for (int i = 0; i < split.getNumPaths(); ++i) {
      splitBytes += split.getLength(i);
      assertTrue(
          split.getLength(i) <= fs.getFileStatus(split.getPath(i)).getLen());
      assertFalse(uniq.contains(split.getPath(i)));
      uniq.add(split.getPath(i));
    }
    assertEquals(bytes, splitBytes);
  }

  @Test
  public void testStriper() throws Exception {
    final Random r = new Random();
    final Configuration conf = new Configuration();
    final FileSystem fs = FileSystem.getLocal(conf).getRaw();
    conf.setLong(FilePool.GRIDMIX_MIN_FILE, 3 * 1024);
    final FilePool pool = new FilePool(conf, base) {
      @Override
      public BlockLocation[] locationsFor(FileStatus stat, long start, long len)
          throws IOException {
        return new BlockLocation[] { new BlockLocation() };
      }
    };
    pool.refresh();

    final int expectedPoolSize = (NFILES / 2 * (NFILES / 2 + 1) - 6) * 1024;
    final InputStriper striper = new InputStriper(pool, expectedPoolSize);
    int last = 0;
    for (int i = 0; i < expectedPoolSize;
        last = Math.min(expectedPoolSize - i, r.nextInt(expectedPoolSize))) {
      checkSplitEq(fs, striper.splitFor(pool, last, 0), last);
      i += last;
    }
    final InputStriper striper2 = new InputStriper(pool, expectedPoolSize);
    checkSplitEq(fs, striper2.splitFor(pool, expectedPoolSize, 0),
        expectedPoolSize);
  }

}
