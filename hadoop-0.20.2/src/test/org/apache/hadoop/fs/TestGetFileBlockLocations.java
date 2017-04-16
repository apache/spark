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

package org.apache.hadoop.fs;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;

/**
 * Testing the correctness of FileSystem.getFileBlockLocations.
 */
public class TestGetFileBlockLocations extends TestCase {
  private static String TEST_ROOT_DIR =
      System.getProperty("test.build.data", "/tmp/testGetFileBlockLocations");
  private static final int FileLength = 4 * 1024 * 1024; // 4MB
  private Configuration conf;
  private Path path;
  private FileSystem fs;
  private Random random;

  /**
   * @see TestCase#setUp()
   */
  @Override
  protected void setUp() throws IOException {
    conf = new Configuration();
    Path rootPath = new Path(TEST_ROOT_DIR);
    path = new Path(rootPath, "TestGetFileBlockLocations");
    fs = rootPath.getFileSystem(conf);
    FSDataOutputStream fsdos = fs.create(path, true);
    byte[] buffer = new byte[1024];
    while (fsdos.getPos() < FileLength) {
      fsdos.write(buffer);
    }
    fsdos.close();
    random = new Random(System.nanoTime());
  }

  private void oneTest(int offBegin, int offEnd, FileStatus status)
      throws IOException {
    if (offBegin > offEnd) {
      int tmp = offBegin;
      offBegin = offEnd;
      offEnd = tmp;
    }
    BlockLocation[] locations =
        fs.getFileBlockLocations(status, offBegin, offEnd - offBegin);
    if (offBegin < status.getLen()) {
      Arrays.sort(locations, new Comparator<BlockLocation>() {

        @Override
        public int compare(BlockLocation arg0, BlockLocation arg1) {
          long cmprv = arg0.getOffset() - arg1.getOffset();
          if (cmprv < 0) return -1;
          if (cmprv > 0) return 1;
          cmprv = arg0.getLength() - arg1.getLength();
          if (cmprv < 0) return -1;
          if (cmprv > 0) return 1;
          return 0;
        }

      });
      offBegin = (int) Math.min(offBegin, status.getLen() - 1);
      offEnd = (int) Math.min(offEnd, status.getLen());
      BlockLocation first = locations[0];
      BlockLocation last = locations[locations.length - 1];
      assertTrue(first.getOffset() <= offBegin);
      assertTrue(offEnd <= last.getOffset() + last.getLength());
    } else {
      assertTrue(locations.length == 0);
    }
  }
  /**
   * @see TestCase#tearDown()
   */
  @Override
  protected void tearDown() throws IOException {
    fs.delete(path, true);
    fs.close();
  }

  public void testFailureNegativeParameters() throws IOException {
    FileStatus status = fs.getFileStatus(path);
    try {
      BlockLocation[] locations = fs.getFileBlockLocations(status, -1, 100);
      fail("Expecting exception being throw");
    } catch (IllegalArgumentException e) {

    }

    try {
      BlockLocation[] locations = fs.getFileBlockLocations(status, 100, -1);
      fail("Expecting exception being throw");
    } catch (IllegalArgumentException e) {

    }
  }

  public void testGetFileBlockLocations1() throws IOException {
    FileStatus status = fs.getFileStatus(path);
    oneTest(0, (int) status.getLen(), status);
    oneTest(0, (int) status.getLen() * 2, status);
    oneTest((int) status.getLen() * 2, (int) status.getLen() * 4, status);
    oneTest((int) status.getLen() / 2, (int) status.getLen() * 3, status);
    for (int i = 0; i < 10; ++i) {
      oneTest((int) status.getLen() * i / 10, (int) status.getLen() * (i + 1)
          / 10, status);
    }
  }

  public void testGetFileBlockLocations2() throws IOException {
    FileStatus status = fs.getFileStatus(path);
    for (int i = 0; i < 1000; ++i) {
      int offBegin = random.nextInt((int) (2 * status.getLen()));
      int offEnd = random.nextInt((int) (2 * status.getLen()));
      oneTest(offBegin, offEnd, status);
    }
  }
}
