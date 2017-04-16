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
package org.apache.hadoop.mapreduce.split;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class TestJobSplitWriter {

  static final String TEST_ROOT = System.getProperty("test.build.data", "/tmp");
  static final Path TEST_DIR =
    new Path(TEST_ROOT, TestJobSplitWriter.class.getSimpleName());

  @AfterClass
  public static void cleanup() throws IOException {
    final FileSystem fs = FileSystem.getLocal(new Configuration()).getRaw();
    fs.delete(TEST_DIR, true);
  }

  static abstract class NewSplit extends InputSplit implements Writable {
    @Override public long getLength() { return 42L; }
    @Override public void readFields(DataInput in) throws IOException { }
    @Override public void write(DataOutput in) throws IOException { }
  }

  @Test
  public void testSplitLocationLimit()
      throws IOException, InterruptedException  {
    final int SPLITS = 5;
    final int MAX_LOC = 10;
    final Path outdir = new Path(TEST_DIR, "testSplitLocationLimit");
    final String[] locs = getLoc(MAX_LOC + 5);
    final Configuration conf = new Configuration();
    final FileSystem rfs = FileSystem.getLocal(conf).getRaw();
    final InputSplit split = new NewSplit() {
      @Override public String[] getLocations() { return locs; }
    };
    List<InputSplit> splits = Collections.nCopies(SPLITS, split);

    conf.setInt(JobSplitWriter.MAX_SPLIT_LOCATIONS, MAX_LOC);
    JobSplitWriter.createSplitFiles(outdir, conf,
        FileSystem.getLocal(conf).getRaw(), splits);

    checkMeta(MAX_LOC,
        SplitMetaInfoReader.readSplitMetaInfo(null, rfs, conf, outdir),
        Arrays.copyOf(locs, MAX_LOC));

    conf.setInt(JobSplitWriter.MAX_SPLIT_LOCATIONS, MAX_LOC / 2);
    try {
      SplitMetaInfoReader.readSplitMetaInfo(null, rfs, conf, outdir);
      fail("Reader failed to detect location limit");
    } catch (IOException e) { }
  }

  static abstract class OldSplit
      implements org.apache.hadoop.mapred.InputSplit {
    @Override public long getLength() { return 42L; }
    @Override public void readFields(DataInput in) throws IOException { }
    @Override public void write(DataOutput in) throws IOException { }
  }

  @Test
  public void testSplitLocationLimitOldApi() throws IOException {
    final int SPLITS = 5;
    final int MAX_LOC = 10;
    final Path outdir = new Path(TEST_DIR, "testSplitLocationLimitOldApi");
    final String[] locs = getLoc(MAX_LOC + 5);
    final Configuration conf = new Configuration();
    final FileSystem rfs = FileSystem.getLocal(conf).getRaw();
    final org.apache.hadoop.mapred.InputSplit split = new OldSplit() {
      @Override public String[] getLocations() { return locs; }
    };
    org.apache.hadoop.mapred.InputSplit[] splits =
      new org.apache.hadoop.mapred.InputSplit[SPLITS];
    Arrays.fill(splits, split);

    conf.setInt(JobSplitWriter.MAX_SPLIT_LOCATIONS, MAX_LOC);
    JobSplitWriter.createSplitFiles(outdir, conf,
        FileSystem.getLocal(conf).getRaw(), splits);
    checkMeta(MAX_LOC,
        SplitMetaInfoReader.readSplitMetaInfo(null, rfs, conf, outdir),
        Arrays.copyOf(locs, MAX_LOC));

    conf.setInt(JobSplitWriter.MAX_SPLIT_LOCATIONS, MAX_LOC / 2);
    try {
      SplitMetaInfoReader.readSplitMetaInfo(null, rfs, conf, outdir);
      fail("Reader failed to detect location limit");
    } catch (IOException e) { }
  }

  private static void checkMeta(int MAX_LOC,
      JobSplit.TaskSplitMetaInfo[] metaSplits, String[] chk_locs) {
    for (JobSplit.TaskSplitMetaInfo meta : metaSplits) {
      final String[] meta_locs = meta.getLocations();
      assertEquals(MAX_LOC, meta_locs.length);
      assertArrayEquals(chk_locs, meta_locs);
    }
  }

  private static String[] getLoc(int locations) {
    final String ret[] = new String[locations];
    for (int i = 0; i < locations; ++i) {
      ret[i] = "LOC" + i;
    }
    return ret;
  }

}
