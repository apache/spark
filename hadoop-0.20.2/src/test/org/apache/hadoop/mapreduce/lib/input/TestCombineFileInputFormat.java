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

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

public class TestCombineFileInputFormat extends TestCase {

  private static final String rack1[] = new String[] {
    "/r1"
  };
  private static final String hosts1[] = new String[] {
    "host1.rack1.com"
  };
  private static final String rack2[] = new String[] {
    "/r2"
  };
  private static final String hosts2[] = new String[] {
    "host2.rack2.com"
  };
  private static final String rack3[] = new String[] {
    "/r3"
  };
  private static final String hosts3[] = new String[] {
    "host3.rack3.com"
  };
  final Path inDir = new Path("/racktesting");
  final Path outputPath = new Path("/output");
  final Path dir1 = new Path(inDir, "/dir1");
  final Path dir2 = new Path(inDir, "/dir2");
  final Path dir3 = new Path(inDir, "/dir3");
  final Path dir4 = new Path(inDir, "/dir4");

  static final int BLOCKSIZE = 1024;
  static final byte[] databuf = new byte[BLOCKSIZE];

  /** Dummy class to extend CombineFileInputFormat*/
  private class DummyInputFormat extends CombineFileInputFormat<Text, Text> {
    @Override
    public RecordReader<Text,Text> createRecordReader(InputSplit split, 
        TaskAttemptContext context) throws IOException {
      return null;
    }
  }

  /** Dummy class to extend CombineFileInputFormat. It allows 
   * non-existent files to be passed into the CombineFileInputFormat, allows
   * for easy testing without having to create real files.
   */
  private class DummyInputFormat1 extends DummyInputFormat {
    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException {
      Path[] files = getInputPaths(job);
      List<FileStatus> results = new ArrayList<FileStatus>();
      for (int i = 0; i < files.length; i++) {
        Path p = files[i];
        FileSystem fs = p.getFileSystem(job.getConfiguration());
        results.add(fs.getFileStatus(p));
      }
      return results;
    }
  }

  private static final String DUMMY_KEY = "dummy.rr.key";

  private static class DummyRecordReader extends RecordReader<Text, Text> {
    private TaskAttemptContext context;
    private CombineFileSplit s;
    private int idx;
    private boolean used;

    public DummyRecordReader(CombineFileSplit split, TaskAttemptContext context,
        Integer i) {
      this.context = context;
      this.idx = i;
      this.s = split;
      this.used = true;
    }

    /** @return a value specified in the context to check whether the
     * context is properly updated by the initialize() method.
     */
    public String getDummyConfVal() {
      return this.context.getConfiguration().get(DUMMY_KEY);
    }

    public void initialize(InputSplit split, TaskAttemptContext context) {
      this.context = context;
      this.s = (CombineFileSplit) split;

      // By setting used to true in the c'tor, but false in initialize,
      // we can check that initialize() is always called before use
      // (e.g., in testReinit()).
      this.used = false;
    }

    public boolean nextKeyValue() {
      boolean ret = !used;
      this.used = true;
      return ret;
    }

    public Text getCurrentKey() {
      return new Text(this.context.getConfiguration().get(DUMMY_KEY));
    }

    public Text getCurrentValue() {
      return new Text(this.s.getPath(idx).toString());
    }

    public float getProgress() {
      return used ? 1.0f : 0.0f;
    }

    public void close() {
    }
  }

  /** Extend CFIF to use CFRR with DummyRecordReader */
  private class ChildRRInputFormat extends CombineFileInputFormat<Text, Text> {
    @SuppressWarnings("unchecked")
    @Override
    public RecordReader<Text,Text> createRecordReader(InputSplit split, 
        TaskAttemptContext context) throws IOException {
      return new CombineFileRecordReader((CombineFileSplit) split, context,
          (Class) DummyRecordReader.class);
    }
  }

  public void testRecordReaderInit() throws InterruptedException, IOException {
    // Test that we properly initialize the child recordreader when
    // CombineFileInputFormat and CombineFileRecordReader are used.

    TaskAttemptID taskId = new TaskAttemptID("jt", 0, true, 0, 0);
    Configuration conf1 = new Configuration();
    conf1.set(DUMMY_KEY, "STATE1");
    TaskAttemptContext context1 = new TaskAttemptContext(conf1, taskId);

    // This will create a CombineFileRecordReader that itself contains a
    // DummyRecordReader.
    InputFormat inputFormat = new ChildRRInputFormat();

    Path [] files = { new Path("file1") };
    long [] lengths = { 1 };

    CombineFileSplit split = new CombineFileSplit(files, lengths);

    RecordReader rr = inputFormat.createRecordReader(split, context1);
    assertTrue("Unexpected RR type!", rr instanceof CombineFileRecordReader);

    // Verify that the initial configuration is the one being used.
    // Right after construction the dummy key should have value "STATE1"
    assertEquals("Invalid initial dummy key value", "STATE1",
      rr.getCurrentKey().toString());

    // Switch the active context for the RecordReader...
    Configuration conf2 = new Configuration();
    conf2.set(DUMMY_KEY, "STATE2");
    TaskAttemptContext context2 = new TaskAttemptContext(conf2, taskId);
    rr.initialize(split, context2);

    // And verify that the new context is updated into the child record reader.
    assertEquals("Invalid secondary dummy key value", "STATE2",
      rr.getCurrentKey().toString());
  }

  public void testReinit() throws Exception {
    // Test that a split containing multiple files works correctly,
    // with the child RecordReader getting its initialize() method
    // called a second time.
    TaskAttemptID taskId = new TaskAttemptID("jt", 0, true, 0, 0);
    Configuration conf = new Configuration();
    TaskAttemptContext context = new TaskAttemptContext(conf, taskId);

    // This will create a CombineFileRecordReader that itself contains a
    // DummyRecordReader.
    InputFormat inputFormat = new ChildRRInputFormat();

    Path [] files = { new Path("file1"), new Path("file2") };
    long [] lengths = { 1, 1 };

    CombineFileSplit split = new CombineFileSplit(files, lengths);
    RecordReader rr = inputFormat.createRecordReader(split, context);
    assertTrue("Unexpected RR type!", rr instanceof CombineFileRecordReader);

    // first initialize() call comes from MapTask. We'll do it here.
    rr.initialize(split, context);

    // First value is first filename.
    assertTrue(rr.nextKeyValue());
    assertEquals("file1", rr.getCurrentValue().toString());

    // The inner RR will return false, because it only emits one (k, v) pair.
    // But there's another sub-split to process. This returns true to us.
    assertTrue(rr.nextKeyValue());
    
    // And the 2nd rr will have its initialize method called correctly.
    assertEquals("file2", rr.getCurrentValue().toString());
    
    // But after both child RR's have returned their singleton (k, v), this
    // should also return false.
    assertFalse(rr.nextKeyValue());
  }

  public void testSplitPlacement() throws IOException {
    MiniDFSCluster dfs = null;
    FileSystem fileSys = null;
    try {
      /* Start 3 datanodes, one each in rack r1, r2, r3. Create three files
       * 1) file1, just after starting the datanode on r1, with 
       *    a repl factor of 1, and,
       * 2) file2, just after starting the datanode on r2, with 
       *    a repl factor of 2, and,
       * 3) file3 after starting the all three datanodes, with a repl 
       *    factor of 3.
       * At the end, file1 will be present on only datanode1, file2 will be
       * present on datanode 1 and datanode2 and 
       * file3 will be present on all datanodes. 
       */
      Configuration conf = new Configuration();
      conf.setBoolean("dfs.replication.considerLoad", false);
      dfs = new MiniDFSCluster(conf, 1, true, rack1, hosts1);
      dfs.waitActive();

      fileSys = dfs.getFileSystem();
      if (!fileSys.mkdirs(inDir)) {
        throw new IOException("Mkdirs failed to create " + inDir.toString());
      }
      Path file1 = new Path(dir1 + "/file1");
      writeFile(conf, file1, (short)1, 1);
      dfs.startDataNodes(conf, 1, true, null, rack2, hosts2, null);
      dfs.waitActive();

      // create file on two datanodes.
      Path file2 = new Path(dir2 + "/file2");
      writeFile(conf, file2, (short)2, 2);

      // split it using a CombinedFile input format
      DummyInputFormat inFormat = new DummyInputFormat();
      Job job = new Job(conf);
      FileInputFormat.setInputPaths(job, dir1 + "," + dir2);
      inFormat.setMinSplitSizeRack(BLOCKSIZE);
      List<InputSplit> splits = inFormat.getSplits(job);
      System.out.println("Made splits(Test1): " + splits.size());

      // make sure that each split has different locations
      for (InputSplit split : splits) {
        System.out.println("File split(Test1): " + split);
      }
      assertEquals(splits.size(), 2);
      CombineFileSplit fileSplit = (CombineFileSplit) splits.get(0);
      assertEquals(fileSplit.getNumPaths(), 2);
      assertEquals(fileSplit.getLocations().length, 1);
      assertEquals(fileSplit.getPath(0).getName(), file2.getName());
      assertEquals(fileSplit.getOffset(0), 0);
      assertEquals(fileSplit.getLength(0), BLOCKSIZE);
      assertEquals(fileSplit.getPath(1).getName(), file2.getName());
      assertEquals(fileSplit.getOffset(1), BLOCKSIZE);
      assertEquals(fileSplit.getLength(1), BLOCKSIZE);
      assertEquals(fileSplit.getLocations()[0], hosts2[0]); // should be on r2
      fileSplit = (CombineFileSplit) splits.get(1);
      assertEquals(fileSplit.getNumPaths(), 1);
      assertEquals(fileSplit.getLocations().length, 1);
      assertEquals(fileSplit.getPath(0).getName(), file1.getName());
      assertEquals(fileSplit.getOffset(0), 0);
      assertEquals(fileSplit.getLength(0), BLOCKSIZE);
      assertEquals(fileSplit.getLocations()[0], hosts1[0]); // should be on r1

      // create another file on 3 datanodes and 3 racks.
      dfs.startDataNodes(conf, 1, true, null, rack3, hosts3, null);
      dfs.waitActive();
      Path file3 = new Path(dir3 + "/file3");
      writeFile(conf, new Path(dir3 + "/file3"), (short)3, 3);
      inFormat = new DummyInputFormat();
      FileInputFormat.setInputPaths(job, dir1 + "," + dir2 + "," + dir3);
      inFormat.setMinSplitSizeRack(BLOCKSIZE);
      splits = inFormat.getSplits(job);
      for (InputSplit split : splits) {
        System.out.println("File split(Test2): " + split);
      }
      assertEquals(splits.size(), 3);
      fileSplit = (CombineFileSplit) splits.get(0);
      assertEquals(fileSplit.getNumPaths(), 3);
      assertEquals(fileSplit.getLocations().length, 1);
      assertEquals(fileSplit.getPath(0).getName(), file3.getName());
      assertEquals(fileSplit.getOffset(0), 0);
      assertEquals(fileSplit.getLength(0), BLOCKSIZE);
      assertEquals(fileSplit.getPath(1).getName(), file3.getName());
      assertEquals(fileSplit.getOffset(1), BLOCKSIZE);
      assertEquals(fileSplit.getLength(1), BLOCKSIZE);
      assertEquals(fileSplit.getPath(2).getName(), file3.getName());
      assertEquals(fileSplit.getOffset(2), 2 * BLOCKSIZE);
      assertEquals(fileSplit.getLength(2), BLOCKSIZE);
      assertEquals(fileSplit.getLocations()[0], hosts3[0]); // should be on r3
      fileSplit = (CombineFileSplit) splits.get(1);
      assertEquals(fileSplit.getNumPaths(), 2);
      assertEquals(fileSplit.getLocations().length, 1);
      assertEquals(fileSplit.getPath(0).getName(), file2.getName());
      assertEquals(fileSplit.getOffset(0), 0);
      assertEquals(fileSplit.getLength(0), BLOCKSIZE);
      assertEquals(fileSplit.getPath(1).getName(), file2.getName());
      assertEquals(fileSplit.getOffset(1), BLOCKSIZE);
      assertEquals(fileSplit.getLength(1), BLOCKSIZE);
      assertEquals(fileSplit.getLocations()[0], hosts2[0]); // should be on r2
      fileSplit = (CombineFileSplit) splits.get(2);
      assertEquals(fileSplit.getNumPaths(), 1);
      assertEquals(fileSplit.getLocations().length, 1);
      assertEquals(fileSplit.getPath(0).getName(), file1.getName());
      assertEquals(fileSplit.getOffset(0), 0);
      assertEquals(fileSplit.getLength(0), BLOCKSIZE);
      assertEquals(fileSplit.getLocations()[0], hosts1[0]); // should be on r1

      // create file4 on all three racks
      Path file4 = new Path(dir4 + "/file4");
      writeFile(conf, file4, (short)3, 3);
      inFormat = new DummyInputFormat();
      FileInputFormat.setInputPaths(job, dir1 + "," + dir2 + "," + dir3 + "," + dir4);
      inFormat.setMinSplitSizeRack(BLOCKSIZE);
      splits = inFormat.getSplits(job);
      for (InputSplit split : splits) {
        System.out.println("File split(Test3): " + split);
      }
      assertEquals(splits.size(), 3);
      fileSplit = (CombineFileSplit) splits.get(0);
      assertEquals(fileSplit.getNumPaths(), 6);
      assertEquals(fileSplit.getLocations().length, 1);
      assertEquals(fileSplit.getPath(0).getName(), file3.getName());
      assertEquals(fileSplit.getOffset(0), 0);
      assertEquals(fileSplit.getLength(0), BLOCKSIZE);
      assertEquals(fileSplit.getPath(1).getName(), file3.getName());
      assertEquals(fileSplit.getOffset(1), BLOCKSIZE);
      assertEquals(fileSplit.getLength(1), BLOCKSIZE);
      assertEquals(fileSplit.getPath(2).getName(), file3.getName());
      assertEquals(fileSplit.getOffset(2), 2 * BLOCKSIZE);
      assertEquals(fileSplit.getLength(2), BLOCKSIZE);
      assertEquals(fileSplit.getLocations()[0], hosts3[0]); // should be on r3
      fileSplit = (CombineFileSplit) splits.get(1);
      assertEquals(fileSplit.getNumPaths(), 2);
      assertEquals(fileSplit.getLocations().length, 1);
      assertEquals(fileSplit.getPath(0).getName(), file2.getName());
      assertEquals(fileSplit.getOffset(0), 0);
      assertEquals(fileSplit.getLength(0), BLOCKSIZE);
      assertEquals(fileSplit.getPath(1).getName(), file2.getName());
      assertEquals(fileSplit.getOffset(1), BLOCKSIZE);
      assertEquals(fileSplit.getLength(1), BLOCKSIZE);
      assertEquals(fileSplit.getLocations()[0], hosts2[0]); // should be on r2
      fileSplit = (CombineFileSplit) splits.get(2);
      assertEquals(fileSplit.getNumPaths(), 1);
      assertEquals(fileSplit.getLocations().length, 1);
      assertEquals(fileSplit.getPath(0).getName(), file1.getName());
      assertEquals(fileSplit.getOffset(0), 0);
      assertEquals(fileSplit.getLength(0), BLOCKSIZE);
      assertEquals(fileSplit.getLocations()[0], hosts1[0]); // should be on r1

      // maximum split size is 2 blocks 
      inFormat = new DummyInputFormat();
      inFormat.setMinSplitSizeNode(BLOCKSIZE);
      inFormat.setMaxSplitSize(2*BLOCKSIZE);
      FileInputFormat.setInputPaths(job, 
        dir1 + "," + dir2 + "," + dir3 + "," + dir4);
      splits = inFormat.getSplits(job);
      for (InputSplit split : splits) {
        System.out.println("File split(Test4): " + split);
      }
      assertEquals(splits.size(), 5);
      fileSplit = (CombineFileSplit) splits.get(0);
      assertEquals(fileSplit.getNumPaths(), 2);
      assertEquals(fileSplit.getLocations().length, 1);
      assertEquals(fileSplit.getPath(0).getName(), file3.getName());
      assertEquals(fileSplit.getOffset(0), 0);
      assertEquals(fileSplit.getLength(0), BLOCKSIZE);
      assertEquals(fileSplit.getPath(1).getName(), file3.getName());
      assertEquals(fileSplit.getOffset(1), BLOCKSIZE);
      assertEquals(fileSplit.getLength(1), BLOCKSIZE);
      assertEquals(fileSplit.getLocations()[0], "host3.rack3.com");
      fileSplit = (CombineFileSplit) splits.get(1);
      assertEquals(fileSplit.getPath(0).getName(), file3.getName());
      assertEquals(fileSplit.getOffset(0), 2 * BLOCKSIZE);
      assertEquals(fileSplit.getLength(0), BLOCKSIZE);
      assertEquals(fileSplit.getPath(1).getName(), file4.getName());
      assertEquals(fileSplit.getOffset(1), 0);
      assertEquals(fileSplit.getLength(1), BLOCKSIZE);
      assertEquals(fileSplit.getLocations()[0], "host3.rack3.com");
      fileSplit = (CombineFileSplit) splits.get(2);
      assertEquals(fileSplit.getNumPaths(), 2);
      assertEquals(fileSplit.getLocations().length, 1);
      assertEquals(fileSplit.getPath(0).getName(), file4.getName());
      assertEquals(fileSplit.getOffset(0), BLOCKSIZE);
      assertEquals(fileSplit.getLength(0), BLOCKSIZE);
      assertEquals(fileSplit.getPath(1).getName(), file4.getName());
      assertEquals(fileSplit.getOffset(1), 2 * BLOCKSIZE);
      assertEquals(fileSplit.getLength(1), BLOCKSIZE);
      assertEquals(fileSplit.getLocations()[0], "host3.rack3.com");

      // maximum split size is 3 blocks 
      inFormat = new DummyInputFormat();
      inFormat.setMinSplitSizeNode(BLOCKSIZE);
      inFormat.setMaxSplitSize(3*BLOCKSIZE);
      FileInputFormat.setInputPaths(job, 
        dir1 + "," + dir2 + "," + dir3 + "," + dir4);
      splits = inFormat.getSplits(job);
      for (InputSplit split : splits) {
        System.out.println("File split(Test1): " + split);
      }
      assertEquals(splits.size(), 4);
      fileSplit = (CombineFileSplit) splits.get(0);
      assertEquals(fileSplit.getNumPaths(), 3);
      assertEquals(fileSplit.getLocations().length, 1);
      assertEquals(fileSplit.getPath(0).getName(), file3.getName());
      assertEquals(fileSplit.getOffset(0), 0);
      assertEquals(fileSplit.getLength(0), BLOCKSIZE);
      assertEquals(fileSplit.getPath(1).getName(), file3.getName());
      assertEquals(fileSplit.getOffset(1), BLOCKSIZE);
      assertEquals(fileSplit.getLength(1), BLOCKSIZE);
      assertEquals(fileSplit.getPath(2).getName(), file3.getName());
      assertEquals(fileSplit.getOffset(2), 2 * BLOCKSIZE);
      assertEquals(fileSplit.getLength(2), BLOCKSIZE);
      assertEquals(fileSplit.getLocations()[0], "host3.rack3.com");
      fileSplit = (CombineFileSplit) splits.get(1);
      assertEquals(fileSplit.getPath(0).getName(), file4.getName());
      assertEquals(fileSplit.getOffset(0), 0);
      assertEquals(fileSplit.getLength(0), BLOCKSIZE);
      assertEquals(fileSplit.getPath(1).getName(), file4.getName());
      assertEquals(fileSplit.getOffset(1), BLOCKSIZE);
      assertEquals(fileSplit.getLength(1), BLOCKSIZE);
      assertEquals(fileSplit.getPath(2).getName(), file4.getName());
      assertEquals(fileSplit.getOffset(2),  2 * BLOCKSIZE);
      assertEquals(fileSplit.getLength(2), BLOCKSIZE);
      assertEquals(fileSplit.getLocations()[0], "host3.rack3.com");
      fileSplit = (CombineFileSplit) splits.get(2);
      assertEquals(fileSplit.getNumPaths(), 2);
      assertEquals(fileSplit.getLocations().length, 1);
      assertEquals(fileSplit.getPath(0).getName(), file2.getName());
      assertEquals(fileSplit.getOffset(0), 0);
      assertEquals(fileSplit.getLength(0), BLOCKSIZE);
      assertEquals(fileSplit.getPath(1).getName(), file2.getName());
      assertEquals(fileSplit.getOffset(1), BLOCKSIZE);
      assertEquals(fileSplit.getLength(1), BLOCKSIZE);
      assertEquals(fileSplit.getLocations()[0], "host2.rack2.com");
      fileSplit = (CombineFileSplit) splits.get(3);
      assertEquals(fileSplit.getNumPaths(), 1);
      assertEquals(fileSplit.getLocations().length, 1);
      assertEquals(fileSplit.getPath(0).getName(), file1.getName());
      assertEquals(fileSplit.getOffset(0), 0);
      assertEquals(fileSplit.getLength(0), BLOCKSIZE);
      assertEquals(fileSplit.getLocations()[0], "host1.rack1.com");

      // maximum split size is 4 blocks 
      inFormat = new DummyInputFormat();
      inFormat.setMaxSplitSize(4*BLOCKSIZE);
      FileInputFormat.setInputPaths(job, dir1 + "," + dir2 + "," + dir3 + "," + dir4);
      splits = inFormat.getSplits(job);
      for (InputSplit split : splits) {
        System.out.println("File split(Test1): " + split);
      }
      assertEquals(splits.size(), 3);
      fileSplit = (CombineFileSplit) splits.get(0);
      assertEquals(fileSplit.getNumPaths(), 4);
      assertEquals(fileSplit.getLocations().length, 1);
      assertEquals(fileSplit.getPath(0).getName(), file3.getName());
      assertEquals(fileSplit.getOffset(0), 0);
      assertEquals(fileSplit.getLength(0), BLOCKSIZE);
      assertEquals(fileSplit.getPath(1).getName(), file3.getName());
      assertEquals(fileSplit.getOffset(1), BLOCKSIZE);
      assertEquals(fileSplit.getLength(1), BLOCKSIZE);
      assertEquals(fileSplit.getPath(2).getName(), file3.getName());
      assertEquals(fileSplit.getOffset(2), 2 * BLOCKSIZE);
      assertEquals(fileSplit.getLength(2), BLOCKSIZE);
      assertEquals(fileSplit.getLocations()[0], "host3.rack3.com");
      fileSplit = (CombineFileSplit) splits.get(1);
      assertEquals(fileSplit.getNumPaths(), 4);
      assertEquals(fileSplit.getPath(0).getName(), file2.getName());
      assertEquals(fileSplit.getOffset(0), 0);
      assertEquals(fileSplit.getLength(0), BLOCKSIZE);
      assertEquals(fileSplit.getPath(1).getName(), file2.getName());
      assertEquals(fileSplit.getOffset(1), BLOCKSIZE);
      assertEquals(fileSplit.getLength(1), BLOCKSIZE);
      assertEquals(fileSplit.getPath(2).getName(), file4.getName());
      assertEquals(fileSplit.getOffset(2), BLOCKSIZE);
      assertEquals(fileSplit.getLength(2), BLOCKSIZE);
      assertEquals(fileSplit.getPath(3).getName(), file4.getName());
      assertEquals(fileSplit.getOffset(3),  2 * BLOCKSIZE);
      assertEquals(fileSplit.getLength(3), BLOCKSIZE);
      assertEquals(fileSplit.getLocations()[0], "host2.rack2.com");
      fileSplit = (CombineFileSplit) splits.get(2);
      assertEquals(fileSplit.getNumPaths(), 1);
      assertEquals(fileSplit.getLocations().length, 1);
      assertEquals(fileSplit.getPath(0).getName(), file1.getName());
      assertEquals(fileSplit.getOffset(0), 0);
      assertEquals(fileSplit.getLength(0), BLOCKSIZE);
      assertEquals(fileSplit.getLocations()[0], hosts1[0]); // should be on r1

      // maximum split size is 7 blocks and min is 3 blocks
      inFormat = new DummyInputFormat();
      inFormat.setMaxSplitSize(7*BLOCKSIZE);
      inFormat.setMinSplitSizeNode(3*BLOCKSIZE);
      inFormat.setMinSplitSizeRack(3*BLOCKSIZE);
      FileInputFormat.setInputPaths(job, 
        dir1 + "," + dir2 + "," + dir3 + "," + dir4);
      splits = inFormat.getSplits(job);
      for (InputSplit split : splits) {
        System.out.println("File split(Test1): " + split);
      }
      assertEquals(splits.size(), 2);
      fileSplit = (CombineFileSplit) splits.get(0);
      assertEquals(fileSplit.getNumPaths(), 6);
      assertEquals(fileSplit.getLocations().length, 1);
      assertEquals(fileSplit.getLocations()[0], "host3.rack3.com");
      fileSplit = (CombineFileSplit) splits.get(1);
      assertEquals(fileSplit.getNumPaths(), 3);
      assertEquals(fileSplit.getLocations().length, 1);
      assertEquals(fileSplit.getLocations()[0], "host1.rack1.com");

      // Rack 1 has file1, file2 and file3 and file4
      // Rack 2 has file2 and file3 and file4
      // Rack 3 has file3 and file4
      // setup a filter so that only file1 and file2 can be combined
      inFormat = new DummyInputFormat();
      FileInputFormat.addInputPath(job, inDir);
      inFormat.setMinSplitSizeRack(1); // everything is at least rack local
      inFormat.createPool(new TestFilter(dir1), 
                          new TestFilter(dir2));
      splits = inFormat.getSplits(job);
      for (InputSplit split : splits) {
        System.out.println("File split(Test1): " + split);
      }
      assertEquals(splits.size(), 3);
      fileSplit = (CombineFileSplit) splits.get(0);
      assertEquals(fileSplit.getNumPaths(), 2);
      assertEquals(fileSplit.getLocations().length, 1);
      assertEquals(fileSplit.getLocations()[0], hosts2[0]); // should be on r2
      fileSplit = (CombineFileSplit) splits.get(1);
      assertEquals(fileSplit.getNumPaths(), 1);
      assertEquals(fileSplit.getLocations().length, 1);
      assertEquals(fileSplit.getLocations()[0], hosts1[0]); // should be on r1
      fileSplit = (CombineFileSplit) splits.get(2);
      assertEquals(fileSplit.getNumPaths(), 6);
      assertEquals(fileSplit.getLocations().length, 1);
      assertEquals(fileSplit.getLocations()[0], hosts3[0]); // should be on r3

      // measure performance when there are multiple pools and
      // many files in each pool.
      int numPools = 100;
      int numFiles = 1000;
      DummyInputFormat1 inFormat1 = new DummyInputFormat1();
      for (int i = 0; i < numFiles; i++) {
        FileInputFormat.setInputPaths(job, file1);
      }
      inFormat1.setMinSplitSizeRack(1); // everything is at least rack local
      final Path dirNoMatch1 = new Path(inDir, "/dirxx");
      final Path dirNoMatch2 = new Path(inDir, "/diryy");
      for (int i = 0; i < numPools; i++) {
        inFormat1.createPool(new TestFilter(dirNoMatch1), 
                            new TestFilter(dirNoMatch2));
      }
      long start = System.currentTimeMillis();
      splits = inFormat1.getSplits(job);
      long end = System.currentTimeMillis();
      System.out.println("Elapsed time for " + numPools + " pools " +
                         " and " + numFiles + " files is " + 
                         ((end - start)/1000) + " seconds.");
    } finally {
      if (dfs != null) {
        dfs.shutdown();
      }
    }
  }

  static void writeFile(Configuration conf, Path name,
      short replication, int numBlocks) throws IOException {
    FileSystem fileSys = FileSystem.get(conf);

    FSDataOutputStream stm = fileSys.create(name, true,
                                            conf.getInt("io.file.buffer.size", 4096),
                                            replication, (long)BLOCKSIZE);
    for (int i = 0; i < numBlocks; i++) {
      stm.write(databuf);
    }
    stm.close();
    DFSTestUtil.waitReplication(fileSys, name, replication);
  }

  static class TestFilter implements PathFilter {
    private Path p;

    // store a path prefix in this TestFilter
    public TestFilter(Path p) {
      this.p = p;
    }

    // returns true if the specified path matches the prefix stored
    // in this TestFilter.
    public boolean accept(Path path) {
      if (path.toString().indexOf(p.toString()) == 0) {
        return true;
      }
      return false;
    }

    public String toString() {
      return "PathFilter:" + p;
    }
  }

  /*
   * Prints out the input splits for the specified files
   */
  private void splitRealFiles(String[] args) throws IOException {
    Configuration conf = new Configuration();
    Job job = new Job();
    FileSystem fs = FileSystem.get(conf);
    if (!(fs instanceof DistributedFileSystem)) {
      throw new IOException("Wrong file system: " + fs.getClass().getName());
    }
    int blockSize = conf.getInt("dfs.block.size", 128 * 1024 * 1024);

    DummyInputFormat inFormat = new DummyInputFormat();
    for (int i = 0; i < args.length; i++) {
      FileInputFormat.addInputPaths(job, args[i]);
    }
    inFormat.setMinSplitSizeRack(blockSize);
    inFormat.setMaxSplitSize(10 * blockSize);

    List<InputSplit> splits = inFormat.getSplits(job);
    System.out.println("Total number of splits " + splits.size());
    for (int i = 0; i < splits.size(); ++i) {
      CombineFileSplit fileSplit = (CombineFileSplit) splits.get(i);
      System.out.println("Split[" + i + "] " + fileSplit);
    }
  }

  public static void main(String[] args) throws Exception{

    // if there are some parameters specified, then use those paths
    if (args.length != 0) {
      TestCombineFileInputFormat test = new TestCombineFileInputFormat();
      test.splitRealFiles(args);
    } else {
      TestCombineFileInputFormat test = new TestCombineFileInputFormat();
      test.testSplitPlacement();
    }
  }
}
