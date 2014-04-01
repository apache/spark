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
package org.apache.hadoop.mapred;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.BooleanWritable.Comparator;
import junit.framework.TestCase;
import java.io.*;
import java.util.*;

/**
 * Two different types of comparators can be used in MapReduce. One is used
 * during the Map and Reduce phases, to sort/merge key-value pairs. Another
 * is used to group values for a particular key, when calling the user's 
 * reducer. A user can override both of these two. 
 * This class has tests for making sure we use the right comparators at the 
 * right places. See Hadoop issues 485 and 1535. Our tests: 
 * 1. Test that the same comparator is used for all sort/merge operations 
 * during the Map and Reduce phases.  
 * 2. Test the common use case where values are grouped by keys but values 
 * within each key are grouped by a secondary key (a timestamp, for example). 
 */
public class TestComparators extends TestCase 
{
  JobConf conf = new JobConf(TestMapOutputType.class);
  JobClient jc;
  static Random rng = new Random();

  /** 
   * RandomGen is a mapper that generates 5 random values for each key
   * in the input. The values are in the range [0-4]. The mapper also
   * generates a composite key. If the input key is x and the generated
   * value is y, the composite key is x0y (x-zero-y). Therefore, the inter-
   * mediate key value pairs are ordered by {input key, value}.
   * Think of the random value as a timestamp associated with the record. 
   */
  static class RandomGenMapper
    implements Mapper<IntWritable, Writable, IntWritable, IntWritable> {
    
    public void configure(JobConf job) {
    }
    
    public void map(IntWritable key, Writable value,
                    OutputCollector<IntWritable, IntWritable> out,
                    Reporter reporter) throws IOException {
      int num_values = 5;
      for(int i = 0; i < num_values; ++i) {
        int val = rng.nextInt(num_values);
        int compositeKey = key.get() * 100 + val;
        out.collect(new IntWritable(compositeKey), new IntWritable(val));
      }
    }
    
    public void close() {
    }
  }
  
  /** 
   * Your basic identity mapper. 
   */
  static class IdentityMapper
    implements Mapper<WritableComparable, Writable,
                      WritableComparable, Writable> {
    
    public void configure(JobConf job) {
    }
    
    public void map(WritableComparable key, Writable value,
                    OutputCollector<WritableComparable, Writable> out,
                    Reporter reporter) throws IOException {
      out.collect(key, value);
    }
    
    public void close() {
    }
  }
  
  /** 
   * Checks whether keys are in ascending order.  
   */
  static class AscendingKeysReducer
    implements Reducer<IntWritable, Writable, IntWritable, Text> {
    
    public void configure(JobConf job) {}

    // keep track of the last key we've seen
    private int lastKey = Integer.MIN_VALUE;
    public void reduce(IntWritable key, Iterator<Writable> values, 
                       OutputCollector<IntWritable, Text> out,
                       Reporter reporter) throws IOException {
      int currentKey = key.get();
      // keys should be in ascending order
      if (currentKey < lastKey) {
        fail("Keys not in sorted ascending order");
      }
      lastKey = currentKey;
      out.collect(key, new Text("success"));
    }
    
    public void close() {}
  }
  
  /** 
   * Checks whether keys are in ascending order.  
   */
  static class DescendingKeysReducer
    implements Reducer<IntWritable, Writable, IntWritable, Text> {
    public void configure(JobConf job) {}

    // keep track of the last key we've seen
    private int lastKey = Integer.MAX_VALUE;
    public void reduce(IntWritable key, Iterator<Writable> values, 
                       OutputCollector<IntWritable, Text> out,
                       Reporter reporter) throws IOException {
      int currentKey = ((IntWritable)(key)).get();
      // keys should be in descending order
      if (currentKey > lastKey) {
        fail("Keys not in sorted descending order");
      }
      lastKey = currentKey;
      out.collect(key, new Text("success"));
    }
    
    public void close() {}
  }
  
  /** The reducer checks whether the input values are in ascending order and
   * whether they are correctly grouped by key (i.e. each call to reduce
   * should have 5 values if the grouping is correct). It also checks whether
   * the keys themselves are in ascending order.
   */
  static class AscendingGroupReducer
    implements Reducer<IntWritable, IntWritable, IntWritable, Text> {
    
    public void configure(JobConf job) {
    }

    // keep track of the last key we've seen
    private int lastKey = Integer.MIN_VALUE;
    public void reduce(IntWritable key,
                       Iterator<IntWritable> values,
                       OutputCollector<IntWritable, Text> out,
                       Reporter reporter) throws IOException {
      // check key order
      int currentKey = key.get();
      if (currentKey < lastKey) {
        fail("Keys not in sorted ascending order");
      }
      lastKey = currentKey;
      // check order of values
      IntWritable previous = new IntWritable(Integer.MIN_VALUE);
      int valueCount = 0;
      while (values.hasNext()) {
        IntWritable current = values.next();
        
        // Check that the values are sorted
        if (current.compareTo(previous) < 0)
          fail("Values generated by Mapper not in order");
        previous = current;
        ++valueCount;
      }
      if (valueCount != 5) {
        fail("Values not grouped by primary key");
      }
      out.collect(key, new Text("success"));
    }

    public void close() {
    }
  }
  
  /** The reducer checks whether the input values are in descending order and
   * whether they are correctly grouped by key (i.e. each call to reduce
   * should have 5 values if the grouping is correct). 
   */
  static class DescendingGroupReducer
    implements Reducer<IntWritable, IntWritable, IntWritable, Text> {
    
    public void configure(JobConf job) {
    }

    // keep track of the last key we've seen
    private int lastKey = Integer.MAX_VALUE;
    public void reduce(IntWritable key,
                       Iterator<IntWritable> values,
                       OutputCollector<IntWritable, Text> out,
                       Reporter reporter) throws IOException {
      // check key order
      int currentKey = key.get();
      if (currentKey > lastKey) {
        fail("Keys not in sorted descending order");
      }
      lastKey = currentKey;
      // check order of values
      IntWritable previous = new IntWritable(Integer.MAX_VALUE);
      int valueCount = 0;
      while (values.hasNext()) {
        IntWritable current = values.next();
        
        // Check that the values are sorted
        if (current.compareTo(previous) > 0)
          fail("Values generated by Mapper not in order");
        previous = current;
        ++valueCount;
      }
      if (valueCount != 5) {
        fail("Values not grouped by primary key");
      }
      out.collect(key, new Text("success"));
    }

    public void close() {
    }
  }
  
  /** 
   * A decreasing Comparator for IntWritable 
   */ 
  public static class DecreasingIntComparator extends IntWritable.Comparator {
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return -super.compare(b1, s1, l1, b2, s2, l2);
    }
    static {                    // register this comparator
      WritableComparator.define(DecreasingIntComparator.class, new Comparator());
    }
  }

  /** Grouping function for values based on the composite key. This
   * comparator strips off the secondary key part from the x0y composite
   * and only compares the primary key value (x).
   */
  public static class CompositeIntGroupFn extends WritableComparator {
    public CompositeIntGroupFn() {
      super(IntWritable.class);
    }
    public int compare (WritableComparable v1, WritableComparable v2) {
      int val1 = ((IntWritable)(v1)).get() / 100;
      int val2 = ((IntWritable)(v2)).get() / 100;
      if (val1 < val2)
        return 1;
      else if (val1 > val2)
        return -1;
      else
        return 0;
    }
    
    public boolean equals (IntWritable v1, IntWritable v2) {
      int val1 = v1.get();
      int val2 = v2.get();
      
      return (val1/100) == (val2/100);
    }
    
    static {
      WritableComparator.define(CompositeIntGroupFn.class, new Comparator());
    }
  }

  /** Reverse grouping function for values based on the composite key. 
   */
  public static class CompositeIntReverseGroupFn extends CompositeIntGroupFn {
    public int compare (WritableComparable v1, WritableComparable v2) {
      return -super.compare(v1, v2);
    }
    
    public boolean equals (IntWritable v1, IntWritable v2) {
      return !(super.equals(v1, v2));
    }
    
    static {
      WritableComparator.define(CompositeIntReverseGroupFn.class, new Comparator());
    }
  }


  public void configure() throws Exception {
    Path testdir = new Path("build/test/test.mapred.spill");
    Path inDir = new Path(testdir, "in");
    Path outDir = new Path(testdir, "out");
    FileSystem fs = FileSystem.get(conf);
    fs.delete(testdir, true);
    conf.setInputFormat(SequenceFileInputFormat.class);
    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(Text.class);
    conf.setMapOutputValueClass(IntWritable.class);
    // set up two map jobs, so we can test merge phase in Reduce also
    conf.setNumMapTasks(2);
    
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    if (!fs.mkdirs(testdir)) {
      throw new IOException("Mkdirs failed to create " + testdir.toString());
    }
    if (!fs.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
    // set up input data in 2 files 
    Path inFile = new Path(inDir, "part0");
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, inFile, 
        IntWritable.class, IntWritable.class);
    writer.append(new IntWritable(11), new IntWritable(999));
    writer.append(new IntWritable(23), new IntWritable(456));
    writer.append(new IntWritable(10), new IntWritable(780));
    writer.close();
    inFile = new Path(inDir, "part1");
    writer = SequenceFile.createWriter(fs, conf, inFile, 
        IntWritable.class, IntWritable.class);
    writer.append(new IntWritable(45), new IntWritable(100));
    writer.append(new IntWritable(18), new IntWritable(200));
    writer.append(new IntWritable(27), new IntWritable(300));
    writer.close();
    
    jc = new JobClient(conf);
  }
  
  /**
   * Test the default comparator for Map/Reduce. 
   * Use the identity mapper and see if the keys are sorted at the end
   * @throws Exception
   */
  public void testDefaultMRComparator() throws Exception { 
    configure();
    conf.setMapperClass(IdentityMapper.class);
    conf.setReducerClass(AscendingKeysReducer.class);
    
    RunningJob r_job = jc.submitJob(conf);
    while (!r_job.isComplete()) {
      Thread.sleep(1000);
    }
    
    if (!r_job.isSuccessful()) {
      fail("Oops! The job broke due to an unexpected error");
    }
  }
  
  /**
   * Test user-defined comparator for Map/Reduce.
   * We provide our own comparator that is the reverse of the default int 
   * comparator. Keys should be sorted in reverse order in the reducer. 
   * @throws Exception
   */
  public void testUserMRComparator() throws Exception { 
    configure();
    conf.setMapperClass(IdentityMapper.class);
    conf.setReducerClass(DescendingKeysReducer.class);
    conf.setOutputKeyComparatorClass(DecreasingIntComparator.class);
    
    RunningJob r_job = jc.submitJob(conf);
    while (!r_job.isComplete()) {
      Thread.sleep(1000);
    }
    
    if (!r_job.isSuccessful()) {
      fail("Oops! The job broke due to an unexpected error");
    }
  }
  
  /**
   * Test user-defined grouping comparator for grouping values in Reduce.
   * We generate composite keys that contain a random number, which acts
   * as a timestamp associated with the record. In our Reduce function, 
   * values for a key should be sorted by the 'timestamp'. 
   * @throws Exception
   */
  public void testUserValueGroupingComparator() throws Exception { 
    configure();
    conf.setMapperClass(RandomGenMapper.class);
    conf.setReducerClass(AscendingGroupReducer.class);
    conf.setOutputValueGroupingComparator(CompositeIntGroupFn.class);
    
    RunningJob r_job = jc.submitJob(conf);
    while (!r_job.isComplete()) {
      Thread.sleep(1000);
    }
    
    if (!r_job.isSuccessful()) {
      fail("Oops! The job broke due to an unexpected error");
    }
  }
  
  /**
   * Test all user comparators. Super-test of all tests here. 
   * We generate composite keys that contain a random number, which acts
   * as a timestamp associated with the record. In our Reduce function, 
   * values for a key should be sorted by the 'timestamp'.
   * We also provide our own comparators that reverse the default sorting 
   * order. This lets us make sure that the right comparators are used. 
   * @throws Exception
   */
  public void testAllUserComparators() throws Exception { 
    configure();
    conf.setMapperClass(RandomGenMapper.class);
    // use a decreasing comparator so keys are sorted in reverse order
    conf.setOutputKeyComparatorClass(DecreasingIntComparator.class);
    conf.setReducerClass(DescendingGroupReducer.class);
    conf.setOutputValueGroupingComparator(CompositeIntReverseGroupFn.class);
    RunningJob r_job = jc.submitJob(conf);
    while (!r_job.isComplete()) {
      Thread.sleep(1000);
    }
    
    if (!r_job.isSuccessful()) {
      fail("Oops! The job broke due to an unexpected error");
    }
  }

  /**
   * Test a user comparator that relies on deserializing both arguments
   * for each compare.
   */
  public void testBakedUserComparator() throws Exception {
    MyWritable a = new MyWritable(8, 8);
    MyWritable b = new MyWritable(7, 9);
    assertTrue(a.compareTo(b) > 0);
    assertTrue(WritableComparator.get(MyWritable.class).compare(a, b) < 0);
  }

  public static class MyWritable implements WritableComparable<MyWritable> {
    int i, j;
    public MyWritable() { }
    public MyWritable(int i, int j) {
      this.i = i;
      this.j = j;
    }
    public void readFields(DataInput in) throws IOException {
      i = in.readInt();
      j = in.readInt();
    }
    public void write(DataOutput out) throws IOException {
      out.writeInt(i);
      out.writeInt(j);
    }
    public int compareTo(MyWritable b) {
      return this.i - b.i;
    }
    static {
      WritableComparator.define(MyWritable.class, new MyCmp());
    }
  }

  public static class MyCmp extends WritableComparator {
    public MyCmp() { super(MyWritable.class, true); }
    public int compare(WritableComparable a, WritableComparable b) {
      MyWritable aa = (MyWritable)a;
      MyWritable bb = (MyWritable)b;
      return aa.j - bb.j;
    }
  }

}
