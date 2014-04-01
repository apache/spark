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

package org.apache.hadoop.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * This is an example Hadoop Map/Reduce application.
 * It reads the text input files that must contain two integers per a line.
 * The output is sorted by the first and second number and grouped on the 
 * first number.
 *
 * To run: bin/hadoop jar build/hadoop-examples.jar secondarysort
 *            <i>in-dir</i> <i>out-dir</i> 
 */
public class SecondarySort {
 
  /**
   * Define a pair of integers that are writable.
   * They are serialized in a byte comparable format.
   */
  public static class IntPair 
                      implements WritableComparable<IntPair> {
    private int first = 0;
    private int second = 0;
    
    /**
     * Set the left and right values.
     */
    public void set(int left, int right) {
      first = left;
      second = right;
    }
    public int getFirst() {
      return first;
    }
    public int getSecond() {
      return second;
    }
    /**
     * Read the two integers. 
     * Encoded as: MIN_VALUE -> 0, 0 -> -MIN_VALUE, MAX_VALUE-> -1
     */
    @Override
    public void readFields(DataInput in) throws IOException {
      first = in.readInt() + Integer.MIN_VALUE;
      second = in.readInt() + Integer.MIN_VALUE;
    }
    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(first - Integer.MIN_VALUE);
      out.writeInt(second - Integer.MIN_VALUE);
    }
    @Override
    public int hashCode() {
      return first * 157 + second;
    }
    @Override
    public boolean equals(Object right) {
      if (right instanceof IntPair) {
        IntPair r = (IntPair) right;
        return r.first == first && r.second == second;
      } else {
        return false;
      }
    }
    /** A Comparator that compares serialized IntPair. */ 
    public static class Comparator extends WritableComparator {
      public Comparator() {
        super(IntPair.class);
      }

      public int compare(byte[] b1, int s1, int l1,
                         byte[] b2, int s2, int l2) {
        return compareBytes(b1, s1, l1, b2, s2, l2);
      }
    }

    static {                                        // register this comparator
      WritableComparator.define(IntPair.class, new Comparator());
    }

    @Override
    public int compareTo(IntPair o) {
      if (first != o.first) {
        return first < o.first ? -1 : 1;
      } else if (second != o.second) {
        return second < o.second ? -1 : 1;
      } else {
        return 0;
      }
    }
  }
  
  /**
   * Partition based on the first part of the pair.
   */
  public static class FirstPartitioner extends Partitioner<IntPair,IntWritable>{
    @Override
    public int getPartition(IntPair key, IntWritable value, 
                            int numPartitions) {
      return Math.abs(key.getFirst() * 127) % numPartitions;
    }
  }

  /**
   * Compare only the first part of the pair, so that reduce is called once
   * for each value of the first part.
   */
  public static class FirstGroupingComparator 
                implements RawComparator<IntPair> {
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return WritableComparator.compareBytes(b1, s1, Integer.SIZE/8, 
                                             b2, s2, Integer.SIZE/8);
    }

    @Override
    public int compare(IntPair o1, IntPair o2) {
      int l = o1.getFirst();
      int r = o2.getFirst();
      return l == r ? 0 : (l < r ? -1 : 1);
    }
  }

  /**
   * Read two integers from each line and generate a key, value pair
   * as ((left, right), right).
   */
  public static class MapClass 
         extends Mapper<LongWritable, Text, IntPair, IntWritable> {
    
    private final IntPair key = new IntPair();
    private final IntWritable value = new IntWritable();
    
    @Override
    public void map(LongWritable inKey, Text inValue, 
                    Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(inValue.toString());
      int left = 0;
      int right = 0;
      if (itr.hasMoreTokens()) {
        left = Integer.parseInt(itr.nextToken());
        if (itr.hasMoreTokens()) {
          right = Integer.parseInt(itr.nextToken());
        }
        key.set(left, right);
        value.set(right);
        context.write(key, value);
      }
    }
  }
  
  /**
   * A reducer class that just emits the sum of the input values.
   */
  public static class Reduce 
         extends Reducer<IntPair, IntWritable, Text, IntWritable> {
    private static final Text SEPARATOR = 
      new Text("------------------------------------------------");
    private final Text first = new Text();
    
    @Override
    public void reduce(IntPair key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      context.write(SEPARATOR, null);
      first.set(Integer.toString(key.getFirst()));
      for(IntWritable value: values) {
        context.write(first, value);
      }
    }
  }
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: secondarysrot <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "secondary sort");
    job.setJarByClass(SecondarySort.class);
    job.setMapperClass(MapClass.class);
    job.setReducerClass(Reduce.class);

    // group and partition by the first int in the pair
    job.setPartitionerClass(FirstPartitioner.class);
    job.setGroupingComparatorClass(FirstGroupingComparator.class);

    // the map output is IntPair, IntWritable
    job.setMapOutputKeyClass(IntPair.class);
    job.setMapOutputValueClass(IntWritable.class);

    // the reduce output is Text, IntWritable
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
