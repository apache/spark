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
package org.apache.hadoop.mapreduce.lib.partition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class TestInputSampler {

  static class SequentialSplit extends InputSplit {
    private int i;
    SequentialSplit(int i) {
      this.i = i;
    }
    public long getLength() { return 0; }
    public String[] getLocations() { return new String[0]; }
    public int getInit() { return i; }
  }

  static class TestInputSamplerIF
      extends InputFormat<IntWritable,NullWritable> {

    final int maxDepth;
    final ArrayList<InputSplit> splits = new ArrayList<InputSplit>();

    TestInputSamplerIF(int maxDepth, int numSplits, int... splitInit) {
      this.maxDepth = maxDepth;
      assert splitInit.length == numSplits;
      for (int i = 0; i < numSplits; ++i) {
        splits.add(new SequentialSplit(splitInit[i]));
      }
    }

    public List<InputSplit> getSplits(JobContext context)
        throws IOException, InterruptedException {
      return splits;
    }

    public RecordReader<IntWritable,NullWritable> createRecordReader(
        final InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      return new RecordReader<IntWritable,NullWritable>() {
        private int maxVal;
        private final IntWritable i = new IntWritable();
        public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
          i.set(((SequentialSplit)split).getInit() - 1);
          maxVal = i.get() + maxDepth + 1;
        }
        public boolean nextKeyValue() {
          i.set(i.get() + 1);
          return i.get() < maxVal;
        }
        public IntWritable getCurrentKey() { return i; }
        public NullWritable getCurrentValue() { return NullWritable.get(); }
        public float getProgress() { return 1.0f; }
        public void close() { }
      };
    }

  }

  /**
   * Verify SplitSampler contract, that an equal number of records are taken
   * from the first splits.
   */
  @Test
  @SuppressWarnings("unchecked") // IntWritable comparator not typesafe
  public void testSplitSampler() throws Exception {
    final int TOT_SPLITS = 15;
    final int NUM_SPLITS = 5;
    final int STEP_SAMPLE = 5;
    final int NUM_SAMPLES = NUM_SPLITS * STEP_SAMPLE;
    InputSampler.Sampler<IntWritable,NullWritable> sampler =
      new InputSampler.SplitSampler<IntWritable,NullWritable>(
          NUM_SAMPLES, NUM_SPLITS);
    int inits[] = new int[TOT_SPLITS];
    for (int i = 0; i < TOT_SPLITS; ++i) {
      inits[i] = i * STEP_SAMPLE;
    }
    Job ignored = new Job();
    Object[] samples = sampler.getSample(
        new TestInputSamplerIF(100000, TOT_SPLITS, inits), ignored);
    assertEquals(NUM_SAMPLES, samples.length);
    Arrays.sort(samples, new IntWritable.Comparator());
    for (int i = 0; i < NUM_SAMPLES; ++i) {
      assertEquals(i, ((IntWritable)samples[i]).get());
    }
  }

  /**
   * Verify IntervalSampler contract, that samples are taken at regular
   * intervals from the given splits.
   */
  @Test
  @SuppressWarnings("unchecked") // IntWritable comparator not typesafe
  public void testIntervalSampler() throws Exception {
    final int TOT_SPLITS = 16;
    final int PER_SPLIT_SAMPLE = 4;
    final int NUM_SAMPLES = TOT_SPLITS * PER_SPLIT_SAMPLE;
    final double FREQ = 1.0 / TOT_SPLITS;
    InputSampler.Sampler<IntWritable,NullWritable> sampler =
      new InputSampler.IntervalSampler<IntWritable,NullWritable>(
          FREQ, NUM_SAMPLES);
    int inits[] = new int[TOT_SPLITS];
    for (int i = 0; i < TOT_SPLITS; ++i) {
      inits[i] = i;
    }
    Job ignored = new Job();
    Object[] samples = sampler.getSample(new TestInputSamplerIF(
          NUM_SAMPLES, TOT_SPLITS, inits), ignored);
    assertEquals(NUM_SAMPLES, samples.length);
    Arrays.sort(samples, new IntWritable.Comparator());
    for (int i = 0; i < NUM_SAMPLES; ++i) {
      assertEquals(i, ((IntWritable)samples[i]).get());
    }
  }

}
