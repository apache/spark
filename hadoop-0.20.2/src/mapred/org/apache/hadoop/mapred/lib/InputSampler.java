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

package org.apache.hadoop.mapred.lib;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Utility for collecting samples and writing a partition file for
 * {@link org.apache.hadoop.mapred.lib.TotalOrderPartitioner}.
 */
public class InputSampler<K,V> implements Tool {

  private static final Log LOG = LogFactory.getLog(InputSampler.class);

  static int printUsage() {
    System.out.println("sampler -r <reduces>\n" +
                       "      [-inFormat <input format class>]\n" +
                       "      [-keyClass <map input & output key class>]\n" +
                       "      [-splitRandom <double pcnt> <numSamples> <maxsplits> | " +
                       "// Sample from random splits at random (general)\n" +
                       "       -splitSample <numSamples> <maxsplits> | " +
                       "             // Sample from first records in splits (random data)\n"+
                       "       -splitInterval <double pcnt> <maxsplits>]" +
                       "             // Sample from splits at intervals (sorted data)");
    System.out.println("Default sampler: -splitRandom 0.1 10000 10");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  private JobConf conf;

  public InputSampler(JobConf conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    if (!(conf instanceof JobConf)) {
      this.conf = new JobConf(conf);
    } else {
      this.conf = (JobConf) conf;
    }
  }

  /**
   * Interface to sample using an {@link org.apache.hadoop.mapred.InputFormat}.
   */
  public interface Sampler<K,V> {
    /**
     * For a given job, collect and return a subset of the keys from the
     * input data.
     */
    K[] getSample(InputFormat<K,V> inf, JobConf job) throws IOException;
  }

  /**
   * Samples the first n records from s splits.
   * Inexpensive way to sample random data.
   */
  public static class SplitSampler<K,V> implements Sampler<K,V> {

    private final int numSamples;
    private final int maxSplitsSampled;

    /**
     * Create a SplitSampler sampling <em>all</em> splits.
     * Takes the first numSamples / numSplits records from each split.
     * @param numSamples Total number of samples to obtain from all selected
     *                   splits.
     */
    public SplitSampler(int numSamples) {
      this(numSamples, Integer.MAX_VALUE);
    }

    /**
     * Create a new SplitSampler.
     * @param numSamples Total number of samples to obtain from all selected
     *                   splits.
     * @param maxSplitsSampled The maximum number of splits to examine.
     */
    public SplitSampler(int numSamples, int maxSplitsSampled) {
      this.numSamples = numSamples;
      this.maxSplitsSampled = maxSplitsSampled;
    }

    /**
     * From each split sampled, take the first numSamples / numSplits records.
     */
    @SuppressWarnings("unchecked") // ArrayList::toArray doesn't preserve type
    public K[] getSample(InputFormat<K,V> inf, JobConf job) throws IOException {
      InputSplit[] splits = inf.getSplits(job, job.getNumMapTasks());
      ArrayList<K> samples = new ArrayList<K>(numSamples);
      int splitsToSample = Math.min(maxSplitsSampled, splits.length);
      int splitStep = splits.length / splitsToSample;
      int samplesPerSplit = numSamples / splitsToSample;
      long records = 0;
      for (int i = 0; i < splitsToSample; ++i) {
        RecordReader<K,V> reader = inf.getRecordReader(splits[i * splitStep],
            job, Reporter.NULL);
        K key = reader.createKey();
        V value = reader.createValue();
        while (reader.next(key, value)) {
          samples.add(key);
          key = reader.createKey();
          ++records;
          if ((i+1) * samplesPerSplit <= records) {
            break;
          }
        }
        reader.close();
      }
      return (K[])samples.toArray();
    }
  }

  /**
   * Sample from random points in the input.
   * General-purpose sampler. Takes numSamples / maxSplitsSampled inputs from
   * each split.
   */
  public static class RandomSampler<K,V> implements Sampler<K,V> {
    private double freq;
    private final int numSamples;
    private final int maxSplitsSampled;

    /**
     * Create a new RandomSampler sampling <em>all</em> splits.
     * This will read every split at the client, which is very expensive.
     * @param freq Probability with which a key will be chosen.
     * @param numSamples Total number of samples to obtain from all selected
     *                   splits.
     */
    public RandomSampler(double freq, int numSamples) {
      this(freq, numSamples, Integer.MAX_VALUE);
    }

    /**
     * Create a new RandomSampler.
     * @param freq Probability with which a key will be chosen.
     * @param numSamples Total number of samples to obtain from all selected
     *                   splits.
     * @param maxSplitsSampled The maximum number of splits to examine.
     */
    public RandomSampler(double freq, int numSamples, int maxSplitsSampled) {
      this.freq = freq;
      this.numSamples = numSamples;
      this.maxSplitsSampled = maxSplitsSampled;
    }

    /**
     * Randomize the split order, then take the specified number of keys from
     * each split sampled, where each key is selected with the specified
     * probability and possibly replaced by a subsequently selected key when
     * the quota of keys from that split is satisfied.
     */
    @SuppressWarnings("unchecked") // ArrayList::toArray doesn't preserve type
    public K[] getSample(InputFormat<K,V> inf, JobConf job) throws IOException {
      InputSplit[] splits = inf.getSplits(job, job.getNumMapTasks());
      ArrayList<K> samples = new ArrayList<K>(numSamples);
      int splitsToSample = Math.min(maxSplitsSampled, splits.length);

      Random r = new Random();
      long seed = r.nextLong();
      r.setSeed(seed);
      LOG.debug("seed: " + seed);
      // shuffle splits
      for (int i = 0; i < splits.length; ++i) {
        InputSplit tmp = splits[i];
        int j = r.nextInt(splits.length);
        splits[i] = splits[j];
        splits[j] = tmp;
      }
      // our target rate is in terms of the maximum number of sample splits,
      // but we accept the possibility of sampling additional splits to hit
      // the target sample keyset
      for (int i = 0; i < splitsToSample ||
                     (i < splits.length && samples.size() < numSamples); ++i) {
        RecordReader<K,V> reader = inf.getRecordReader(splits[i], job,
            Reporter.NULL);
        K key = reader.createKey();
        V value = reader.createValue();
        while (reader.next(key, value)) {
          if (r.nextDouble() <= freq) {
            if (samples.size() < numSamples) {
              samples.add(key);
            } else {
              // When exceeding the maximum number of samples, replace a
              // random element with this one, then adjust the frequency
              // to reflect the possibility of existing elements being
              // pushed out
              int ind = r.nextInt(numSamples);
              if (ind != numSamples) {
                samples.set(ind, key);
              }
              freq *= (numSamples - 1) / (double) numSamples;
            }
            key = reader.createKey();
          }
        }
        reader.close();
      }
      return (K[])samples.toArray();
    }
  }

  /**
   * Sample from s splits at regular intervals.
   * Useful for sorted data.
   */
  public static class IntervalSampler<K,V> implements Sampler<K,V> {
    private final double freq;
    private final int maxSplitsSampled;

    /**
     * Create a new IntervalSampler sampling <em>all</em> splits.
     * @param freq The frequency with which records will be emitted.
     */
    public IntervalSampler(double freq) {
      this(freq, Integer.MAX_VALUE);
    }

    /**
     * Create a new IntervalSampler.
     * @param freq The frequency with which records will be emitted.
     * @param maxSplitsSampled The maximum number of splits to examine.
     * @see #getSample
     */
    public IntervalSampler(double freq, int maxSplitsSampled) {
      this.freq = freq;
      this.maxSplitsSampled = maxSplitsSampled;
    }

    /**
     * For each split sampled, emit when the ratio of the number of records
     * retained to the total record count is less than the specified
     * frequency.
     */
    @SuppressWarnings("unchecked") // ArrayList::toArray doesn't preserve type
    public K[] getSample(InputFormat<K,V> inf, JobConf job) throws IOException {
      InputSplit[] splits = inf.getSplits(job, job.getNumMapTasks());
      ArrayList<K> samples = new ArrayList<K>();
      int splitsToSample = Math.min(maxSplitsSampled, splits.length);
      int splitStep = splits.length / splitsToSample;
      long records = 0;
      long kept = 0;
      for (int i = 0; i < splitsToSample; ++i) {
        RecordReader<K,V> reader = inf.getRecordReader(splits[i * splitStep],
            job, Reporter.NULL);
        K key = reader.createKey();
        V value = reader.createValue();
        while (reader.next(key, value)) {
          ++records;
          if ((double) kept / records < freq) {
            ++kept;
            samples.add(key);
            key = reader.createKey();
          }
        }
        reader.close();
      }
      return (K[])samples.toArray();
    }
  }

  /**
   * Write a partition file for the given job, using the Sampler provided.
   * Queries the sampler for a sample keyset, sorts by the output key
   * comparator, selects the keys for each rank, and writes to the destination
   * returned from {@link
     org.apache.hadoop.mapred.lib.TotalOrderPartitioner#getPartitionFile}.
   */
  @SuppressWarnings("unchecked") // getInputFormat, getOutputKeyComparator
  public static <K,V> void writePartitionFile(JobConf job,
      Sampler<K,V> sampler) throws IOException {
    final InputFormat<K,V> inf = (InputFormat<K,V>) job.getInputFormat();
    int numPartitions = job.getNumReduceTasks();
    K[] samples = sampler.getSample(inf, job);
    LOG.info("Using " + samples.length + " samples");
    RawComparator<K> comparator =
      (RawComparator<K>) job.getOutputKeyComparator();
    Arrays.sort(samples, comparator);
    Path dst = new Path(TotalOrderPartitioner.getPartitionFile(job));
    FileSystem fs = dst.getFileSystem(job);
    if (fs.exists(dst)) {
      fs.delete(dst, false);
    }
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, job, dst,
        job.getMapOutputKeyClass(), NullWritable.class);
    NullWritable nullValue = NullWritable.get();
    float stepSize = samples.length / (float) numPartitions;
    int last = -1;
    for(int i = 1; i < numPartitions; ++i) {
      int k = Math.round(stepSize * i);
      while (last >= k && comparator.compare(samples[last], samples[k]) == 0) {
        ++k;
      }
      writer.append(samples[k], nullValue);
      last = k;
    }
    writer.close();
  }

  /**
   * Driver for InputSampler from the command line.
   * Configures a JobConf instance and calls {@link #writePartitionFile}.
   */
  public int run(String[] args) throws Exception {
    JobConf job = (JobConf) getConf();
    ArrayList<String> otherArgs = new ArrayList<String>();
    Sampler<K,V> sampler = null;
    for(int i=0; i < args.length; ++i) {
      try {
        if ("-r".equals(args[i])) {
          job.setNumReduceTasks(Integer.parseInt(args[++i]));
        } else if ("-inFormat".equals(args[i])) {
          job.setInputFormat(
              Class.forName(args[++i]).asSubclass(InputFormat.class));
        } else if ("-keyClass".equals(args[i])) {
          job.setMapOutputKeyClass(
              Class.forName(args[++i]).asSubclass(WritableComparable.class));
        } else if ("-splitSample".equals(args[i])) {
          int numSamples = Integer.parseInt(args[++i]);
          int maxSplits = Integer.parseInt(args[++i]);
          if (0 >= maxSplits) maxSplits = Integer.MAX_VALUE;
          sampler = new SplitSampler<K,V>(numSamples, maxSplits);
        } else if ("-splitRandom".equals(args[i])) {
          double pcnt = Double.parseDouble(args[++i]);
          int numSamples = Integer.parseInt(args[++i]);
          int maxSplits = Integer.parseInt(args[++i]);
          if (0 >= maxSplits) maxSplits = Integer.MAX_VALUE;
          sampler = new RandomSampler<K,V>(pcnt, numSamples, maxSplits);
        } else if ("-splitInterval".equals(args[i])) {
          double pcnt = Double.parseDouble(args[++i]);
          int maxSplits = Integer.parseInt(args[++i]);
          if (0 >= maxSplits) maxSplits = Integer.MAX_VALUE;
          sampler = new IntervalSampler<K,V>(pcnt, maxSplits);
        } else {
          otherArgs.add(args[i]);
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + args[i]);
        return printUsage();
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from " +
            args[i-1]);
        return printUsage();
      }
    }
    if (job.getNumReduceTasks() <= 1) {
      System.err.println("Sampler requires more than one reducer");
      return printUsage();
    }
    if (otherArgs.size() < 2) {
      System.out.println("ERROR: Wrong number of parameters: ");
      return printUsage();
    }
    if (null == sampler) {
      sampler = new RandomSampler<K,V>(0.1, 10000, 10);
    }

    Path outf = new Path(otherArgs.remove(otherArgs.size() - 1));
    TotalOrderPartitioner.setPartitionFile(job, outf);
    for (String s : otherArgs) {
      FileInputFormat.addInputPath(job, new Path(s));
    }
    InputSampler.<K,V>writePartitionFile(job, sampler);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    JobConf job = new JobConf(InputSampler.class);
    InputSampler<?,?> sampler = new InputSampler(job);
    int res = ToolRunner.run(sampler, args);
    System.exit(res);
  }
}
