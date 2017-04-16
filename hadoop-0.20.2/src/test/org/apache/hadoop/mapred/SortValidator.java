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

import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.*;

/**
 * A set of utilities to validate the <b>sort</b> of the map-reduce framework.
 * This utility program has 2 main parts:
 * 1. Checking the records' statistics
 *   a) Validates the no. of bytes and records in sort's input & output. 
 *   b) Validates the xor of the md5's of each key/value pair.
 *   c) Ensures same key/value is present in both input and output.
 * 2. Check individual records  to ensure each record is present in both
 *    the input and the output of the sort (expensive on large data-sets). 
 *    
 * To run: bin/hadoop jar build/hadoop-examples.jar sortvalidate
 *            [-m <i>maps</i>] [-r <i>reduces</i>] [-deep] 
 *            -sortInput <i>sort-in-dir</i> -sortOutput <i>sort-out-dir</i> 
 */
public class SortValidator extends Configured implements Tool {

  static private final IntWritable sortInput = new IntWritable(1); 
  static private final IntWritable sortOutput = new IntWritable(2); 

  static void printUsage() {
    System.err.println("sortvalidate [-m <maps>] [-r <reduces>] [-deep] " +
                       "-sortInput <sort-input-dir> -sortOutput <sort-output-dir>");
    System.exit(1);
  }

  static private IntWritable deduceInputFile(JobConf job) {
    Path[] inputPaths = FileInputFormat.getInputPaths(job);
    Path inputFile = new Path(job.get("map.input.file"));

    // value == one for sort-input; value == two for sort-output
    return (inputFile.getParent().equals(inputPaths[0])) ? 
        sortInput : sortOutput;
  }
  
  static private byte[] pair(BytesWritable a, BytesWritable b) {
    byte[] pairData = new byte[a.getLength()+ b.getLength()];
    System.arraycopy(a.getBytes(), 0, pairData, 0, a.getLength());
    System.arraycopy(b.getBytes(), 0, pairData, a.getLength(), b.getLength());
    return pairData;
  }

  private static final PathFilter sortPathsFilter = new PathFilter() {
    public boolean accept(Path path) {
      return (path.getName().startsWith("part-"));
    }
  };
  
  /**
   * A simple map-reduce job which checks consistency of the
   * MapReduce framework's sort by checking:
   * a) Records are sorted correctly
   * b) Keys are partitioned correctly
   * c) The input and output have same no. of bytes and records.
   * d) The input and output have the correct 'checksum' by xor'ing 
   *    the md5 of each record.
   *    
   */
  public static class RecordStatsChecker {

    /**
     * Generic way to get <b>raw</b> data from a {@link Writable}.
     */
    static class Raw {
      /**
       * Get raw data bytes from a {@link Writable}
       * @param writable {@link Writable} object from whom to get the raw data
       * @return raw data of the writable
       */
      public byte[] getRawBytes(Writable writable) {
        return writable.toString().getBytes(); 
      } 
      
      /**
       * Get number of raw data bytes of the {@link Writable}
       * @param writable {@link Writable} object from whom to get the raw data
       *                 length
       * @return number of raw data bytes
       */
      public int getRawBytesLength(Writable writable) {
        return writable.toString().getBytes().length; 
      }
    }

    /**
     * Specialization of {@link Raw} for {@link BytesWritable}.
     */
    static class RawBytesWritable extends Raw  {
      public byte[] getRawBytes(Writable bw) {
        return ((BytesWritable)bw).getBytes();
      }
      public int getRawBytesLength(Writable bw) {
        return ((BytesWritable)bw).getLength(); 
      }
    }
    
    /**
     * Specialization of {@link Raw} for {@link Text}.
     */
    static class RawText extends Raw  {
      public byte[] getRawBytes(Writable text) {
        return ((Text)text).getBytes();
      }
      public int getRawBytesLength(Writable text) {
        return ((Text)text).getLength();
      }
    }
    
    private static Raw createRaw(Class rawClass) {
      if (rawClass == Text.class) {
        return new RawText();
      } else if (rawClass == BytesWritable.class) {
        System.err.println("Returning " + RawBytesWritable.class);
        return new RawBytesWritable();
      }      
      return new Raw();
    }

    public static class RecordStatsWritable implements Writable {
      private long bytes = 0;
      private long records = 0;
      private int checksum = 0;
      
      public RecordStatsWritable() {}
      
      public RecordStatsWritable(long bytes, long records, int checksum) {
        this.bytes = bytes;
        this.records = records;
        this.checksum = checksum;
      }
      
      public void write(DataOutput out) throws IOException {
        WritableUtils.writeVLong(out, bytes);
        WritableUtils.writeVLong(out, records);
        WritableUtils.writeVInt(out, checksum);
      }

      public void readFields(DataInput in) throws IOException {
        bytes = WritableUtils.readVLong(in);
        records = WritableUtils.readVLong(in);
        checksum = WritableUtils.readVInt(in);
      }
      
      public long getBytes() { return bytes; }
      public long getRecords() { return records; }
      public int getChecksum() { return checksum; }
    }
    
    public static class Map extends MapReduceBase
      implements Mapper<WritableComparable, Writable,
                        IntWritable, RecordStatsWritable> {
      
      private IntWritable key = null;
      private WritableComparable prevKey = null;
      private Class<? extends WritableComparable> keyClass;
      private Partitioner<WritableComparable, Writable> partitioner = null;
      private int partition = -1;
      private int noSortReducers = -1;
      private long recordId = -1;
      
      private Raw rawKey;
      private Raw rawValue;

      public void configure(JobConf job) {
        // 'key' == sortInput for sort-input; key == sortOutput for sort-output
        key = deduceInputFile(job);
        
        if (key == sortOutput) {
          partitioner = new HashPartitioner<WritableComparable, Writable>();
          
          // Figure the 'current' partition and no. of reduces of the 'sort'
          try {
            URI inputURI = new URI(job.get("map.input.file"));
            String inputFile = inputURI.getPath();
            partition = Integer.valueOf(
                                        inputFile.substring(inputFile.lastIndexOf("part")+5)
                                        ).intValue();
            noSortReducers = job.getInt("sortvalidate.sort.reduce.tasks", -1);
          } catch (Exception e) {
            System.err.println("Caught: " + e);
            System.exit(-1);
          }
        }
      }
      
      @SuppressWarnings("unchecked")
      public void map(WritableComparable key, Writable value,
                      OutputCollector<IntWritable, RecordStatsWritable> output, 
                      Reporter reporter) throws IOException {
        // Set up rawKey and rawValue on the first call to 'map'
        if (recordId == -1) {
         rawKey = createRaw(key.getClass());
         rawValue = createRaw(value.getClass());
        }
        ++recordId;
        
        if (this.key == sortOutput) {
          // Check if keys are 'sorted' if this  
          // record is from sort's output
          if (prevKey == null) {
            prevKey = key;
            keyClass = prevKey.getClass();
          } else {
            // Sanity check
            if (keyClass != key.getClass()) {
              throw new IOException("Type mismatch in key: expected " +
                                    keyClass.getName() + ", recieved " +
                                    key.getClass().getName());
            }
            
            // Check if they were sorted correctly
            if (prevKey.compareTo(key) > 0) {
              throw new IOException("The 'map-reduce' framework wrongly" +
                                    " classifed (" + prevKey + ") > (" + 
                                    key + ") "+ "for record# " + recordId); 
            }
            prevKey = key;
          }

          // Check if the sorted output is 'partitioned' right
          int keyPartition = 
            partitioner.getPartition(key, value, noSortReducers);
          if (partition != keyPartition) {
            throw new IOException("Partitions do not match for record# " + 
                                  recordId + " ! - '" + partition + "' v/s '" + 
                                  keyPartition + "'");
          }
        }

        // Construct the record-stats and output (this.key, record-stats)
        byte[] keyBytes = rawKey.getRawBytes(key);
        int keyBytesLen = rawKey.getRawBytesLength(key);
        byte[] valueBytes = rawValue.getRawBytes(value);
        int valueBytesLen = rawValue.getRawBytesLength(value);
        
        int keyValueChecksum = 
          (WritableComparator.hashBytes(keyBytes, keyBytesLen) ^
           WritableComparator.hashBytes(valueBytes, valueBytesLen));

        output.collect(this.key, 
                       new RecordStatsWritable((keyBytesLen+valueBytesLen),
                       1, keyValueChecksum)
                      );
      }
      
    }
    
    public static class Reduce extends MapReduceBase
      implements Reducer<IntWritable, RecordStatsWritable,
                         IntWritable, RecordStatsWritable> {
      
      public void reduce(IntWritable key, Iterator<RecordStatsWritable> values,
                         OutputCollector<IntWritable,
                                         RecordStatsWritable> output, 
                         Reporter reporter) throws IOException {
        long bytes = 0;
        long records = 0;
        int xor = 0;
        while (values.hasNext()) {
          RecordStatsWritable stats = values.next();
          bytes += stats.getBytes();
          records += stats.getRecords();
          xor ^= stats.getChecksum(); 
        }
        
        output.collect(key, new RecordStatsWritable(bytes, records, xor));
      }
    }
    
    public static class NonSplitableSequenceFileInputFormat 
      extends SequenceFileInputFormat {
      protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
      }
    }
    
    static void checkRecords(Configuration defaults, 
                             Path sortInput, Path sortOutput) throws IOException {
      FileSystem inputfs = sortInput.getFileSystem(defaults);
      FileSystem outputfs = sortOutput.getFileSystem(defaults);
      FileSystem defaultfs = FileSystem.get(defaults);
      JobConf jobConf = new JobConf(defaults, RecordStatsChecker.class);
      jobConf.setJobName("sortvalidate-recordstats-checker");

      int noSortReduceTasks = 
        outputfs.listStatus(sortOutput, sortPathsFilter).length;
      jobConf.setInt("sortvalidate.sort.reduce.tasks", noSortReduceTasks);
      int noSortInputpaths =  inputfs.listStatus(sortInput).length;

      jobConf.setInputFormat(NonSplitableSequenceFileInputFormat.class);
      jobConf.setOutputFormat(SequenceFileOutputFormat.class);
      
      jobConf.setOutputKeyClass(IntWritable.class);
      jobConf.setOutputValueClass(RecordStatsChecker.RecordStatsWritable.class);
      
      jobConf.setMapperClass(Map.class);
      jobConf.setCombinerClass(Reduce.class);
      jobConf.setReducerClass(Reduce.class);
      
      jobConf.setNumMapTasks(noSortReduceTasks);
      jobConf.setNumReduceTasks(1);

      FileInputFormat.setInputPaths(jobConf, sortInput);
      FileInputFormat.addInputPath(jobConf, sortOutput);
      Path outputPath = new Path("/tmp/sortvalidate/recordstatschecker");
      if (defaultfs.exists(outputPath)) {
        defaultfs.delete(outputPath, true);
      }
      FileOutputFormat.setOutputPath(jobConf, outputPath);
      
      // Uncomment to run locally in a single process
      //job_conf.set("mapred.job.tracker", "local");
      Path[] inputPaths = FileInputFormat.getInputPaths(jobConf);
      System.out.println("\nSortValidator.RecordStatsChecker: Validate sort " +
                         "from " + inputPaths[0] + " (" + 
                         noSortInputpaths + " files), " + 
                         inputPaths[1] + " (" + 
                         noSortReduceTasks + 
                         " files) into " + 
                         FileOutputFormat.getOutputPath(jobConf) + 
                         " with 1 reducer.");
      Date startTime = new Date();
      System.out.println("Job started: " + startTime);
      JobClient.runJob(jobConf);
      Date end_time = new Date();
      System.out.println("Job ended: " + end_time);
      System.out.println("The job took " + 
                         (end_time.getTime() - startTime.getTime()) /1000 + " seconds.");
      
      // Check to ensure that the statistics of the 
      // framework's sort-input and sort-output match
      SequenceFile.Reader stats = new SequenceFile.Reader(defaultfs,
                                                          new Path(outputPath, "part-00000"), defaults);
      IntWritable k1 = new IntWritable();
      IntWritable k2 = new IntWritable();
      RecordStatsWritable v1 = new RecordStatsWritable();
      RecordStatsWritable v2 = new RecordStatsWritable();
      if (!stats.next(k1, v1)) {
        throw new IOException("Failed to read record #1 from reduce's output");
      }
      if (!stats.next(k2, v2)) {
        throw new IOException("Failed to read record #2 from reduce's output");
      }

      if ((v1.getBytes() != v2.getBytes()) || (v1.getRecords() != v2.getRecords()) || 
          v1.getChecksum() != v2.getChecksum()) {
        throw new IOException("(" + 
                              v1.getBytes() + ", " + v1.getRecords() + ", " + v1.getChecksum() + ") v/s (" +
                              v2.getBytes() + ", " + v2.getRecords() + ", " + v2.getChecksum() + ")");
      }
    }

  }
  
  /**
   * A simple map-reduce task to check if the input and the output
   * of the framework's sort is consistent by ensuring each record 
   * is present in both the input and the output.
   * 
   */
  public static class RecordChecker {
    
    public static class Map extends MapReduceBase
      implements Mapper<BytesWritable, BytesWritable,
                        BytesWritable, IntWritable> {
      
      private IntWritable value = null;
      
      public void configure(JobConf job) {
        // value == one for sort-input; value == two for sort-output
        value = deduceInputFile(job);
      }
      
      public void map(BytesWritable key, 
                      BytesWritable value,
                      OutputCollector<BytesWritable, IntWritable> output, 
                      Reporter reporter) throws IOException {
        // newKey = (key, value)
        BytesWritable keyValue = new BytesWritable(pair(key, value));
    
        // output (newKey, value)
        output.collect(keyValue, this.value);
      }
    }
    
    public static class Reduce extends MapReduceBase
      implements Reducer<BytesWritable, IntWritable,
                        BytesWritable, IntWritable> {
      
      public void reduce(BytesWritable key, Iterator<IntWritable> values,
                         OutputCollector<BytesWritable, IntWritable> output,
                         Reporter reporter) throws IOException {
        int ones = 0;
        int twos = 0;
        while (values.hasNext()) {
          IntWritable count = values.next(); 
          if (count.equals(sortInput)) {
            ++ones;
          } else if (count.equals(sortOutput)) {
            ++twos;
          } else {
            throw new IOException("Invalid 'value' of " + count.get() + 
                                  " for (key,value): " + key.toString());
          }
        }
        
        // Check to ensure there are equal no. of ones and twos
        if (ones != twos) {
          throw new IOException("Illegal ('one', 'two'): (" + ones + ", " + twos +
                                ") for (key, value): " + key.toString());
        }
      }
    }
    
    static void checkRecords(Configuration defaults, int noMaps, int noReduces,
                             Path sortInput, Path sortOutput) throws IOException {
      JobConf jobConf = new JobConf(defaults, RecordChecker.class);
      jobConf.setJobName("sortvalidate-record-checker");
      
      jobConf.setInputFormat(SequenceFileInputFormat.class);
      jobConf.setOutputFormat(SequenceFileOutputFormat.class);
      
      jobConf.setOutputKeyClass(BytesWritable.class);
      jobConf.setOutputValueClass(IntWritable.class);
      
      jobConf.setMapperClass(Map.class);        
      jobConf.setReducerClass(Reduce.class);
      
      JobClient client = new JobClient(jobConf);
      ClusterStatus cluster = client.getClusterStatus();
      if (noMaps == -1) {
        noMaps = cluster.getTaskTrackers() * 
          jobConf.getInt("test.sortvalidate.maps_per_host", 10);
      }
      if (noReduces == -1) {
        noReduces = (int) (cluster.getMaxReduceTasks() * 0.9);
        String sortReduces = jobConf.get("test.sortvalidate.reduces_per_host");
        if (sortReduces != null) {
           noReduces = cluster.getTaskTrackers() * 
                           Integer.parseInt(sortReduces);
        }
      }
      jobConf.setNumMapTasks(noMaps);
      jobConf.setNumReduceTasks(noReduces);
      
      FileInputFormat.setInputPaths(jobConf, sortInput);
      FileInputFormat.addInputPath(jobConf, sortOutput);
      Path outputPath = new Path("/tmp/sortvalidate/recordchecker");
      FileSystem fs = FileSystem.get(defaults);
      if (fs.exists(outputPath)) {
        fs.delete(outputPath, true);
      }
      FileOutputFormat.setOutputPath(jobConf, outputPath);
      
      // Uncomment to run locally in a single process
      //job_conf.set("mapred.job.tracker", "local");
      Path[] inputPaths = FileInputFormat.getInputPaths(jobConf);
      System.out.println("\nSortValidator.RecordChecker: Running on " +
                         cluster.getTaskTrackers() +
                        " nodes to validate sort from " + 
                         inputPaths[0] + ", " + 
                         inputPaths[1] + " into " + 
                         FileOutputFormat.getOutputPath(jobConf) + 
                         " with " + noReduces + " reduces.");
      Date startTime = new Date();
      System.out.println("Job started: " + startTime);
      JobClient.runJob(jobConf);
      Date end_time = new Date();
      System.out.println("Job ended: " + end_time);
      System.out.println("The job took " + 
                         (end_time.getTime() - startTime.getTime()) /1000 + " seconds.");
    }
  }

  
  /**
   * The main driver for sort-validator program.
   * Invoke this method to submit the map/reduce job.
   * @throws IOException When there is communication problems with the 
   *                     job tracker.
   */
  public int run(String[] args) throws Exception {
    Configuration defaults = getConf();
    
    int noMaps = -1, noReduces = -1;
    Path sortInput = null, sortOutput = null;
    boolean deepTest = false;
    for(int i=0; i < args.length; ++i) {
      try {
        if ("-m".equals(args[i])) {
          noMaps = Integer.parseInt(args[++i]);
        } else if ("-r".equals(args[i])) {
          noReduces = Integer.parseInt(args[++i]);
        } else if ("-sortInput".equals(args[i])){
          sortInput = new Path(args[++i]);
        } else if ("-sortOutput".equals(args[i])){
          sortOutput = new Path(args[++i]);
        } else if ("-deep".equals(args[i])) {
          deepTest = true;
        } else {
          printUsage();
          return -1;
        }
      } catch (NumberFormatException except) {
        System.err.println("ERROR: Integer expected instead of " + args[i]);
        printUsage();
        return -1;
      } catch (ArrayIndexOutOfBoundsException except) {
        System.err.println("ERROR: Required parameter missing from " +
                           args[i-1]);
        printUsage();
        return -1;
      }
    }
    
    // Sanity check
    if (sortInput == null || sortOutput == null) {
      printUsage();
      return -2;
    }

    // Check if the records are consistent and sorted correctly
    RecordStatsChecker.checkRecords(defaults, sortInput, sortOutput);

    // Check if the same records are present in sort's inputs & outputs
    if (deepTest) {
      RecordChecker.checkRecords(defaults, noMaps, noReduces, sortInput, 
                                 sortOutput);
    }
    
    System.out.println("\nSUCCESS! Validated the MapReduce framework's 'sort'" +
                       " successfully.");
    
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new SortValidator(), args);
    System.exit(res);
  }
}
