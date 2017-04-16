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

import java.io.IOException;
import java.io.File;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.examples.RandomWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.SortValidator.RecordStatsChecker.NonSplitableSequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Distributed threaded map benchmark.
 * <p>
 * This benchmark generates random data per map and tests the performance 
 * of having multiple spills (using multiple threads) over having just one 
 * spill. Following are the parameters that can be specified
 * <li>File size per map.
 * <li>Number of spills per map. 
 * <li>Number of maps per host.
 * <p>
 * Sort is used for benchmarking the performance. 
 */

public class ThreadedMapBenchmark extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(ThreadedMapBenchmark.class);
  private static Path BASE_DIR =
    new Path(System.getProperty("test.build.data", 
                                File.separator + "benchmarks" + File.separator 
                                + "ThreadedMapBenchmark"));
  private static Path INPUT_DIR = new Path(BASE_DIR, "input");
  private static Path OUTPUT_DIR = new Path(BASE_DIR, "output");
  private static final float FACTOR = 2.3f; // io.sort.mb set to 
                                            // (FACTOR * data_size) should 
                                            // result in only 1 spill

  static enum Counters { RECORDS_WRITTEN, BYTES_WRITTEN }
  
  /**
   * Generates random input data of given size with keys and values of given 
   * sizes. By default it generates 128mb input data with 10 byte keys and 10 
   * byte values.
   */
  public static class Map extends MapReduceBase
  implements Mapper<WritableComparable, Writable,
                    BytesWritable, BytesWritable> {
  
  private long numBytesToWrite;
  private int minKeySize;
  private int keySizeRange;
  private int minValueSize;
  private int valueSizeRange;
  private Random random = new Random();
  private BytesWritable randomKey = new BytesWritable();
  private BytesWritable randomValue = new BytesWritable();
  
  private void randomizeBytes(byte[] data, int offset, int length) {
    for(int i = offset + length - 1; i >= offset; --i) {
      data[i] = (byte) random.nextInt(256);
    }
  }
  
  public void map(WritableComparable key, 
                  Writable value,
                  OutputCollector<BytesWritable, BytesWritable> output, 
                  Reporter reporter) throws IOException {
    int itemCount = 0;
    while (numBytesToWrite > 0) {
      int keyLength = minKeySize 
                      + (keySizeRange != 0 
                         ? random.nextInt(keySizeRange) 
                         : 0);
      randomKey.setSize(keyLength);
      randomizeBytes(randomKey.getBytes(), 0, randomKey.getLength());
      int valueLength = minValueSize 
                        + (valueSizeRange != 0 
                           ? random.nextInt(valueSizeRange) 
                           : 0);
      randomValue.setSize(valueLength);
      randomizeBytes(randomValue.getBytes(), 0, randomValue.getLength());
      output.collect(randomKey, randomValue);
      numBytesToWrite -= keyLength + valueLength;
      reporter.incrCounter(Counters.BYTES_WRITTEN, 1);
      reporter.incrCounter(Counters.RECORDS_WRITTEN, 1);
      if (++itemCount % 200 == 0) {
        reporter.setStatus("wrote record " + itemCount + ". " 
                           + numBytesToWrite + " bytes left.");
      }
    }
    reporter.setStatus("done with " + itemCount + " records.");
  }
  
  @Override
  public void configure(JobConf job) {
    numBytesToWrite = job.getLong("test.tmb.bytes_per_map",
                                  128 * 1024 * 1024);
    minKeySize = job.getInt("test.tmb.min_key", 10);
    keySizeRange = job.getInt("test.tmb.max_key", 10) - minKeySize;
    minValueSize = job.getInt("test.tmb.min_value", 10);
    valueSizeRange = job.getInt("test.tmb.max_value", 10) - minValueSize;
  }
}

  /**
   * Generate input data for the benchmark
   */
  public static void generateInputData(int dataSizePerMap, 
                                       int numSpillsPerMap, 
                                       int numMapsPerHost, 
                                       JobConf masterConf) 
  throws Exception { 
    JobConf job = new JobConf(masterConf, ThreadedMapBenchmark.class);
    job.setJobName("threaded-map-benchmark-random-writer");
    job.setJarByClass(ThreadedMapBenchmark.class);
    job.setInputFormat(UtilsForTests.RandomInputFormat.class);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    
    job.setMapperClass(Map.class);
    job.setReducerClass(IdentityReducer.class);
    
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(BytesWritable.class);
    
    JobClient client = new JobClient(job);
    ClusterStatus cluster = client.getClusterStatus();
    long totalDataSize = dataSizePerMap * numMapsPerHost 
                         * cluster.getTaskTrackers();
    job.set("test.tmb.bytes_per_map", 
            String.valueOf(dataSizePerMap * 1024 * 1024));
    job.setNumReduceTasks(0); // none reduce
    job.setNumMapTasks(numMapsPerHost * cluster.getTaskTrackers());
    FileOutputFormat.setOutputPath(job, INPUT_DIR);
    
    FileSystem fs = FileSystem.get(job);
    fs.delete(BASE_DIR, true);
    
    LOG.info("Generating random input for the benchmark");
    LOG.info("Total data : " + totalDataSize + " mb");
    LOG.info("Data per map: " + dataSizePerMap + " mb");
    LOG.info("Number of spills : " + numSpillsPerMap);
    LOG.info("Number of maps per host : " + numMapsPerHost);
    LOG.info("Number of hosts : " + cluster.getTaskTrackers());
    
    JobClient.runJob(job); // generates the input for the benchmark
  }

  /**
   * This is the main routine for launching the benchmark. It generates random 
   * input data. The input is non-splittable. Sort is used for benchmarking. 
   * This benchmark reports the effect of having multiple sort and spill 
   * cycles over a single sort and spill. 
   * 
   * @throws IOException 
   */
  public int run (String[] args) throws Exception {
    LOG.info("Starting the benchmark for threaded spills");
    String version = "ThreadedMapBenchmark.0.0.1";
    System.out.println(version);
    
    String usage = 
      "Usage: threadedmapbenchmark " +
      "[-dataSizePerMap <data size (in mb) per map, default is 128 mb>] " + 
      "[-numSpillsPerMap <number of spills per map, default is 2>] " +
      "[-numMapsPerHost <number of maps per host, default is 1>]";
    
    int dataSizePerMap = 128; // in mb
    int numSpillsPerMap = 2;
    int numMapsPerHost = 1;
    JobConf masterConf = new JobConf(getConf());
    
    for (int i = 0; i < args.length; i++) { // parse command line
      if (args[i].equals("-dataSizePerMap")) {
        dataSizePerMap = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-numSpillsPerMap")) {
        numSpillsPerMap = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-numMapsPerHost")) {
        numMapsPerHost = Integer.parseInt(args[++i]);
      } else {
        System.err.println(usage);
        System.exit(-1);
      }
    }
    
    if (dataSizePerMap <  1 ||  // verify arguments
        numSpillsPerMap < 1 ||
        numMapsPerHost < 1)
      {
        System.err.println(usage);
        System.exit(-1);
      }
    
    FileSystem fs = null;
    try {
      // using random-writer to generate the input data
      generateInputData(dataSizePerMap, numSpillsPerMap, numMapsPerHost, 
                        masterConf);
      
      // configure job for sorting
      JobConf job = new JobConf(masterConf, ThreadedMapBenchmark.class);
      job.setJobName("threaded-map-benchmark-unspilled");
      job.setJarByClass(ThreadedMapBenchmark.class);

      job.setInputFormat(NonSplitableSequenceFileInputFormat.class);
      job.setOutputFormat(SequenceFileOutputFormat.class);
      
      job.setOutputKeyClass(BytesWritable.class);
      job.setOutputValueClass(BytesWritable.class);
      
      job.setMapperClass(IdentityMapper.class);        
      job.setReducerClass(IdentityReducer.class);
      
      FileInputFormat.addInputPath(job, INPUT_DIR);
      FileOutputFormat.setOutputPath(job, OUTPUT_DIR);
      
      JobClient client = new JobClient(job);
      ClusterStatus cluster = client.getClusterStatus();
      job.setNumMapTasks(numMapsPerHost * cluster.getTaskTrackers());
      job.setNumReduceTasks(1);
      
      // set io.sort.mb to avoid spill
      int ioSortMb = (int)Math.ceil(FACTOR * dataSizePerMap);
      job.set("io.sort.mb", String.valueOf(ioSortMb));
      fs = FileSystem.get(job);
      
      LOG.info("Running sort with 1 spill per map");
      long startTime = System.currentTimeMillis();
      JobClient.runJob(job);
      long endTime = System.currentTimeMillis();
      
      LOG.info("Total time taken : " + String.valueOf(endTime - startTime) 
               + " millisec");
      fs.delete(OUTPUT_DIR, true);
      
      // set io.sort.mb to have multiple spills
      JobConf spilledJob = new JobConf(job, ThreadedMapBenchmark.class);
      ioSortMb = (int)Math.ceil(FACTOR 
                                * Math.ceil((double)dataSizePerMap 
                                            / numSpillsPerMap));
      spilledJob.set("io.sort.mb", String.valueOf(ioSortMb));
      spilledJob.setJobName("threaded-map-benchmark-spilled");
      spilledJob.setJarByClass(ThreadedMapBenchmark.class);
      
      LOG.info("Running sort with " + numSpillsPerMap + " spills per map");
      startTime = System.currentTimeMillis();
      JobClient.runJob(spilledJob);
      endTime = System.currentTimeMillis();
      
      LOG.info("Total time taken : " + String.valueOf(endTime - startTime) 
               + " millisec");
    } finally {
      if (fs != null) {
        fs.delete(BASE_DIR, true);
      }
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new ThreadedMapBenchmark(), args);
    System.exit(res);
  }
}
