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

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import junit.framework.TestCase;

import static org.apache.hadoop.mapred.Task.Counter.SPILLED_RECORDS;
import static org.apache.hadoop.mapred.Task.Counter.MAP_INPUT_RECORDS;
import static org.apache.hadoop.mapred.Task.Counter.MAP_OUTPUT_RECORDS;
import static org.apache.hadoop.mapred.Task.Counter.COMMITTED_HEAP_BYTES;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This is an wordcount application that tests job counters.
 * It generates simple text input files. Then
 * runs the wordcount map/reduce application on (1) 3 i/p files(with 3 maps
 * and 1 reduce) and verifies the counters and (2) 4 i/p files(with 4 maps
 * and 1 reduce) and verifies counters. Wordcount application reads the
 * text input files, breaks each line into words and counts them. The output
 * is a locally sorted list of words and the count of how often they occurred.
 *
 */
public class TestJobCounters extends TestCase {

  String TEST_ROOT_DIR = new Path(System.getProperty("test.build.data",
                          File.separator + "tmp")).toString().replace(' ', '+');
 
  private void validateMapredCounters(Counters counter, long spillRecCnt, 
                                long mapInputRecords, long mapOutputRecords) {
    // Check if the numer of Spilled Records is same as expected
    assertEquals(spillRecCnt,
      counter.findCounter(SPILLED_RECORDS).getCounter());
    assertEquals(mapInputRecords,
      counter.findCounter(MAP_INPUT_RECORDS).getCounter());
    assertEquals(mapOutputRecords, 
      counter.findCounter(MAP_OUTPUT_RECORDS).getCounter());
  }

  private void validateCounters(org.apache.hadoop.mapreduce.Counters counter, 
                                long spillRecCnt, 
                                long mapInputRecords, long mapOutputRecords) {
    // Check if the numer of Spilled Records is same as expected
    assertEquals(spillRecCnt,
      counter.findCounter(SPILLED_RECORDS).getValue());
    assertEquals(mapInputRecords,
      counter.findCounter(MAP_INPUT_RECORDS).getValue());
    assertEquals(mapOutputRecords, 
      counter.findCounter(MAP_OUTPUT_RECORDS).getValue());
  }
  
  private void createWordsFile(File inpFile) throws Exception {
    Writer out = new BufferedWriter(new FileWriter(inpFile));
    try {
      // 500*4 unique words --- repeated 5 times => 5*2K words
      int REPLICAS=5, NUMLINES=500, NUMWORDSPERLINE=4;

      for (int i = 0; i < REPLICAS; i++) {
        for (int j = 1; j <= NUMLINES*NUMWORDSPERLINE; j+=NUMWORDSPERLINE) {
          out.write("word" + j + " word" + (j+1) + " word" + (j+2) 
                    + " word" + (j+3) + '\n');
        }
      }
    } finally {
      out.close();
    }
  }


  /**
   * The main driver for word count map/reduce program.
   * Invoke this method to submit the map/reduce job.
   * @throws IOException When there is communication problems with the
   *                     job tracker.
   */
  public void testOldJobWithMapAndReducers() throws Exception {
    JobConf conf = new JobConf(TestJobCounters.class);
    conf.setJobName("wordcount-map-reducers");

    // the keys are words (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are counts (ints)
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(WordCount.MapClass.class);
    conf.setCombinerClass(WordCount.Reduce.class);
    conf.setReducerClass(WordCount.Reduce.class);

    conf.setNumMapTasks(3);
    conf.setNumReduceTasks(1);
    conf.setInt("io.sort.mb", 1);
    conf.setInt("io.sort.factor", 2);
    conf.set("io.sort.record.percent", "0.05");
    conf.set("io.sort.spill.percent", "0.80");

    FileSystem fs = FileSystem.get(conf);
    Path testDir = new Path(TEST_ROOT_DIR, "countertest");
    conf.set("test.build.data", testDir.toString());
    try {
      if (fs.exists(testDir)) {
        fs.delete(testDir, true);
      }
      if (!fs.mkdirs(testDir)) {
        throw new IOException("Mkdirs failed to create " + testDir.toString());
      }

      String inDir = testDir +  File.separator + "genins" + File.separator;
      String outDir = testDir + File.separator;
      Path wordsIns = new Path(inDir);
      if (!fs.mkdirs(wordsIns)) {
        throw new IOException("Mkdirs failed to create " + wordsIns.toString());
      }

      //create 3 input files each with 5*2k words
      File inpFile = new File(inDir + "input5_2k_1");
      createWordsFile(inpFile);
      inpFile = new File(inDir + "input5_2k_2");
      createWordsFile(inpFile);
      inpFile = new File(inDir + "input5_2k_3");
      createWordsFile(inpFile);

      FileInputFormat.setInputPaths(conf, inDir);
      Path outputPath1 = new Path(outDir, "output5_2k_3");
      FileOutputFormat.setOutputPath(conf, outputPath1);

      RunningJob myJob = JobClient.runJob(conf);
      Counters c1 = myJob.getCounters();
      // 3maps & in each map, 4 first level spills --- So total 12.
      // spilled records count:
      // Each Map: 1st level:2k+2k+2k+2k=8k;2ndlevel=4k+4k=8k;
      //           3rd level=2k(4k from 1st level & 4k from 2nd level & combineAndSpill)
      //           So total 8k+8k+2k=18k
      // For 3 Maps, total = 3*18=54k
      // Reduce: each of the 3 map o/p's(2k each) will be spilled in shuffleToDisk()
      //         So 3*2k=6k in 1st level; 2nd level:4k(2k+2k);
      //         3rd level directly given to reduce(4k+2k --- combineAndSpill => 2k.
      //         So 0 records spilled to disk in 3rd level)
      //         So total of 6k+4k=10k
      // Total job counter will be 54k+10k = 64k
      
      //3 maps and 2.5k lines --- So total 7.5k map input records
      //3 maps and 10k words in each --- So total of 30k map output recs
      validateMapredCounters(c1, 64000, 7500, 30000);

      //create 4th input file each with 5*2k words and test with 4 maps
      inpFile = new File(inDir + "input5_2k_4");
      createWordsFile(inpFile);
      conf.setNumMapTasks(4);
      Path outputPath2 = new Path(outDir, "output5_2k_4");
      FileOutputFormat.setOutputPath(conf, outputPath2);

      myJob = JobClient.runJob(conf);
      c1 = myJob.getCounters();
      // 4maps & in each map 4 first level spills --- So total 16.
      // spilled records count:
      // Each Map: 1st level:2k+2k+2k+2k=8k;2ndlevel=4k+4k=8k;
      //           3rd level=2k(4k from 1st level & 4k from 2nd level & combineAndSpill)
      //           So total 8k+8k+2k=18k
      // For 3 Maps, total = 4*18=72k
      // Reduce: each of the 4 map o/p's(2k each) will be spilled in shuffleToDisk()
      //         So 4*2k=8k in 1st level; 2nd level:4k+4k=8k;
      //         3rd level directly given to reduce(4k+4k --- combineAndSpill => 2k.
      //         So 0 records spilled to disk in 3rd level)
      //         So total of 8k+8k=16k
      // Total job counter will be 72k+16k = 88k
      
      // 4 maps and 2.5k words in each --- So 10k map input records
      // 4 maps and 10k unique words --- So 40k map output records
      validateMapredCounters(c1, 88000, 10000, 40000);
      
      // check for a map only job
      conf.setNumReduceTasks(0);
      Path outputPath3 = new Path(outDir, "output5_2k_5");
      FileOutputFormat.setOutputPath(conf, outputPath3);

      myJob = JobClient.runJob(conf);
      c1 = myJob.getCounters();
      // 4 maps and 2.5k words in each --- So 10k map input records
      // 4 maps and 10k unique words --- So 40k map output records
      validateMapredCounters(c1, 0, 10000, 40000);
    } finally {
      //clean up the input and output files
      if (fs.exists(testDir)) {
        fs.delete(testDir, true);
      }
    }
  }
  
  public static class NewMapTokenizer 
  extends Mapper<Object, Text, Text, IntWritable> {
 private final static IntWritable one = new IntWritable(1);
 private Text word = new Text();

 public void map(Object key, Text value, Context context) 
 throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }
  
  public static class NewIdentityReducer  
  extends Reducer<Text, IntWritable, Text, IntWritable> {
  private IntWritable result = new IntWritable();
  
  public void reduce(Text key, Iterable<IntWritable> values, 
                     Context context) throws IOException, InterruptedException {
    int sum = 0;
    for (IntWritable val : values) {
      sum += val.get();
    }
    result.set(sum);
    context.write(key, result);
  }
 }
  
  /**
   * The main driver for word count map/reduce program.
   * Invoke this method to submit the map/reduce job.
   * @throws IOException When there is communication problems with the
   *                     job tracker.
   */
  public void testNewJobWithMapAndReducers() throws Exception {
    JobConf conf = new JobConf(TestJobCounters.class);
    conf.setInt("io.sort.mb", 1);
    conf.setInt("io.sort.factor", 2);
    conf.set("io.sort.record.percent", "0.05");
    conf.set("io.sort.spill.percent", "0.80");

    FileSystem fs = FileSystem.get(conf);
    Path testDir = new Path(TEST_ROOT_DIR, "countertest2");
    conf.set("test.build.data", testDir.toString());
    try {
      if (fs.exists(testDir)) {
        fs.delete(testDir, true);
      }
      if (!fs.mkdirs(testDir)) {
        throw new IOException("Mkdirs failed to create " + testDir.toString());
      }

      String inDir = testDir +  File.separator + "genins" + File.separator;
      Path wordsIns = new Path(inDir);
      if (!fs.mkdirs(wordsIns)) {
        throw new IOException("Mkdirs failed to create " + wordsIns.toString());
      }
      String outDir = testDir + File.separator;

      //create 3 input files each with 5*2k words
      File inpFile = new File(inDir + "input5_2k_1");
      createWordsFile(inpFile);
      inpFile = new File(inDir + "input5_2k_2");
      createWordsFile(inpFile);
      inpFile = new File(inDir + "input5_2k_3");
      createWordsFile(inpFile);

      FileInputFormat.setInputPaths(conf, inDir);
      Path outputPath1 = new Path(outDir, "output5_2k_3");
      FileOutputFormat.setOutputPath(conf, outputPath1);
      
      Job job = new Job(conf);
      job.setJobName("wordcount-map-reducers");

      // the keys are words (strings)
      job.setOutputKeyClass(Text.class);
      // the values are counts (ints)
      job.setOutputValueClass(IntWritable.class);

      job.setMapperClass(NewMapTokenizer.class);
      job.setCombinerClass(NewIdentityReducer.class);
      job.setReducerClass(NewIdentityReducer.class);

      job.setNumReduceTasks(1);

      job.waitForCompletion(false);
      
      org.apache.hadoop.mapreduce.Counters c1 = job.getCounters();
      // 3maps & in each map, 4 first level spills --- So total 12.
      // spilled records count:
      // Each Map: 1st level:2k+2k+2k+2k=8k;2ndlevel=4k+4k=8k;
      //           3rd level=2k(4k from 1st level & 4k from 2nd level & combineAndSpill)
      //           So total 8k+8k+2k=18k
      // For 3 Maps, total = 3*18=54k
      // Reduce: each of the 3 map o/p's(2k each) will be spilled in shuffleToDisk()
      //         So 3*2k=6k in 1st level; 2nd level:4k(2k+2k);
      //         3rd level directly given to reduce(4k+2k --- combineAndSpill => 2k.
      //         So 0 records spilled to disk in 3rd level)
      //         So total of 6k+4k=10k
      // Total job counter will be 54k+10k = 64k
      
      //3 maps and 2.5k lines --- So total 7.5k map input records
      //3 maps and 10k words in each --- So total of 30k map output recs
      validateCounters(c1, 64000, 7500, 30000);

      //create 4th input file each with 5*2k words and test with 4 maps
      inpFile = new File(inDir + "input5_2k_4");
      createWordsFile(inpFile);
      JobConf newJobConf = new JobConf(job.getConfiguration());
      
      Path outputPath2 = new Path(outDir, "output5_2k_4");
      
      FileOutputFormat.setOutputPath(newJobConf, outputPath2);

      Job newJob = new Job(newJobConf);
      newJob.waitForCompletion(false);
      c1 = newJob.getCounters();
      // 4maps & in each map 4 first level spills --- So total 16.
      // spilled records count:
      // Each Map: 1st level:2k+2k+2k+2k=8k;2ndlevel=4k+4k=8k;
      //           3rd level=2k(4k from 1st level & 4k from 2nd level & combineAndSpill)
      //           So total 8k+8k+2k=18k
      // For 3 Maps, total = 4*18=72k
      // Reduce: each of the 4 map o/p's(2k each) will be spilled in shuffleToDisk()
      //         So 4*2k=8k in 1st level; 2nd level:4k+4k=8k;
      //         3rd level directly given to reduce(4k+4k --- combineAndSpill => 2k.
      //         So 0 records spilled to disk in 3rd level)
      //         So total of 8k+8k=16k
      // Total job counter will be 72k+16k = 88k
      
      // 4 maps and 2.5k words in each --- So 10k map input records
      // 4 maps and 10k unique words --- So 40k map output records
      validateCounters(c1, 88000, 10000, 40000);
      
      JobConf newJobConf2 = new JobConf(newJob.getConfiguration());
      
      Path outputPath3 = new Path(outDir, "output5_2k_5");
      
      FileOutputFormat.setOutputPath(newJobConf2, outputPath3);

      Job newJob2 = new Job(newJobConf2);
      newJob2.setNumReduceTasks(0);
      newJob2.waitForCompletion(false);
      c1 = newJob2.getCounters();
      // 4 maps and 2.5k words in each --- So 10k map input records
      // 4 maps and 10k unique words --- So 40k map output records
      validateCounters(c1, 0, 10000, 40000);
    } finally {
      //clean up the input and output files
      if (fs.exists(testDir)) {
        fs.delete(testDir, true);
      }
    }
  }
  
  /** 
   * Increases the JVM's heap usage to the specified target value.
   */
  static class MemoryLoader {
    private static final int DEFAULT_UNIT_LOAD_SIZE = 10 * 1024 * 1024; // 10mb
    
    // the target value to reach
    private long targetValue;
    // a list to hold the load objects
    private List<String> loadObjects = new ArrayList<String>();
    
    MemoryLoader(long targetValue) {
      this.targetValue = targetValue;
    }
    
    /**
     * Loads the memory to the target value.
     */
    void load() {
      while (Runtime.getRuntime().totalMemory() < targetValue) {
        System.out.println("Loading memory with " + DEFAULT_UNIT_LOAD_SIZE 
                           + " characters. Current usage : " 
                           + Runtime.getRuntime().totalMemory());
        // load some objects in the memory
        loadObjects.add(RandomStringUtils.random(DEFAULT_UNIT_LOAD_SIZE));

        // sleep for 100ms
        try {
          Thread.sleep(100);
        } catch (InterruptedException ie) {}
      }
    }
  }

  /**
   * A mapper that increases the JVM's heap usage to a target value configured 
   * via {@link MemoryLoaderMapper#TARGET_VALUE} using a {@link MemoryLoader}.
   */
  @SuppressWarnings({"deprecation", "unchecked"})
  static class MemoryLoaderMapper 
  extends MapReduceBase 
  implements org.apache.hadoop.mapred.Mapper<WritableComparable, Writable, 
                    WritableComparable, Writable> {
    static final String TARGET_VALUE = "map.memory-loader.target-value";
    
    private static MemoryLoader loader = null;
    
    public void map(WritableComparable key, Writable val, 
                    OutputCollector<WritableComparable, Writable> output,
                    Reporter reporter)
    throws IOException {
      assertNotNull("Mapper not configured!", loader);
      
      // load the memory
      loader.load();
      
      // work as identity mapper
      output.collect(key, val);
    }

    public void configure(JobConf conf) {
      loader = new MemoryLoader(conf.getLong(TARGET_VALUE, -1));
    }
  }

  /** 
   * A reducer that increases the JVM's heap usage to a target value configured 
   * via {@link MemoryLoaderReducer#TARGET_VALUE} using a {@link MemoryLoader}.
   */
  @SuppressWarnings({"deprecation", "unchecked"})
  static class MemoryLoaderReducer extends MapReduceBase 
  implements org.apache.hadoop.mapred.Reducer<WritableComparable, Writable, 
                     WritableComparable, Writable> {
    static final String TARGET_VALUE = "reduce.memory-loader.target-value";
    private static MemoryLoader loader = null;
    
    public void reduce(WritableComparable key, Iterator<Writable> val, 
                       OutputCollector<WritableComparable, Writable> output,
                       Reporter reporter)
    throws IOException {
      assertNotNull("Reducer not configured!", loader);
      
      // load the memory
      loader.load();
      
      // work as identity reducer
      output.collect(key, key);
    }

    public void configure(JobConf conf) {
      loader = new MemoryLoader(conf.getLong(TARGET_VALUE, -1));
    }
  }

  @SuppressWarnings("deprecation")
  private long getTaskCounterUsage (JobClient client, JobID id, int numReports,
                                    int taskId, boolean isMap) 
  throws Exception {
    TaskReport[] reports = null;
    if (isMap) {
      reports = client.getMapTaskReports(id);
    } else {
      reports = client.getReduceTaskReports(id);
    }
    
    assertNotNull("No reports found for " + (isMap? "map" : "reduce") + " tasks" 
                  + "' in job " + id, reports);
    // make sure that the total number of reports match the expected
    assertEquals("Mismatch in task id", numReports, reports.length);
    
    Counters counters = reports[taskId].getCounters();
    
    return counters.getCounter(COMMITTED_HEAP_BYTES);
  }

  // set up heap options, target value for memory loader and the output 
  // directory before running the job
  @SuppressWarnings("deprecation")
  private static RunningJob runHeapUsageTestJob(JobConf conf, Path testRootDir,
                              String heapOptions, long targetMapValue,
                              long targetReduceValue, FileSystem fs, 
                              JobClient client, Path inDir) 
  throws IOException {
    // define a job
    JobConf jobConf = new JobConf(conf);
    
    // configure the jobs
    jobConf.setNumMapTasks(1);
    jobConf.setNumReduceTasks(1);
    jobConf.setMapperClass(MemoryLoaderMapper.class);
    jobConf.setReducerClass(MemoryLoaderReducer.class);
    jobConf.setInputFormat(TextInputFormat.class);
    jobConf.setOutputKeyClass(LongWritable.class);
    jobConf.setOutputValueClass(Text.class);
    jobConf.setMaxMapAttempts(1);
    jobConf.setMaxReduceAttempts(1);
    jobConf.set(JobConf.MAPRED_TASK_JAVA_OPTS, heapOptions);
    
    // set the targets
    jobConf.setLong(MemoryLoaderMapper.TARGET_VALUE, targetMapValue);
    jobConf.setLong(MemoryLoaderReducer.TARGET_VALUE, targetReduceValue);
    
    // set the input directory for the job
    FileInputFormat.setInputPaths(jobConf, inDir);
    
    // define job output folder
    Path outDir = new Path(testRootDir, "out");
    fs.delete(outDir, true);
    FileOutputFormat.setOutputPath(jobConf, outDir);
    
    // run the job
    RunningJob job = client.submitJob(jobConf);
    job.waitForCompletion();
    JobID jobID = job.getID();
    assertTrue("Job " + jobID + " failed!", job.isSuccessful());
    
    return job;
  }

  /**
   * Tests {@link TaskCounter}'s {@link TaskCounter.COMMITTED_HEAP_BYTES}. 
   * The test consists of running a low-memory job which consumes less heap 
   * memory and then running a high-memory job which consumes more heap memory, 
   * and then ensuring that COMMITTED_HEAP_BYTES of low-memory job is smaller 
   * than that of the high-memory job.
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  public void testHeapUsageCounter() throws Exception {
    JobConf conf = new JobConf();
    // create a local filesystem handle
    FileSystem fileSystem = FileSystem.getLocal(conf);
    
    // define test root directories
    File rootDir =
      new File(System.getProperty("test.build.data", "/tmp"));
    File testRootDir = new File(rootDir, "testHeapUsageCounter");
    // cleanup the test root directory
    Path testRootDirPath = new Path(testRootDir.toString());
    fileSystem.delete(testRootDirPath, true);
    // set the current working directory
    fileSystem.setWorkingDirectory(testRootDirPath);
    
    fileSystem.deleteOnExit(testRootDirPath);
    
    // create a mini cluster using the local file system
    MiniMRCluster mrCluster = 
      new MiniMRCluster(1, fileSystem.getUri().toString(), 1);
    
    try {
      conf = mrCluster.createJobConf();
      JobClient jobClient = new JobClient(conf);

      // define job input
      File file = new File(testRootDir, "in");
      Path inDir = new Path(file.toString());
      // create input data
      createWordsFile(file);

      // configure and run a low memory job which will run without loading the
      // jvm's heap
      RunningJob lowMemJob = 
        runHeapUsageTestJob(conf, testRootDirPath, "-Xms32m -Xmx1G", 
                            0, 0, fileSystem, jobClient, inDir);
      JobID lowMemJobID = lowMemJob.getID();
      long lowMemJobMapHeapUsage = getTaskCounterUsage(jobClient, lowMemJobID, 
                                                       1, 0, true);
      System.out.println("Job1 (low memory job) map task heap usage: " 
                         + lowMemJobMapHeapUsage);
      long lowMemJobReduceHeapUsage =
        getTaskCounterUsage(jobClient, lowMemJobID, 1, 0, false);
      System.out.println("Job1 (low memory job) reduce task heap usage: " 
                         + lowMemJobReduceHeapUsage);

      // configure and run a high memory job which will load the jvm's heap
      RunningJob highMemJob = 
        runHeapUsageTestJob(conf, testRootDirPath, "-Xms32m -Xmx1G", 
                            lowMemJobMapHeapUsage + 256*1024*1024, 
                            lowMemJobReduceHeapUsage + 256*1024*1024,
                            fileSystem, jobClient, inDir);
      JobID highMemJobID = highMemJob.getID();

      long highMemJobMapHeapUsage = getTaskCounterUsage(jobClient, highMemJobID,
                                                        1, 0, true);
      System.out.println("Job2 (high memory job) map task heap usage: " 
                         + highMemJobMapHeapUsage);
      long highMemJobReduceHeapUsage =
        getTaskCounterUsage(jobClient, highMemJobID, 1, 0, false);
      System.out.println("Job2 (high memory job) reduce task heap usage: " 
                         + highMemJobReduceHeapUsage);

      assertTrue("Incorrect map heap usage reported by the map task", 
                 lowMemJobMapHeapUsage < highMemJobMapHeapUsage);

      assertTrue("Incorrect reduce heap usage reported by the reduce task", 
                 lowMemJobReduceHeapUsage < highMemJobReduceHeapUsage);
    } finally {
      // shutdown the mr cluster
      mrCluster.shutdown();
      try {
        fileSystem.delete(testRootDirPath, true);
      } catch (IOException ioe) {} 
    }
  }
}
