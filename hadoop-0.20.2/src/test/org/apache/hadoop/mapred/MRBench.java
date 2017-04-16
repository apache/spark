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
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Runs a job multiple times and takes average of all runs.
 */
public class MRBench extends Configured implements Tool{
  
  private static final Log LOG = LogFactory.getLog(MRBench.class);
  private static Path BASE_DIR =
    new Path(System.getProperty("test.build.data","/benchmarks/MRBench"));
  private static Path INPUT_DIR = new Path(BASE_DIR, "mr_input");
  private static Path OUTPUT_DIR = new Path(BASE_DIR, "mr_output");
  
  public static enum Order {RANDOM, ASCENDING, DESCENDING}; 
  
  /**
   * Takes input format as text lines, runs some processing on it and 
   * writes out data as text again. 
   */
  public static class Map extends MapReduceBase
    implements Mapper<WritableComparable, Text, UTF8, UTF8> {
    
    public void map(WritableComparable key, Text value,
                    OutputCollector<UTF8, UTF8> output,
                    Reporter reporter) throws IOException 
    {
      String line = value.toString();
      output.collect(new UTF8(process(line)), new UTF8(""));		
    }
    public String process(String line) {
      return line; 
    }
  }

  /**
   * Ignores the key and writes values to the output. 
   */
  public static class Reduce extends MapReduceBase
    implements Reducer<UTF8, UTF8, UTF8, UTF8> {
    
    public void reduce(UTF8 key, Iterator<UTF8> values,
                       OutputCollector<UTF8, UTF8> output, Reporter reporter) throws IOException 
    {
      while(values.hasNext()) {
        output.collect(key, new UTF8(values.next().toString()));
      }
    }
  }

  /**
   * Generate a text file on the given filesystem with the given path name.
   * The text file will contain the given number of lines of generated data.
   * The generated data are string representations of numbers.  Each line
   * is the same length, which is achieved by padding each number with
   * an appropriate number of leading '0' (zero) characters.  The order of
   * generated data is one of ascending, descending, or random.
   */
  public void generateTextFile(FileSystem fs, Path inputFile, 
                                      long numLines, Order sortOrder) throws IOException 
  {
    LOG.info("creating control file: "+numLines+" numLines, "+sortOrder+" sortOrder");
    PrintStream output = null;
    try {
      output = new PrintStream(fs.create(inputFile));
      int padding = String.valueOf(numLines).length();
      switch(sortOrder) {
      case RANDOM:
        for (long l = 0; l < numLines; l++) {
          output.println(pad((new Random()).nextLong(), padding));
        }
        break; 
      case ASCENDING: 
        for (long l = 0; l < numLines; l++) {
          output.println(pad(l, padding));
        }
        break;
      case DESCENDING: 
        for (long l = numLines; l > 0; l--) {
          output.println(pad(l, padding));
        }
        break;
      }
    } finally {
      if (output != null)
        output.close();
    }
    LOG.info("created control file: " + inputFile);
  }
  
  /**
   * Convert the given number to a string and pad the number with 
   * leading '0' (zero) characters so that the string is exactly
   * the given length.
   */
  private static String pad(long number, int length) {
    String str = String.valueOf(number);
    StringBuffer value = new StringBuffer(); 
    for (int i = str.length(); i < length; i++) {
      value.append("0"); 
    }
    value.append(str); 
    return value.toString();
  }
  
  /**
   * Create the job configuration.
   */
  private JobConf setupJob(int numMaps, int numReduces, String jarFile) {
    JobConf jobConf = new JobConf(getConf());
    jobConf.setJarByClass(MRBench.class);
    FileInputFormat.addInputPath(jobConf, INPUT_DIR);
    
    jobConf.setInputFormat(TextInputFormat.class);
    jobConf.setOutputFormat(TextOutputFormat.class);
    
    jobConf.setOutputValueClass(UTF8.class);
    
    jobConf.setMapOutputKeyClass(UTF8.class);
    jobConf.setMapOutputValueClass(UTF8.class);
    
    if (null != jarFile) {
      jobConf.setJar(jarFile);
    }
    jobConf.setMapperClass(Map.class);
    jobConf.setReducerClass(Reduce.class);
    
    jobConf.setNumMapTasks(numMaps);
    jobConf.setNumReduceTasks(numReduces);
    jobConf
        .setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);
    return jobConf; 
  }
  
  /**
   * Runs a MapReduce task, given number of times. The input to each run
   * is the same file.
   */
  private ArrayList<Long> runJobInSequence(JobConf masterJobConf, int numRuns) throws IOException {
    Random rand = new Random();
    ArrayList<Long> execTimes = new ArrayList<Long>(); 
    
    for (int i = 0; i < numRuns; i++) {
      // create a new job conf every time, reusing same object does not work 
      JobConf jobConf = new JobConf(masterJobConf);
      // reset the job jar because the copy constructor doesn't
      jobConf.setJar(masterJobConf.getJar());
      // give a new random name to output of the mapred tasks
      FileOutputFormat.setOutputPath(jobConf, 
                         new Path(OUTPUT_DIR, "output_" + rand.nextInt()));

      LOG.info("Running job " + i + ":" +
               " input=" + FileInputFormat.getInputPaths(jobConf)[0] + 
               " output=" + FileOutputFormat.getOutputPath(jobConf));
      
      // run the mapred task now 
      long curTime = System.currentTimeMillis();
      JobClient.runJob(jobConf);
      execTimes.add(new Long(System.currentTimeMillis() - curTime));
    }
    return execTimes;
  }
  
  /**
   * <pre>
   * Usage: mrbench
   *    [-baseDir <base DFS path for output/input, default is /benchmarks/MRBench>]
   *    [-jar <local path to job jar file containing Mapper and Reducer implementations, default is current jar file>]
   *    [-numRuns <number of times to run the job, default is 1>]
   *    [-maps <number of maps for each run, default is 2>]
   *    [-reduces <number of reduces for each run, default is 1>]
   *    [-inputLines <number of input lines to generate, default is 1>]
   *    [-inputType <type of input to generate, one of ascending (default), descending, random>]
   *    [-verbose]
   * </pre>
   */
  public static void main (String[] args) throws Exception {
    int res = ToolRunner.run(new MRBench(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    String version = "MRBenchmark.0.0.2";
    System.out.println(version);

    String usage = 
      "Usage: mrbench " +
      "[-baseDir <base DFS path for output/input, default is /benchmarks/MRBench>] " + 
      "[-jar <local path to job jar file containing Mapper and Reducer implementations, default is current jar file>] " + 
      "[-numRuns <number of times to run the job, default is 1>] " +
      "[-maps <number of maps for each run, default is 2>] " +
      "[-reduces <number of reduces for each run, default is 1>] " +
      "[-inputLines <number of input lines to generate, default is 1>] " +
      "[-inputType <type of input to generate, one of ascending (default), descending, random>] " + 
      "[-verbose]";
    
    String jarFile = null;
    int inputLines = 1; 
    int numRuns = 1;
    int numMaps = 2; 
    int numReduces = 1;
    boolean verbose = false;         
    Order inputSortOrder = Order.ASCENDING;     
    for (int i = 0; i < args.length; i++) { // parse command line
      if (args[i].equals("-jar")) {
        jarFile = args[++i];
      } else if (args[i].equals("-numRuns")) {
        numRuns = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-baseDir")) {
        BASE_DIR = new Path(args[++i]);
      } else if (args[i].equals("-maps")) {
        numMaps = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-reduces")) {
        numReduces = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-inputLines")) {
        inputLines = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-inputType")) {
        String s = args[++i]; 
        if (s.equalsIgnoreCase("ascending")) {
          inputSortOrder = Order.ASCENDING;
        } else if (s.equalsIgnoreCase("descending")) {
          inputSortOrder = Order.DESCENDING; 
        } else if (s.equalsIgnoreCase("random")) {
          inputSortOrder = Order.RANDOM;
        } else {
          inputSortOrder = null;
        }
      } else if (args[i].equals("-verbose")) {
        verbose = true;
      } else {
        System.err.println(usage);
        System.exit(-1);
      }
    }
    
    if (numRuns < 1 ||  // verify args
        numMaps < 1 ||
        numReduces < 1 ||
        inputLines < 0 ||
        inputSortOrder == null)
      {
        System.err.println(usage);
        return -1;
      }

    JobConf jobConf = setupJob(numMaps, numReduces, jarFile);
    FileSystem fs = FileSystem.get(jobConf);
    Path inputFile = new Path(INPUT_DIR, "input_" + (new Random()).nextInt() + ".txt");
    generateTextFile(fs, inputFile, inputLines, inputSortOrder);

    // setup test output directory
    fs.mkdirs(BASE_DIR); 
    ArrayList<Long> execTimes = new ArrayList<Long>();
    try {
      execTimes = runJobInSequence(jobConf, numRuns);
    } finally {
      // delete output -- should we really do this?
      fs.delete(BASE_DIR, true);
    }
    
    if (verbose) {
      // Print out a report 
      System.out.println("Total MapReduce jobs executed: " + numRuns);
      System.out.println("Total lines of data per job: " + inputLines);
      System.out.println("Maps per job: " + numMaps);
      System.out.println("Reduces per job: " + numReduces);
    }
    int i = 0;
    long totalTime = 0; 
    for (Long time : execTimes) {
      totalTime += time.longValue(); 
      if (verbose) {
        System.out.println("Total milliseconds for task: " + (++i) + 
                           " = " +  time);
      }
    }
    long avgTime = totalTime / numRuns;    
    System.out.println("DataLines\tMaps\tReduces\tAvgTime (milliseconds)");
    System.out.println(inputLines + "\t\t" + numMaps + "\t" + 
                       numReduces + "\t" + avgTime);
    return 0;
  }
  
}
