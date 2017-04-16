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

package org.apache.hadoop.tools;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.LongSumReducer;

/**
 * Logalyzer: A utility tool for archiving and analyzing hadoop logs.
 * <p>
 * This tool supports archiving and anaylzing (sort/grep) of log-files.
 * It takes as input
 *  a) Input uri which will serve uris of the logs to be archived.
 *  b) Output directory (not mandatory).
 *  b) Directory on dfs to archive the logs. 
 *  c) The sort/grep patterns for analyzing the files and separator for boundaries.
 * Usage: 
 * Logalyzer -archive -archiveDir <directory to archive logs> -analysis <directory> -logs <log-list uri> -grep <pattern> -sort <col1, col2> -separator <separator>   
 * <p>
 */

public class Logalyzer {
  // Constants
  private static Configuration fsConfig = new Configuration();
  
  /** A {@link Mapper} that extracts text matching a regular expression. */
  public static class LogRegexMapper<K extends WritableComparable>
    extends MapReduceBase
    implements Mapper<K, Text, Text, LongWritable> {
    
    private Pattern pattern;
    
    public void configure(JobConf job) {
      pattern = Pattern.compile(job.get("mapred.mapper.regex"));
    }
    
    public void map(K key, Text value,
                    OutputCollector<Text, LongWritable> output,
                    Reporter reporter)
      throws IOException {
      String text = value.toString();
      Matcher matcher = pattern.matcher(text);
      while (matcher.find()) {
        output.collect(value, new LongWritable(1));
      }
    }
    
  }
  
  /** A WritableComparator optimized for UTF8 keys of the logs. */
  public static class LogComparator extends Text.Comparator implements Configurable {
    
    private static Log LOG = LogFactory.getLog(Logalyzer.class);
    private JobConf conf = null;
    private String[] sortSpec = null;
    private String columnSeparator = null;
    
    public void setConf(Configuration conf) {
      if (conf instanceof JobConf) {
        this.conf = (JobConf) conf;
      } else {
        this.conf = new JobConf(conf);
      }
      
      //Initialize the specification for *comparision*
      String sortColumns = this.conf.get("mapred.reducer.sort", null);
      if (sortColumns != null) {
        sortSpec = sortColumns.split(",");
      }
      
      //Column-separator
      columnSeparator = this.conf.get("mapred.reducer.separator", "");
    }
    
    public Configuration getConf() {
      return conf;
    }
    
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      
      if (sortSpec == null) {
        return super.compare(b1, s1, l1, b2, s2, l2);
      }
      
      try {
        Text logline1 = new Text(); 
        logline1.readFields(new DataInputStream(new ByteArrayInputStream(b1, s1, l1)));
        String line1 = logline1.toString();
        String[] logColumns1 = line1.split(columnSeparator);
        
        Text logline2 = new Text(); 
        logline2.readFields(new DataInputStream(new ByteArrayInputStream(b2, s2, l2)));
        String line2 = logline2.toString();
        String[] logColumns2 = line2.split(columnSeparator);
        
        if (logColumns1 == null || logColumns2 == null) {
          return super.compare(b1, s1, l1, b2, s2, l2);
        }
        
        //Compare column-wise according to *sortSpec*
        for(int i=0; i < sortSpec.length; ++i) {
          int column = (Integer.valueOf(sortSpec[i]).intValue());
          String c1 = logColumns1[column]; 
          String c2 = logColumns2[column];
          
          //Compare columns
          int comparision = super.compareBytes(
                                               c1.getBytes(), 0, c1.length(),
                                               c2.getBytes(), 0, c2.length()
                                               );
          
          //They differ!
          if (comparision != 0) {
            return comparision;
          }
        }
        
      } catch (IOException ioe) {
        LOG.fatal("Caught " + ioe);
        return 0;
      }
      
      return 0;
    }
    
    static {                                        
      // register this comparator
      WritableComparator.define(Text.class, new LogComparator());
    }
  }
  
  /**
   * doArchive: Workhorse function to archive log-files.
   * @param logListURI : The uri which will serve list of log-files to archive.
   * @param archiveDirectory : The directory to store archived logfiles.
   * @throws IOException
   */
  public void	
    doArchive(String logListURI, String archiveDirectory)
    throws IOException
  {
    String destURL = FileSystem.getDefaultUri(fsConfig) + archiveDirectory;
    DistCp.copy(new JobConf(fsConfig), logListURI, destURL, null, true, false);
  }
  
  /**
   * doAnalyze: 
   * @param inputFilesDirectory : Directory containing the files to be analyzed.
   * @param outputDirectory : Directory to store analysis (output).
   * @param grepPattern : Pattern to *grep* for.
   * @param sortColumns : Sort specification for output.
   * @param columnSeparator : Column separator.
   * @throws IOException
   */
  public void
    doAnalyze(String inputFilesDirectory, String outputDirectory,
              String grepPattern, String sortColumns, String columnSeparator)
    throws IOException
  {		
    Path grepInput = new Path(inputFilesDirectory);
    
    Path analysisOutput = null;
    if (outputDirectory.equals("")) {
      analysisOutput =  new Path(inputFilesDirectory, "logalyzer_" + 
                                 Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
    } else {
      analysisOutput = new Path(outputDirectory);
    }
    
    JobConf grepJob = new JobConf(fsConfig);
    grepJob.setJobName("logalyzer-grep-sort");
    
    FileInputFormat.setInputPaths(grepJob, grepInput);
    grepJob.setInputFormat(TextInputFormat.class);
    
    grepJob.setMapperClass(LogRegexMapper.class);
    grepJob.set("mapred.mapper.regex", grepPattern);
    grepJob.set("mapred.reducer.sort", sortColumns);
    grepJob.set("mapred.reducer.separator", columnSeparator);
    
    grepJob.setCombinerClass(LongSumReducer.class);
    grepJob.setReducerClass(LongSumReducer.class);
    
    FileOutputFormat.setOutputPath(grepJob, analysisOutput);
    grepJob.setOutputFormat(TextOutputFormat.class);
    grepJob.setOutputKeyClass(Text.class);
    grepJob.setOutputValueClass(LongWritable.class);
    grepJob.setOutputKeyComparatorClass(LogComparator.class);
    
    grepJob.setNumReduceTasks(1);                 // write a single file
    
    JobClient.runJob(grepJob);
  }
  
  public static void main(String[] args) {
    
    Log LOG = LogFactory.getLog(Logalyzer.class);
    
    String version = "Logalyzer.0.0.1";
    String usage = "Usage: Logalyzer [-archive -logs <urlsFile>] " +
      "-archiveDir <archiveDirectory> " +
      "-grep <pattern> -sort <column1,column2,...> -separator <separator> " +
      "-analysis <outputDirectory>";
    
    System.out.println(version);
    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }
    
    //Command line arguments
    boolean archive = false;
    boolean grep = false;
    boolean sort = false;
    
    String archiveDir = "";
    String logListURI = "";
    String grepPattern = ".*";
    String sortColumns = "";
    String columnSeparator = " ";
    String outputDirectory = "";
    
    for (int i = 0; i < args.length; i++) { // parse command line
      if (args[i].equals("-archive")) {
        archive = true;
      } else if (args[i].equals("-archiveDir")) {
        archiveDir = args[++i];
      } else if (args[i].equals("-grep")) {
        grep = true;
        grepPattern = args[++i];
      } else if (args[i].equals("-logs")) {
        logListURI = args[++i];
      } else if (args[i].equals("-sort")) {
        sort = true;
        sortColumns = args[++i];
      } else if (args[i].equals("-separator")) {
        columnSeparator = args[++i];
      } else if (args[i].equals("-analysis")) {
        outputDirectory = args[++i];
      }
    }
    
    LOG.info("analysisDir = " + outputDirectory);
    LOG.info("archiveDir = " + archiveDir);
    LOG.info("logListURI = " + logListURI);
    LOG.info("grepPattern = " + grepPattern);
    LOG.info("sortColumns = " + sortColumns);
    LOG.info("separator = " + columnSeparator);
    
    try {
      Logalyzer logalyzer = new Logalyzer();
      
      // Archive?
      if (archive) {
        logalyzer.doArchive(logListURI, archiveDir);
      }
      
      // Analyze?
      if (grep || sort) {
        logalyzer.doAnalyze(archiveDir, outputDirectory, grepPattern, sortColumns, columnSeparator);
      }
    } catch (IOException ioe) {
      ioe.printStackTrace();
      System.exit(-1);
    }
    
  } //main
  
} //class Logalyzer
