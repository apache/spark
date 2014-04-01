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
import junit.framework.TestCase;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobHistory.JobInfo;
import org.apache.hadoop.mapred.JobHistory.RecordTypes;

/**
 * Tests the JobHistory parser with different versions of job history files.
 * This may have to change when new versions of job history files come up.
 */
public class TestJobHistoryVersion extends TestCase {
  private static final String HOSTNAME = "localhost";
  private static final String TIME= "1234567890123";
  private static final String USER = "user";
  private static final String JOBNAME = "job";
  private static final String JOB = "job_200809180000_0001";
  private static final String FILENAME = 
    HOSTNAME + "_" + TIME + "_" +  JOB + "_" + USER + "_" + JOBNAME;
  private static final String TASK_ID = "tip_200809180000_0001_0";
  private static final String TASK_ATTEMPT_ID = 
    "attempt_200809180000_0001_0_1234567890123";
  private static final String COUNTERS = 
    "Job Counters.Launched map tasks:1," 
    + "Map-Reduce Framework.Map input records:0,"
    + "Map-Reduce Framework.Map input bytes:0,"
    + "File Systems.HDFS bytes written:0,";
  private static final Path TEST_DIR = 
    new Path(System.getProperty("test.build.data", "/tmp"), 
             "test-history-version");
  private static final String DELIM = ".";
  
  /**
   * Creates a job history file of a given specific version. This method should
   * change if format/content of future versions of job history file changes.
   */
  private void writeHistoryFile(FSDataOutputStream out, long version)
  throws IOException {
    String delim = "\n"; // '\n' for version 0
    String counters = COUNTERS;
    String jobConf = "job.xml";
    if (version > 0) { // line delimeter should be '.' for later versions
      // Change the delimiter
      delim = DELIM + delim;
      
      // Write the version line
      out.writeBytes(RecordTypes.Meta.name() + " VERSION=\"" 
                     + JobHistory.VERSION + "\" " + delim);
      jobConf = JobHistory.escapeString(jobConf);
      counters = JobHistory.escapeString(counters);
    }
    
    // Write the job-start line
    
    
    out.writeBytes("Job JOBID=\"" + JOB + "\" JOBNAME=\"" + JOBNAME + "\"" 
                   + " USER=\"" + USER + "\" SUBMIT_TIME=\"" + TIME + "\"" 
                   + " JOBCONF=\"" + jobConf + "\" " + delim);
    
    // Write the job-launch line
    out.writeBytes("Job JOBID=\"" + JOB + "\" LAUNCH_TIME=\"" + TIME + "\"" 
                   + " TOTAL_MAPS=\"1\" TOTAL_REDUCES=\"0\" " + delim);
    
    // Write the task start line
    out.writeBytes("Task TASKID=\"" + TASK_ID + "\" TASK_TYPE=\"MAP\"" 
                   + " START_TIME=\"" + TIME + "\" SPLITS=\"\"" 
                   + " TOTAL_MAPS=\"1\" TOTAL_REDUCES=\"0\" " + delim);
    
    // Write the attempt start line
    out.writeBytes("MapAttempt TASK_TYPE=\"MAP\" TASKID=\"" + TASK_ID + "\"" 
                   + " TASK_ATTEMPT_ID=\"" + TASK_ATTEMPT_ID + "\"" 
                   + " START_TIME=\"" + TIME + "\"" 
                   + " HOSTNAME=\"" + HOSTNAME + "\" " + delim);
    
    // Write the attempt finish line
    out.writeBytes("MapAttempt TASK_TYPE=\"MAP\" TASKID=\"" + TASK_ID + "\"" 
                   + " TASK_ATTEMPT_ID=\"" + TASK_ATTEMPT_ID + "\"" 
                   + " FINISH_TIME=\"" + TIME + "\""
                   + " TASK_STATUS=\"SUCCESS\""
                   + " HOSTNAME=\"" + HOSTNAME + "\" " + delim);
    
    // Write the task finish line
    out.writeBytes("Task TASKID=\"" + TASK_ID + "\" TASK_TYPE=\"MAP\""
                   + " TASK_STATUS=\"SUCCESS\""
                   + " FINISH_TIME=\"" + TIME + "\""
                   + " COUNTERS=\"" + counters + "\" " + delim);
    
    // Write the job-finish line
    out.writeBytes("Job JOBID=\"" + JOB + "\" FINISH_TIME=\"" + TIME + "\"" 
                   + " TOTAL_MAPS=\"1\" TOTAL_REDUCES=\"0\""
                   + " JOB_STATUS=\"SUCCESS\" FINISHED_MAPS=\"1\""
                   + " FINISHED_REDUCES=\"0\" FAILED_MAPS=\"0\""
                   + " FAILED_REDUCES=\"0\""
                   + " COUNTERS=\"" + counters + "\" " + delim);
    
  }
  
  /**
   * Tests the JobHistory parser with different versions of job history files
   */
  public void testJobHistoryVersion() throws IOException {
    // If new job history version comes up, the modified parser may fail for
    // the history file created by writeHistoryFile().
    for (long version = 0; version <= JobHistory.VERSION; version++) {
      JobConf conf = new JobConf();
      FileSystem fs = FileSystem.getLocal(conf);
    
      // cleanup
      fs.delete(TEST_DIR, true);
    
      Path historyPath = new Path(TEST_DIR + "/_logs/history/" +
                                  FILENAME + version);
    
      fs.delete(historyPath, false);
    
      FSDataOutputStream out = fs.create(historyPath);
      writeHistoryFile(out, version);
      out.close();
    
      JobInfo job = new JobHistory.JobInfo(JOB); 
      DefaultJobHistoryParser.parseJobTasks(historyPath.toString(), job, fs);
    
      assertTrue("Failed to parse jobhistory files of version " + version,
                 job.getAllTasks().size() > 0);
    
      // cleanup
      fs.delete(TEST_DIR, true);
    }
  }
}
