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
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobHistory.*;

import junit.framework.TestCase;

public class TestJobHistoryParsing  extends TestCase {
  ArrayList<PrintWriter> historyWriter = new ArrayList<PrintWriter>();

  /**
   * Listener for a test history log file, it populates JobHistory.JobInfo 
   * object with data from log file. 
   */
  static class TestListener implements Listener {
    JobHistory.JobInfo job;

    TestListener(JobHistory.JobInfo job) {
      this.job = job;
    }
    // JobHistory.Listener implementation 
    public void handle(RecordTypes recType, 
                       Map<JobHistory.Keys, String> values)
    throws IOException {
      if (recType == JobHistory.RecordTypes.Job) {
        job.handle(values);
      }
    }
  }

  public void testHistoryParsing() throws IOException {
    // open a test history file
    Path historyDir = new Path(System.getProperty("test.build.data", "."), 
                                "history");
    JobConf conf = new JobConf();
    conf.set("hadoop.job.history.location", historyDir.toString());
    FileSystem fs = FileSystem.getLocal(new JobConf());
    JobHistory.init(null, conf, "localhost", 1234);
    Path historyLog = new Path(historyDir, "testlog");
    PrintWriter out = new PrintWriter(fs.create(historyLog));
    historyWriter.add(out);
    // log keys and values into history
    String value1 = "Value has equal=to, \"quotes\" and spaces in it";
    String value2 = "Value has \n new line \n and " + 
                    "dot followed by new line .\n in it ";
    String value3 = "Value has characters: " +
                    "`1234567890-=qwertyuiop[]\\asdfghjkl;'zxcvbnm,./" +
                    "~!@#$%^&*()_+QWERTYUIOP{}|ASDFGHJKL:\"'ZXCVBNM<>?" + 
                    "\t\b\n\f\"\n in it";
    String value4 = "Value ends with escape\\";
    String value5 = "Value ends with \\\" \\.\n";
    StringBuilder sb = new StringBuilder("Longer value with many escaped "+
        "chars, which tends to overflow the stack of brittle regex parsers");
    for (int i = 0; i < 1000; ++i) {
      sb.append(",");
      sb.append("\\split.");
      sb.append(i);
    }
    String value6 = sb.toString();

    // Log the history version
    JobHistory.MetaInfoManager.logMetaInfo(historyWriter);
    
    JobHistory.log(historyWriter, RecordTypes.Job, 
                   new JobHistory.Keys[] {Keys.JOBTRACKERID, 
                                          Keys.TRACKER_NAME, 
                                          Keys.JOBNAME, 
                                          Keys.JOBCONF,
                                          Keys.USER,
                                          Keys.SPLITS},
                   new String[] {value1, value2, value3, value4, value5,
                                 value6});
    // close history file
    out.close();
    historyWriter.remove(out);

    // parse history
    String jobId = "job_200809171136_0001"; // random jobid for tesing 
    JobHistory.JobInfo job = new JobHistory.JobInfo(jobId);
    JobHistory.parseHistoryFromFS(historyLog.toString(), 
                 new TestListener(job), fs);
    // validate keys and values
    assertEquals(value1, job.get(Keys.JOBTRACKERID));
    assertEquals(value2, job.get(Keys.TRACKER_NAME));
    assertEquals(value3, job.get(Keys.JOBNAME));
    assertEquals(value4, job.get(Keys.JOBCONF));
    assertEquals(value5, job.get(Keys.USER));
    assertEquals(value6, job.get(Keys.SPLITS));
  }
}
