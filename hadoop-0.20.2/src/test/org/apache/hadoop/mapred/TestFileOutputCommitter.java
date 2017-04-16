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
import junit.framework.TestCase;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

public class TestFileOutputCommitter extends TestCase {
  private static Path outDir = new Path(
     System.getProperty("test.build.data", "."), "output");

  // A random task attempt id for testing.
  private static String attempt = "attempt_200707121733_0001_m_000000_0";
  private static TaskAttemptID taskID = TaskAttemptID.forName(attempt);

  @SuppressWarnings("unchecked")
  public void testCommitter() throws Exception {
    JobConf job = new JobConf();
    job.set("mapred.task.id", attempt);
    job.setOutputCommitter(FileOutputCommitter.class);
    FileOutputFormat.setOutputPath(job, outDir);
    JobContext jContext = new JobContext(job, taskID.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContext(job, taskID);
    FileOutputCommitter committer = new FileOutputCommitter();
    FileOutputFormat.setWorkOutputPath(job, 
      committer.getTempTaskOutputPath(tContext));

    committer.setupJob(jContext);
    committer.setupTask(tContext);
    String file = "test.txt";

    // A reporter that does nothing
    Reporter reporter = Reporter.NULL;
    FileSystem localFs = FileSystem.getLocal(job);
    TextOutputFormat theOutputFormat = new TextOutputFormat();
    RecordWriter theRecordWriter =
      theOutputFormat.getRecordWriter(localFs, job, file, reporter);
    Text key1 = new Text("key1");
    Text key2 = new Text("key2");
    Text val1 = new Text("val1");
    Text val2 = new Text("val2");
    NullWritable nullWritable = NullWritable.get();

    try {
      theRecordWriter.write(key1, val1);
      theRecordWriter.write(null, nullWritable);
      theRecordWriter.write(null, val1);
      theRecordWriter.write(nullWritable, val2);
      theRecordWriter.write(key2, nullWritable);
      theRecordWriter.write(key1, null);
      theRecordWriter.write(null, null);
      theRecordWriter.write(key2, val2);
    } finally {
      theRecordWriter.close(reporter);
    }
    committer.commitTask(tContext);
    committer.commitJob(jContext);
    
    File expectedFile = new File(new Path(outDir, file).toString());
    StringBuffer expectedOutput = new StringBuffer();
    expectedOutput.append(key1).append('\t').append(val1).append("\n");
    expectedOutput.append(val1).append("\n");
    expectedOutput.append(val2).append("\n");
    expectedOutput.append(key2).append("\n");
    expectedOutput.append(key1).append("\n");
    expectedOutput.append(key2).append('\t').append(val2).append("\n");
    String output = UtilsForTests.slurp(expectedFile);
    assertEquals(output, expectedOutput.toString());
  }

  public static void main(String[] args) throws Exception {
    new TestFileOutputCommitter().testCommitter();
  }
}
