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

package testjar;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class JobKillCommitter {
  /**
   * The class provides a overrided implementation of output committer
   * set up method, which causes the job to fail during set up.
   */
  public static class CommitterWithFailSetup extends FileOutputCommitter {
    @Override
    public void setupJob(JobContext context) throws IOException {
      throw new IOException();
    }
  }

  /**
   * The class provides a dummy implementation of outputcommitter
   * which does nothing
   */
  public static class CommitterWithNoError extends FileOutputCommitter {
    @Override
    public void setupJob(JobContext context) throws IOException {
    }

    @Override
    public void commitJob(JobContext context) throws IOException {
    }
  }

  /**
   * The class provides a overrided implementation of commitJob which
   * causes the clean up method to fail.
   */
  public static class CommitterWithFailCleanup extends FileOutputCommitter {
    @Override
    public void commitJob(JobContext context) throws IOException {
      throw new IOException();
    }
  }

  /**
   * The class is used provides a dummy implementation for mapper method which
   * does nothing.
   */
  public static class MapperPass extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
    }
  }
  /**
  * The class provides a sleep implementation for mapper method.
  */
 public static class MapperPassSleep extends 
     Mapper<LongWritable, Text, Text, Text> {
   public void map(LongWritable key, Text value, Context context)
       throws IOException, InterruptedException {
     Thread.sleep(10000);
   }
 }

  /**
   * The class  provides a way for the mapper function to fail by
   * intentionally throwing an IOException
   */
  public static class MapperFail extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      throw new IOException();
    }
  }

  /**
   * The class provides a way for the reduce function to fail by
   * intentionally throwing an IOException
   */
  public static class ReducerFail extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, Context context)
        throws IOException, InterruptedException {
      throw new IOException();
    }
  }

  /**
   * The class provides a empty implementation of reducer method that
   * does nothing
   */
  public static class ReducerPass extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, Context context)
        throws IOException, InterruptedException {
    }
  }
}
