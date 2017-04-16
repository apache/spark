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

package org.apache.hadoop.streaming;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters.Group;

/**
 * This class tests streaming counters in MapReduce local mode.
 */
public class TestStreamingCounters extends TestStreaming {

  public TestStreamingCounters() throws IOException {
    super();
  }

  public void testCommandLine() throws IOException
  {
    try {
      try {
        FileUtil.fullyDelete(OUTPUT_DIR.getAbsoluteFile());
      } catch (Exception e) {
      }

      createInput();
      boolean mayExit = false;

      // During tests, the default Configuration will use a local mapred
      // So don't specify -config or -cluster
      StreamJob job = new StreamJob(genArgs(), mayExit);      
      job.go();
      File outFile = new File(OUTPUT_DIR, "part-00000").getAbsoluteFile();
      String output = StreamUtil.slurp(outFile);
      outFile.delete();
      assertEquals(outputExpect, output);
      
      Counters counters = job.running_.getCounters();
      assertNotNull("Counters", counters);
      Group group = counters.getGroup("UserCounters");
      assertNotNull("Group", group);
      Counter counter = group.getCounterForName("InputLines");
      assertNotNull("Counter", counter);
      assertEquals(3, counter.getCounter());
    } finally {
      INPUT_FILE.delete();
      FileUtil.fullyDelete(OUTPUT_DIR.getAbsoluteFile());
    }
  }
  
}
