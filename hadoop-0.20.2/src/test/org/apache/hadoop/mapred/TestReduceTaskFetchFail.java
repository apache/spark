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

import static org.junit.Assert.*;

import java.io.IOException;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.ReduceTask;
import org.junit.Test;
import static org.mockito.Mockito.*;



public class TestReduceTaskFetchFail {

  public static class TestReduceTask extends ReduceTask {
    public TestReduceTask() {
       super();
    }
    public String getJobFile() { return "/foo"; }

    public class TestReduceCopier extends ReduceCopier {
      public TestReduceCopier(TaskUmbilicalProtocol umbilical, JobConf conf,
                        TaskReporter reporter
                        )throws ClassNotFoundException, IOException {
        super(umbilical, conf, reporter);
      }

      public void checkAndInformJobTracker(int failures, TaskAttemptID mapId, boolean readError) {
        super.checkAndInformJobTracker(failures, mapId, readError);
      }

    }

  }


  @SuppressWarnings("deprecation")
  @Test
  public void testcheckAndInformJobTracker() throws Exception {
    //mock creation
    TaskUmbilicalProtocol mockUmbilical = mock(TaskUmbilicalProtocol.class);
    TaskReporter mockTaskReporter = mock(TaskReporter.class);

    JobConf conf = new JobConf();
    conf.setUser("testuser");
    conf.setJobName("testJob");
    conf.setSessionId("testSession");

    TaskAttemptID tid =  new TaskAttemptID();
    TestReduceTask rTask = new TestReduceTask();
    rTask.setConf(conf);

    ReduceTask.ReduceCopier reduceCopier = rTask.new TestReduceCopier(mockUmbilical, conf, mockTaskReporter);
    reduceCopier.checkAndInformJobTracker(1, tid, false);

    verify(mockTaskReporter, never()).progress();

    reduceCopier.checkAndInformJobTracker(10, tid, false);
    verify(mockTaskReporter, times(1)).progress();

    // Test the config setting
    conf.setInt("mapreduce.reduce.shuffle.maxfetchfailures", 3);

    rTask.setConf(conf);
    reduceCopier = rTask.new TestReduceCopier(mockUmbilical, conf, mockTaskReporter);

    reduceCopier.checkAndInformJobTracker(1, tid, false);
    verify(mockTaskReporter, times(1)).progress();

    reduceCopier.checkAndInformJobTracker(3, tid, false);
    verify(mockTaskReporter, times(2)).progress();

    reduceCopier.checkAndInformJobTracker(5, tid, false);
    verify(mockTaskReporter, times(2)).progress();

    reduceCopier.checkAndInformJobTracker(6, tid, false);
    verify(mockTaskReporter, times(3)).progress();

    // test readError and its config
    reduceCopier.checkAndInformJobTracker(7, tid, true);
    verify(mockTaskReporter, times(4)).progress();

    conf.setBoolean("mapreduce.reduce.shuffle.notify.readerror", false);

    rTask.setConf(conf);
    reduceCopier = rTask.new TestReduceCopier(mockUmbilical, conf, mockTaskReporter);

    reduceCopier.checkAndInformJobTracker(7, tid, true);
    verify(mockTaskReporter, times(4)).progress();

  }
}
