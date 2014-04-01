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
package org.apache.hadoop.tools.rumen;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestConcurrentRead {
  static final List<LoggedJob> cachedTrace = new ArrayList<LoggedJob>();
  static final String traceFile = 
      "rumen/small-trace-test/job-tracker-logs-trace-output.gz";
  
  static Configuration conf;
  static FileSystem lfs;
  static Path path;
  
  @BeforeClass
  static public void globalSetUp() throws IOException {
    conf = new Configuration();
    lfs = FileSystem.getLocal(conf);
    Path rootInputDir = new Path(System.getProperty("test.tools.input.dir", ""))
        .makeQualified(lfs);
    path = new Path(rootInputDir, traceFile);
    JobTraceReader reader = new JobTraceReader(path, conf);
    try {
      LoggedJob job;
      while ((job = reader.getNext()) != null) {
        cachedTrace.add(job);
      }
    } finally {
      reader.close();
    }
  }

  void readAndCompare() throws IOException {
    JobTraceReader reader = new JobTraceReader(path, conf);
    try {
      for (Iterator<LoggedJob> it = cachedTrace.iterator(); it.hasNext();) {
        LoggedJob jobExpected = it.next();
        LoggedJob jobRead = reader.getNext();
        assertNotNull(jobRead);
        try {
          jobRead.deepCompare(jobExpected, null);
        } catch (DeepInequalityException e) {
          fail(e.toString());
        }
      }
      assertNull(reader.getNext());
    } finally {
      reader.close();
    }
  }

  class TestThread extends Thread {
    final int repeat;
    final CountDownLatch startSignal, doneSignal;
    final Map<String, Throwable> errors;

    TestThread(int id, int repeat, CountDownLatch startSignal, CountDownLatch doneSignal, Map<String, Throwable> errors) {
      super(String.format("TestThread-%d", id));
      this.repeat = repeat;
      this.startSignal = startSignal;
      this.doneSignal = doneSignal;
      this.errors = errors;
    }

    @Override
    public void run() {
      try {
        startSignal.await();
        for (int i = 0; i < repeat; ++i) {
          try {
            readAndCompare();
          } catch (Throwable e) {
            errors.put(getName(), e);
            break;
          }
        }
        doneSignal.countDown();
      } catch (Throwable e) {
        errors.put(getName(), e);
      }
    }
  }

  @Test
  public void testConcurrentRead() throws InterruptedException {
    int nThr = conf.getInt("test.rumen.concurrent-read.threads", 4);
    int repeat = conf.getInt("test.rumen.concurrent-read.repeat", 10);
    CountDownLatch startSignal = new CountDownLatch(1);
    CountDownLatch doneSignal = new CountDownLatch(nThr);
    Map<String, Throwable> errors = Collections
        .synchronizedMap(new TreeMap<String, Throwable>());
    for (int i = 0; i < nThr; ++i) {
      new TestThread(i, repeat, startSignal, doneSignal, errors).start();
    }
    startSignal.countDown();
    doneSignal.await();
    if (!errors.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      for (Map.Entry<String, Throwable> e : errors.entrySet()) {
        sb.append(String.format("%s:\n%s\n", e.getKey(), e.getValue().toString()));
      }
      fail(sb.toString());
    }
  }
}
