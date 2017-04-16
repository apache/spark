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

package org.apache.hadoop.mapred.pipes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * Handles the upward (C++ to Java) messages from the application.
 */
class OutputHandler<K extends WritableComparable,
                    V extends Writable>
  implements UpwardProtocol<K, V> {
  
  private Reporter reporter;
  private OutputCollector<K, V> collector;
  private float progressValue = 0.0f;
  private boolean done = false;
  
  private Throwable exception = null;
  RecordReader<FloatWritable,NullWritable> recordReader = null;
  private Map<Integer, Counters.Counter> registeredCounters = 
    new HashMap<Integer, Counters.Counter>();

  private String expectedDigest = null;
  private boolean digestReceived = false;
  /**
   * Create a handler that will handle any records output from the application.
   * @param collector the "real" collector that takes the output
   * @param reporter the reporter for reporting progress
   */
  public OutputHandler(OutputCollector<K, V> collector, Reporter reporter, 
                       RecordReader<FloatWritable,NullWritable> recordReader,
                       String expectedDigest) {
    this.reporter = reporter;
    this.collector = collector;
    this.recordReader = recordReader;
    this.expectedDigest = expectedDigest;
  }

  /**
   * The task output a normal record.
   */
  public void output(K key, V value) throws IOException {
    collector.collect(key, value);
  }

  /**
   * The task output a record with a partition number attached.
   */
  public void partitionedOutput(int reduce, K key, 
                                V value) throws IOException {
    PipesPartitioner.setNextPartition(reduce);
    collector.collect(key, value);
  }

  /**
   * Update the status message for the task.
   */
  public void status(String msg) {
    reporter.setStatus(msg);
  }

  private FloatWritable progressKey = new FloatWritable(0.0f);
  private NullWritable nullValue = NullWritable.get();
  /**
   * Update the amount done and call progress on the reporter.
   */
  public void progress(float progress) throws IOException {
    progressValue = progress;
    reporter.progress();
    
    if (recordReader != null) {
      progressKey.set(progress);
      recordReader.next(progressKey, nullValue);
    }
  }

  /**
   * The task finished successfully.
   */
  public void done() throws IOException {
    synchronized (this) {
      done = true;
      notify();
    }
  }

  /**
   * Get the current amount done.
   * @return a float between 0.0 and 1.0
   */
  public float getProgress() {
    return progressValue;
  }

  /**
   * The task failed with an exception.
   */
  public void failed(Throwable e) {
    synchronized (this) {
      exception = e;
      notify();
    }
  }

  /**
   * Wait for the task to finish or abort.
   * @return did the task finish correctly?
   * @throws Throwable
   */
  public synchronized boolean waitForFinish() throws Throwable {
    while (!done && exception == null) {
      wait();
    }
    if (exception != null) {
      throw exception;
    }
    return done;
  }

  public void registerCounter(int id, String group, String name) throws IOException {
    Counters.Counter counter = reporter.getCounter(group, name);
    registeredCounters.put(id, counter);
  }

  public void incrementCounter(int id, long amount) throws IOException {
    if (id < registeredCounters.size()) {
      Counters.Counter counter = registeredCounters.get(id);
      counter.increment(amount);
    } else {
      throw new IOException("Invalid counter with id: " + id);
    }
  }
  
  public synchronized boolean authenticate(String digest) throws IOException {
    boolean success = true;
    if (!expectedDigest.equals(digest)) {
      exception = new IOException("Authentication Failed: Expected digest="
          + expectedDigest + ", received=" + digestReceived);
      success = false;
    }
    digestReceived = true;
    notify();
    return success;
  }

  /**
   * This is called by Application and blocks the thread until
   * authentication response is received.
   * @throws IOException
   * @throws InterruptedException
   */
  synchronized void waitForAuthentication()
      throws IOException, InterruptedException {
    while (digestReceived == false && exception == null) {
      wait();
    }
    if (exception != null) {
      throw new IOException(exception.getMessage());
    }
  }
}
