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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SkipBadRecords;

import java.io.IOException;
import java.util.Iterator;

/**
 * This class is used to talk to a C++ reduce task.
 */
class PipesReducer<K2 extends WritableComparable, V2 extends Writable,
    K3 extends WritableComparable, V3 extends Writable>
    implements Reducer<K2, V2, K3, V3> {
  private static final Log LOG= LogFactory.getLog(PipesReducer.class.getName());
  private JobConf job;
  private Application<K2, V2, K3, V3> application = null;
  private DownwardProtocol<K2, V2> downlink = null;
  private boolean isOk = true;
  private boolean skipping = false;

  public void configure(JobConf job) {
    this.job = job;
    //disable the auto increment of the counter. For pipes, no of processed 
    //records could be different(equal or less) than the no of records input.
    SkipBadRecords.setAutoIncrReducerProcCount(job, false);
    skipping = job.getBoolean("mapred.skip.on", false);
  }

  /**
   * Process all of the keys and values. Start up the application if we haven't
   * started it yet.
   */
  public void reduce(K2 key, Iterator<V2> values, 
                     OutputCollector<K3, V3> output, Reporter reporter
                     ) throws IOException {
    isOk = false;
    startApplication(output, reporter);
    downlink.reduceKey(key);
    while (values.hasNext()) {
      downlink.reduceValue(values.next());
    }
    if(skipping) {
      //flush the streams on every record input if running in skip mode
      //so that we don't buffer other records surrounding a bad record.
      downlink.flush();
    }
    isOk = true;
  }

  @SuppressWarnings("unchecked")
  private void startApplication(OutputCollector<K3, V3> output, Reporter reporter) throws IOException {
    if (application == null) {
      try {
        LOG.info("starting application");
        application = 
          new Application<K2, V2, K3, V3>(
              job, null, output, reporter, 
              (Class<? extends K3>) job.getOutputKeyClass(), 
              (Class<? extends V3>) job.getOutputValueClass());
        downlink = application.getDownlink();
      } catch (InterruptedException ie) {
        throw new RuntimeException("interrupted", ie);
      }
      int reduce=0;
      downlink.runReduce(reduce, Submitter.getIsJavaRecordWriter(job));
    }
  }

  /**
   * Handle the end of the input by closing down the application.
   */
  public void close() throws IOException {
    // if we haven't started the application, we have nothing to do
    if (isOk) {
      OutputCollector<K3, V3> nullCollector = new OutputCollector<K3, V3>() {
        public void collect(K3 key, 
                            V3 value) throws IOException {
          // NULL
        }
      };
      startApplication(nullCollector, Reporter.NULL);
    }
    try {
      if (isOk) {
        application.getDownlink().endOfInput();
      } else {
        // send the abort to the application and let it clean up
        application.getDownlink().abort();
      }
      LOG.info("waiting for finish");
      application.waitForFinish();
      LOG.info("got done");
    } catch (Throwable t) {
      application.abort(t);
    } finally {
      application.cleanup();
    }
  }
}
