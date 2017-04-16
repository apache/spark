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

package org.apache.hadoop.contrib.index.mapred;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.contrib.index.lucene.ShardWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Closeable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * This reducer applies to a shard the changes for it. A "new version" of
 * a shard is created at the end of a reduce. It is important to note that
 * the new version of the shard is not derived from scratch. By leveraging
 * Lucene's update algorithm, the new version of each Lucene instance will
 * share as many files as possible as the previous version. 
 */
public class IndexUpdateReducer extends MapReduceBase implements
    Reducer<Shard, IntermediateForm, Shard, Text> {
  static final Log LOG = LogFactory.getLog(IndexUpdateReducer.class);
  static final Text DONE = new Text("done");

  /**
   * Get the reduce output key class.
   * @return the reduce output key class
   */
  public static Class<? extends WritableComparable> getOutputKeyClass() {
    return Shard.class;
  }

  /**
   * Get the reduce output value class.
   * @return the reduce output value class
   */
  public static Class<? extends Writable> getOutputValueClass() {
    return Text.class;
  }

  private IndexUpdateConfiguration iconf;
  private String mapredTempDir;

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
   */
  public void reduce(Shard key, Iterator<IntermediateForm> values,
      OutputCollector<Shard, Text> output, Reporter reporter)
      throws IOException {

    LOG.info("Construct a shard writer for " + key);
    FileSystem fs = FileSystem.get(iconf.getConfiguration());
    String temp =
        mapredTempDir + Path.SEPARATOR + "shard_" + System.currentTimeMillis();
    final ShardWriter writer = new ShardWriter(fs, key, temp, iconf);

    // update the shard
    while (values.hasNext()) {
      IntermediateForm form = values.next();
      writer.process(form);
      reporter.progress();
    }

    // close the shard
    final Reporter fReporter = reporter;
    new Closeable() {
      volatile boolean closed = false;

      public void close() throws IOException {
        // spawn a thread to give progress heartbeats
        Thread prog = new Thread() {
          public void run() {
            while (!closed) {
              try {
                fReporter.setStatus("closing");
                Thread.sleep(1000);
              } catch (InterruptedException e) {
                continue;
              } catch (Throwable e) {
                return;
              }
            }
          }
        };

        try {
          prog.start();

          if (writer != null) {
            writer.close();
          }
        } finally {
          closed = true;
        }
      }
    }.close();
    LOG.info("Closed the shard writer for " + key + ", writer = " + writer);

    output.collect(key, DONE);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapred.MapReduceBase#configure(org.apache.hadoop.mapred.JobConf)
   */
  public void configure(JobConf job) {
    iconf = new IndexUpdateConfiguration(job);
    mapredTempDir = iconf.getMapredTempDir();
    mapredTempDir = Shard.normalizePath(mapredTempDir);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapred.MapReduceBase#close()
   */
  public void close() throws IOException {
  }

}
