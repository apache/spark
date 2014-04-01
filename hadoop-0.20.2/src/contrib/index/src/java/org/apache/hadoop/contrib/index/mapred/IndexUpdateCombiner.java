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
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * This combiner combines multiple intermediate forms into one intermediate
 * form. More specifically, the input intermediate forms are a single-document
 * ram index and/or a single delete term. An output intermediate form contains
 * a multi-document ram index and/or multiple delete terms.   
 */
public class IndexUpdateCombiner extends MapReduceBase implements
    Reducer<Shard, IntermediateForm, Shard, IntermediateForm> {
  static final Log LOG = LogFactory.getLog(IndexUpdateCombiner.class);

  IndexUpdateConfiguration iconf;

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
   */
  public void reduce(Shard key, Iterator<IntermediateForm> values,
      OutputCollector<Shard, IntermediateForm> output, Reporter reporter)
      throws IOException {

    LOG.info("Construct a form writer for " + key);
    IntermediateForm form = new IntermediateForm();
    form.configure(iconf);
    while (values.hasNext()) {
      IntermediateForm singleDocForm = values.next();
      form.process(singleDocForm);
    }
    form.closeWriter();
    LOG.info("Closed the form writer for " + key + ", form = " + form);

    output.collect(key, form);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapred.MapReduceBase#configure(org.apache.hadoop.mapred.JobConf)
   */
  public void configure(JobConf job) {
    iconf = new IndexUpdateConfiguration(job);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapred.MapReduceBase#close()
   */
  public void close() throws IOException {
  }

}
