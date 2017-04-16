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

package org.apache.hadoop.mapred.lib.aggregate;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;

/**
 * This abstract class implements some common functionalities of the
 * the generic mapper, reducer and combiner classes of Aggregate.
 */
public abstract class ValueAggregatorJobBase<K1 extends WritableComparable,
                                             V1 extends Writable>
  implements Mapper<K1, V1, Text, Text>, Reducer<Text, Text, Text, Text> {

  protected ArrayList<ValueAggregatorDescriptor> aggregatorDescriptorList = null;

  public void configure(JobConf job) {
    this.initializeMySpec(job);
    this.logSpec();
  }

  private static ValueAggregatorDescriptor getValueAggregatorDescriptor(
      String spec, JobConf job) {
    if (spec == null)
      return null;
    String[] segments = spec.split(",", -1);
    String type = segments[0];
    if (type.compareToIgnoreCase("UserDefined") == 0) {
      String className = segments[1];
      return new UserDefinedValueAggregatorDescriptor(className, job);
    }
    return null;
  }

  private static ArrayList<ValueAggregatorDescriptor> getAggregatorDescriptors(JobConf job) {
    String advn = "aggregator.descriptor";
    int num = job.getInt(advn + ".num", 0);
    ArrayList<ValueAggregatorDescriptor> retv = new ArrayList<ValueAggregatorDescriptor>(num);
    for (int i = 0; i < num; i++) {
      String spec = job.get(advn + "." + i);
      ValueAggregatorDescriptor ad = getValueAggregatorDescriptor(spec, job);
      if (ad != null) {
        retv.add(ad);
      }
    }
    return retv;
  }

  private void initializeMySpec(JobConf job) {
    this.aggregatorDescriptorList = getAggregatorDescriptors(job);
    if (this.aggregatorDescriptorList.size() == 0) {
      this.aggregatorDescriptorList
          .add(new UserDefinedValueAggregatorDescriptor(
              ValueAggregatorBaseDescriptor.class.getCanonicalName(), job));
    }
  }

  protected void logSpec() {

  }

  public void close() throws IOException {
  }
}
