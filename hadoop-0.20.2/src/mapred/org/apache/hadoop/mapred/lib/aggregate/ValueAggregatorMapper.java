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
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * This class implements the generic mapper of Aggregate.
 */
public class ValueAggregatorMapper<K1 extends WritableComparable,
                                   V1 extends Writable>
  extends ValueAggregatorJobBase<K1, V1> {

  /**
   *  the map function. It iterates through the value aggregator descriptor 
   *  list to generate aggregation id/value pairs and emit them.
   */
  public void map(K1 key, V1 value,
                  OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

    Iterator iter = this.aggregatorDescriptorList.iterator();
    while (iter.hasNext()) {
      ValueAggregatorDescriptor ad = (ValueAggregatorDescriptor) iter.next();
      Iterator<Entry<Text, Text>> ens =
        ad.generateKeyValPairs(key, value).iterator();
      while (ens.hasNext()) {
        Entry<Text, Text> en = ens.next();
        output.collect(en.getKey(), en.getValue());
      }
    }
  }

  /**
   * Do nothing. Should not be called.
   */
  public void reduce(Text arg0, Iterator<Text> arg1,
                     OutputCollector<Text, Text> arg2,
                     Reporter arg3) throws IOException {
    throw new IOException("should not be called\n");
  }
}
