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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * This class implements the generic reducer of Aggregate.
 * 
 * 
 */
public class ValueAggregatorReducer<K1 extends WritableComparable,
                                    V1 extends Writable>
  extends ValueAggregatorJobBase<K1, V1> {

  /**
   * @param key
   *          the key is expected to be a Text object, whose prefix indicates
   *          the type of aggregation to aggregate the values. In effect, data
   *          driven computing is achieved. It is assumed that each aggregator's
   *          getReport method emits appropriate output for the aggregator. This
   *          may be further customiized.
   * @value the values to be aggregated
   */
  public void reduce(Text key, Iterator<Text> values,
                     OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    String keyStr = key.toString();
    int pos = keyStr.indexOf(ValueAggregatorDescriptor.TYPE_SEPARATOR);
    String type = keyStr.substring(0, pos);
    keyStr = keyStr.substring(pos
                              + ValueAggregatorDescriptor.TYPE_SEPARATOR.length());

    ValueAggregator aggregator = ValueAggregatorBaseDescriptor
      .generateValueAggregator(type);
    while (values.hasNext()) {
      aggregator.addNextValue(values.next());
    }

    String val = aggregator.getReport();
    key = new Text(keyStr);
    output.collect(key, new Text(val));
  }

  /**
   * Do nothing. Should not be called
   */
  public void map(K1 arg0, V1 arg1, OutputCollector<Text, Text> arg2,
                  Reporter arg3) throws IOException {
    throw new IOException ("should not be called\n");
  }
}
