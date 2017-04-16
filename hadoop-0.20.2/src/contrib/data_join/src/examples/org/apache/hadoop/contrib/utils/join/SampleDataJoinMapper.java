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

package org.apache.hadoop.contrib.utils.join;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.contrib.utils.join.DataJoinMapperBase;
import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
import org.apache.hadoop.contrib.utils.join.SampleTaggedMapOutput;

/**
 * This is a subclass of DataJoinMapperBase that is used to
 * demonstrate the functionality of INNER JOIN between 2 data
 * sources (TAB separated text files) based on the first column.
 */
public class SampleDataJoinMapper extends DataJoinMapperBase {


  protected Text generateInputTag(String inputFile) {
    // tag the row with input file name (data source)
    return new Text(inputFile);
  }

  protected Text generateGroupKey(TaggedMapOutput aRecord) {
    // first column in the input tab separated files becomes the key (to perform the JOIN)
    String line = ((Text) aRecord.getData()).toString();
    String groupKey = "";
    String[] tokens = line.split("\\t", 2);
    groupKey = tokens[0];
    return new Text(groupKey);
  }

  protected TaggedMapOutput generateTaggedMapOutput(Object value) {
    TaggedMapOutput retv = new SampleTaggedMapOutput((Text) value);
    retv.setTag(new Text(this.inputTag));
    return retv;
  }
}
