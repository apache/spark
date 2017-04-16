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

import org.apache.hadoop.contrib.utils.join.DataJoinReducerBase;
import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;

/**
 * This is a subclass of DataJoinReducerBase that is used to
 * demonstrate the functionality of INNER JOIN between 2 data
 * sources (TAB separated text files) based on the first column.
 */
public class SampleDataJoinReducer extends DataJoinReducerBase {

  /**
   * 
   * @param tags
   *          a list of source tags
   * @param values
   *          a value per source
   * @return combined value derived from values of the sources
   */
  protected TaggedMapOutput combine(Object[] tags, Object[] values) {
    // eliminate rows which didnot match in one of the two tables (for INNER JOIN)
    if (tags.length < 2)
       return null;  
    String joinedStr = ""; 
    for (int i=0; i<tags.length; i++) {
      if (i > 0)
         joinedStr += "\t";
      // strip first column as it is the key on which we joined
      String line = ((Text) (((TaggedMapOutput) values[i]).getData())).toString();
      String[] tokens = line.split("\\t", 2);
      joinedStr += tokens[1];
    }
    TaggedMapOutput retv = new SampleTaggedMapOutput(new Text(joinedStr));
    retv.setTag((Text) tags[0]); 
    return retv;
  }
}
