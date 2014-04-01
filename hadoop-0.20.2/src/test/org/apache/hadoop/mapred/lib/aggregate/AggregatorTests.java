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

import org.apache.hadoop.io.Text;
import java.util.ArrayList;
import java.util.Map.Entry;

public class AggregatorTests extends ValueAggregatorBaseDescriptor {
  
  public ArrayList<Entry<Text, Text>> generateKeyValPairs(Object key, Object val) {
    ArrayList<Entry<Text, Text>> retv = new ArrayList<Entry<Text, Text>>();
    String [] words = val.toString().split(" ");
    
    String countType;
    String id;
    Entry<Text, Text> e;
    
    for (String word: words) {
      long numVal = Long.parseLong(word);
      countType = LONG_VALUE_SUM;
      id = "count_" + word;
      e = generateEntry(countType, id, ONE);
      if (e != null) {
        retv.add(e);
      }
      countType = LONG_VALUE_MAX;
      id = "max";
      e = generateEntry(countType, id, new Text(word));
      if (e != null) {
        retv.add(e);
      }
      
      countType = LONG_VALUE_MIN;
      id = "min";
      e = generateEntry(countType, id, new Text(word));
      if (e != null) {
        retv.add(e);
      }
      
      countType = STRING_VALUE_MAX;
      id = "value_as_string_max";
      e = generateEntry(countType, id, new Text(""+numVal));
      if (e != null) {
        retv.add(e);
      }
      
      countType = STRING_VALUE_MIN;
      id = "value_as_string_min";
      e = generateEntry(countType, id, new Text(""+numVal));
      if (e != null) {
        retv.add(e);
      }
      
      countType = UNIQ_VALUE_COUNT;
      id = "uniq_count";
      e = generateEntry(countType, id, new Text(word));
      if (e != null) {
        retv.add(e);
      }
      
      countType = VALUE_HISTOGRAM;
      id = "histogram";
      e = generateEntry(countType, id, new Text(word));
      if (e != null) {
        retv.add(e);
      }
    }
    return retv;
  }

}
