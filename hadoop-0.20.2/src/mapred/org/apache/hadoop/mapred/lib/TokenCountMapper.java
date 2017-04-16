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

package org.apache.hadoop.mapred.lib;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


/** A {@link Mapper} that maps text values into <token,freq> pairs.  Uses
 * {@link StringTokenizer} to break text into tokens. 
 */
public class TokenCountMapper<K> extends MapReduceBase
    implements Mapper<K, Text, Text, LongWritable> {

  public void map(K key, Text value,
                  OutputCollector<Text, LongWritable> output,
                  Reporter reporter)
    throws IOException {
    // get input text
    String text = value.toString();       // value is line of text

    // tokenize the value
    StringTokenizer st = new StringTokenizer(text);
    while (st.hasMoreTokens()) {
      // output <token,1> pairs
      output.collect(new Text(st.nextToken()), new LongWritable(1));
    }  
  }
  
}
