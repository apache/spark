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

package org.apache.hadoop.mapreduce.lib.map;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/** A {@link Mapper} that extracts text matching a regular expression. */
public class RegexMapper<K> extends Mapper<K, Text, Text, LongWritable> {

  public static String PATTERN = "mapreduce.mapper.regex";
  public static String GROUP = "mapreduce.mapper.regexmapper..group";
  private Pattern pattern;
  private int group;

  public void setup(Context context) {
    Configuration conf = context.getConfiguration();
    pattern = Pattern.compile(conf.get(PATTERN));
    group = conf.getInt(GROUP, 0);
  }

  public void map(K key, Text value,
                  Context context)
    throws IOException, InterruptedException {
    String text = value.toString();
    Matcher matcher = pattern.matcher(text);
    while (matcher.find()) {
      context.write(new Text(matcher.group(group)), new LongWritable(1));
    }
  }
}
