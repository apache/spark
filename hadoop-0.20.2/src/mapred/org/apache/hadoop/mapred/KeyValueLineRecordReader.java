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

package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * This class treats a line in the input as a key/value pair separated by a 
 * separator character. The separator can be specified in config file 
 * under the attribute name key.value.separator.in.input.line. The default
 * separator is the tab character ('\t').
 */
public class KeyValueLineRecordReader implements RecordReader<Text, Text> {
  
  private final LineRecordReader lineRecordReader;

  private byte separator = (byte) '\t';

  private LongWritable dummyKey;

  private Text innerValue;

  public Class getKeyClass() { return Text.class; }
  
  public Text createKey() {
    return new Text();
  }
  
  public Text createValue() {
    return new Text();
  }

  public KeyValueLineRecordReader(Configuration job, FileSplit split)
    throws IOException {
    
    lineRecordReader = new LineRecordReader(job, split);
    dummyKey = lineRecordReader.createKey();
    innerValue = lineRecordReader.createValue();
    String sepStr = job.get("key.value.separator.in.input.line", "\t");
    this.separator = (byte) sepStr.charAt(0);
  }

  public static int findSeparator(byte[] utf, int start, int length, 
      byte sep) {
    return org.apache.hadoop.mapreduce.lib.input.
      KeyValueLineRecordReader.findSeparator(utf, start, length, sep);
  }

  /** Read key/value pair in a line. */
  public synchronized boolean next(Text key, Text value)
    throws IOException {
    byte[] line = null;
    int lineLen = -1;
    if (lineRecordReader.next(dummyKey, innerValue)) {
      line = innerValue.getBytes();
      lineLen = innerValue.getLength();
    } else {
      return false;
    }
    if (line == null)
      return false;
    int pos = findSeparator(line, 0, lineLen, this.separator);
    org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader.
      setKeyValue(key, value, line, lineLen, pos);
    return true;
  }
  
  public float getProgress() {
    return lineRecordReader.getProgress();
  }
  
  public synchronized long getPos() throws IOException {
    return lineRecordReader.getPos();
  }

  public synchronized void close() throws IOException { 
    lineRecordReader.close();
  }
}
