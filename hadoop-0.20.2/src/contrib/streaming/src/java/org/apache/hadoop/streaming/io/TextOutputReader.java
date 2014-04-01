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

package org.apache.hadoop.streaming.io;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.CharacterCodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.streaming.PipeMapRed;
import org.apache.hadoop.streaming.StreamKeyValUtil;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.UTF8ByteArrayUtils;

/**
 * OutputReader that reads the client's output as text.
 */
public class TextOutputReader extends OutputReader<Text, Text> {

  private LineReader lineReader;
  private byte[] bytes;
  private DataInput clientIn;
  private Configuration conf;
  private int numKeyFields;
  private byte[] separator;
  private Text key;
  private Text value;
  private Text line;
  
  @Override
  public void initialize(PipeMapRed pipeMapRed) throws IOException {
    super.initialize(pipeMapRed);
    clientIn = pipeMapRed.getClientInput();
    conf = pipeMapRed.getConfiguration();
    numKeyFields = pipeMapRed.getNumOfKeyFields();
    separator = pipeMapRed.getFieldSeparator();
    lineReader = new LineReader((InputStream)clientIn, conf);
    key = new Text();
    value = new Text();
    line = new Text();
  }
  
  @Override
  public boolean readKeyValue() throws IOException {
    if (lineReader.readLine(line) <= 0) {
      return false;
    }
    bytes = line.getBytes();
    splitKeyVal(bytes, line.getLength(), key, value);
    line.clear();
    return true;
  }
  
  @Override
  public Text getCurrentKey() throws IOException {
    return key;
  }
  
  @Override
  public Text getCurrentValue() throws IOException {
    return value;
  }

  @Override
  public String getLastOutput() {
    if (bytes != null) {
      try {
        return new String(bytes, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        return "<undecodable>";
      }
    } else {
      return null;
    }
  }

  // split a UTF-8 line into key and value
  private void splitKeyVal(byte[] line, int length, Text key, Text val)
    throws IOException {
    // Need to find numKeyFields separators
    int pos = UTF8ByteArrayUtils.findBytes(line, 0, length, separator);
    for(int k=1; k<numKeyFields && pos!=-1; k++) {
      pos = UTF8ByteArrayUtils.findBytes(line, pos + separator.length, 
        length, separator);
    }
    try {
      if (pos == -1) {
        key.set(line, 0, length);
        val.set("");
      } else {
        StreamKeyValUtil.splitKeyVal(line, 0, length, key, val, pos,
          separator.length);
      }
    } catch (CharacterCodingException e) {
      throw new IOException(StringUtils.stringifyException(e));
    }
  }
  
}
