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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * An {@link InputSplit} that tags another InputSplit with extra data for use by
 * {@link DelegatingInputFormat}s and {@link DelegatingMapper}s.
 */
class TaggedInputSplit implements Configurable, InputSplit {

  private Class<? extends InputSplit> inputSplitClass;

  private InputSplit inputSplit;

  private Class<? extends InputFormat> inputFormatClass;

  private Class<? extends Mapper> mapperClass;

  private Configuration conf;

  public TaggedInputSplit() {
    // Default constructor.
  }

  /**
   * Creates a new TaggedInputSplit.
   * 
   * @param inputSplit The InputSplit to be tagged
   * @param conf The configuration to use
   * @param inputFormatClass The InputFormat class to use for this job
   * @param mapperClass The Mapper class to use for this job
   */
  public TaggedInputSplit(InputSplit inputSplit, Configuration conf,
      Class<? extends InputFormat> inputFormatClass,
      Class<? extends Mapper> mapperClass) {
    this.inputSplitClass = inputSplit.getClass();
    this.inputSplit = inputSplit;
    this.conf = conf;
    this.inputFormatClass = inputFormatClass;
    this.mapperClass = mapperClass;
  }

  /**
   * Retrieves the original InputSplit.
   * 
   * @return The InputSplit that was tagged
   */
  public InputSplit getInputSplit() {
    return inputSplit;
  }

  /**
   * Retrieves the InputFormat class to use for this split.
   * 
   * @return The InputFormat class to use
   */
  public Class<? extends InputFormat> getInputFormatClass() {
    return inputFormatClass;
  }

  /**
   * Retrieves the Mapper class to use for this split.
   * 
   * @return The Mapper class to use
   */
  public Class<? extends Mapper> getMapperClass() {
    return mapperClass;
  }

  public long getLength() throws IOException {
    return inputSplit.getLength();
  }

  public String[] getLocations() throws IOException {
    return inputSplit.getLocations();
  }

  @SuppressWarnings("unchecked")
  public void readFields(DataInput in) throws IOException {
    inputSplitClass = (Class<? extends InputSplit>) readClass(in);
    inputSplit = (InputSplit) ReflectionUtils
       .newInstance(inputSplitClass, conf);
    inputSplit.readFields(in);
    inputFormatClass = (Class<? extends InputFormat>) readClass(in);
    mapperClass = (Class<? extends Mapper>) readClass(in);
  }

  private Class<?> readClass(DataInput in) throws IOException {
    String className = Text.readString(in);
    try {
      return conf.getClassByName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("readObject can't find class", e);
    }
  }

  public void write(DataOutput out) throws IOException {
    Text.writeString(out, inputSplitClass.getName());
    inputSplit.write(out);
    Text.writeString(out, inputFormatClass.getName());
    Text.writeString(out, mapperClass.getName());
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

}
