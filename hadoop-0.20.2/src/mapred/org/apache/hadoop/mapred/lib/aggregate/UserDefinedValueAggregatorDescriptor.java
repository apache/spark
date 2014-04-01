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

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

/**
 * This class implements a wrapper for a user defined value aggregator descriptor.
 * It servs two functions: One is to create an object of ValueAggregatorDescriptor from the
 * name of a user defined class that may be dynamically loaded. The other is to
 * deligate inviokations of generateKeyValPairs function to the created object.
 * 
 */
public class UserDefinedValueAggregatorDescriptor implements
    ValueAggregatorDescriptor {
  private String className;

  private ValueAggregatorDescriptor theAggregatorDescriptor = null;

  private static final Class[] argArray = new Class[] {};

  /**
   * Create an instance of the given class
   * @param className the name of the class
   * @return a dynamically created instance of the given class 
   */
  public static Object createInstance(String className) {
    Object retv = null;
    try {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      Class<?> theFilterClass = Class.forName(className, true, classLoader);
      Constructor meth = theFilterClass.getDeclaredConstructor(argArray);
      meth.setAccessible(true);
      retv = meth.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return retv;
  }

  private void createAggregator(JobConf job) {
    if (theAggregatorDescriptor == null) {
      theAggregatorDescriptor = (ValueAggregatorDescriptor) createInstance(this.className);
      theAggregatorDescriptor.configure(job);
    }
  }

  /**
   * 
   * @param className the class name of the user defined descriptor class
   * @param job a configure object used for decriptor configuration
   */
  public UserDefinedValueAggregatorDescriptor(String className, JobConf job) {
    this.className = className;
    this.createAggregator(job);
  }

  /**
   *   Generate a list of aggregation-id/value pairs for the given key/value pairs
   *   by delegating the invocation to the real object.
   *   
   * @param key
   *          input key
   * @param val
   *          input value
   * @return a list of aggregation id/value pairs. An aggregation id encodes an
   *         aggregation type which is used to guide the way to aggregate the
   *         value in the reduce/combiner phrase of an Aggregate based job.
   */
  public ArrayList<Entry<Text, Text>> generateKeyValPairs(Object key,
                                                          Object val) {
    ArrayList<Entry<Text, Text>> retv = new ArrayList<Entry<Text, Text>>();
    if (this.theAggregatorDescriptor != null) {
      retv = this.theAggregatorDescriptor.generateKeyValPairs(key, val);
    }
    return retv;
  }

  /**
   * @return the string representation of this object.
   */
  public String toString() {
    return "UserDefinedValueAggregatorDescriptor with class name:" + "\t"
      + this.className;
  }

  /**
   *  Do nothing.
   */
  public void configure(JobConf job) {

  }

}
