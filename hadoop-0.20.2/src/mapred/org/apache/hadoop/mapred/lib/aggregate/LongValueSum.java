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

import java.util.ArrayList;

/**
 * This class implements a value aggregator that sums up 
 * a sequence of long values.
 * 
 */
public class LongValueSum implements ValueAggregator {

  long sum = 0;
    
  /**
   *  the default constructor
   *
   */
  public LongValueSum() {
    reset();
  }

  /**
   * add a value to the aggregator
   * 
   * @param val
   *          an object whose string representation represents a long value.
   * 
   */
  public void addNextValue(Object val) {
    this.sum += Long.parseLong(val.toString());
  }
    
  /**
   * add a value to the aggregator
   * 
   * @param val
   *          a long value.
   * 
   */
  public void addNextValue(long val) {
    this.sum += val;
  }
    
  /**
   * @return the aggregated value
   */
  public long getSum() {
    return this.sum;
  }
    
  /**
   * @return the string representation of the aggregated value
   */
  public String getReport() {
    return ""+sum;
  }

  /**
   * reset the aggregator
   */
  public void reset() {
    sum = 0;
  }

  /**
   * @return return an array of one element. The element is a string
   *         representation of the aggregated value. The return value is
   *         expected to be used by the a combiner.
   */
  public ArrayList<String> getCombinerOutput() {
    ArrayList<String> retv = new ArrayList<String>(1);
    retv.add(""+sum);
    return retv;
  }
}


