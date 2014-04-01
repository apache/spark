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
import java.util.Iterator;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.Arrays;


/**
 * This class implements a value aggregator that computes the 
 * histogram of a sequence of strings.
 * 
 */
public class ValueHistogram implements ValueAggregator {

  TreeMap<Object, Object> items = null;

  public ValueHistogram() {
    items = new TreeMap<Object, Object>();
  }

  /**
   * add the given val to the aggregator.
   * 
   * @param val the value to be added. It is expected to be a string
   * in the form of xxxx\tnum, meaning xxxx has num occurrences.
   */
  public void addNextValue(Object val) {
    String valCountStr = val.toString();
    int pos = valCountStr.lastIndexOf("\t");
    String valStr = valCountStr;
    String countStr = "1";
    if (pos >= 0) {
      valStr = valCountStr.substring(0, pos);
      countStr = valCountStr.substring(pos + 1);
    }
    
    Long count = (Long) this.items.get(valStr);
    long inc = Long.parseLong(countStr);

    if (count == null) {
      count = inc;
    } else {
      count = count.longValue() + inc;
    }
    items.put(valStr, count);
  }

  /**
   * @return the string representation of this aggregator.
   * It includes the following basic statistics of the histogram:
   *    the number of unique values
   *    the minimum value
   *    the media value
   *    the maximum value
   *    the average value
   *    the standard deviation
   */
  public String getReport() {
    long[] counts = new long[items.size()];

    StringBuffer sb = new StringBuffer();
    Iterator iter = items.values().iterator();
    int i = 0;
    while (iter.hasNext()) {
      Long count = (Long) iter.next();
      counts[i] = count.longValue();
      i += 1;
    }
    Arrays.sort(counts);
    sb.append(counts.length);
    i = 0;
    long acc = 0;
    while (i < counts.length) {
      long nextVal = counts[i];
      int j = i + 1;
      while (j < counts.length && counts[j] == nextVal) {
        j++;
      }
      acc += nextVal * (j - i);
      //sbVal.append("\t").append(nextVal).append("\t").append(j - i)
      //.append("\n");
      i = j;
    }
    double average = 0.0;
    double sd = 0.0;
    if (counts.length > 0) {
      sb.append("\t").append(counts[0]);
      sb.append("\t").append(counts[counts.length / 2]);
      sb.append("\t").append(counts[counts.length - 1]);

      average = acc * 1.0 / counts.length;
      sb.append("\t").append(average);

      i = 0;
      while (i < counts.length) {
        double nextDiff = counts[i] - average;
        sd += nextDiff * nextDiff;
        i += 1;
      }
      sd = Math.sqrt(sd / counts.length);

      sb.append("\t").append(sd);

    }
    //sb.append("\n").append(sbVal.toString());
    return sb.toString();
  }

  /** 
   * 
   * @return a string representation of the list of value/frequence pairs of 
   * the histogram
   */
  public String getReportDetails() {
    StringBuffer sb = new StringBuffer();
    Iterator iter = items.entrySet().iterator();
    while (iter.hasNext()) {
      Entry en = (Entry) iter.next();
      Object val = en.getKey();
      Long count = (Long) en.getValue();
      sb.append("\t").append(val.toString()).append("\t").append(
                                                                 count.longValue()).append("\n");
    }
    return sb.toString();
  }

  /**
   *  @return a list value/frequence pairs.
   *  The return value is expected to be used by the reducer.
   */
  public ArrayList getCombinerOutput() {
    ArrayList<String> retv = new ArrayList<String>();
    Iterator iter = items.entrySet().iterator();

    while (iter.hasNext()) {
      Entry en = (Entry) iter.next();
      Object val = en.getKey();
      Long count = (Long) en.getValue();
      retv.add(val.toString() + "\t" + count.longValue());
    }
    return retv;
  }

  /** 
   * 
   * @return a TreeMap representation of the histogram
   */
  public TreeMap getReportItems() {
    return items;
  }

  /** 
   * reset the aggregator
   */
  public void reset() {
    items = new TreeMap<Object, Object>();
  }

}
