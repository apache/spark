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

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;

/**
 * A common base implementing some statics collecting mechanisms that are
 * commonly used in a typical map/reduce job.
 * 
 */
public abstract class JobBase implements Mapper, Reducer {

  public static final Log LOG = LogFactory.getLog("datajoin.job");

  private SortedMap<Object, Long> longCounters = null;

  private SortedMap<Object, Double> doubleCounters = null;

  /**
   * Set the given counter to the given value
   * 
   * @param name
   *          the counter name
   * @param value
   *          the value for the counter
   */
  protected void setLongValue(Object name, long value) {
    this.longCounters.put(name, new Long(value));
  }

  /**
   * Set the given counter to the given value
   * 
   * @param name
   *          the counter name
   * @param value
   *          the value for the counter
   */
  protected void setDoubleValue(Object name, double value) {
    this.doubleCounters.put(name, new Double(value));
  }

  /**
   * 
   * @param name
   *          the counter name
   * @return return the value of the given counter.
   */
  protected Long getLongValue(Object name) {
    return this.longCounters.get(name);
  }

  /**
   * 
   * @param name
   *          the counter name
   * @return return the value of the given counter.
   */
  protected Double getDoubleValue(Object name) {
    return this.doubleCounters.get(name);
  }

  /**
   * Increment the given counter by the given incremental value If the counter
   * does not exist, one is created with value 0.
   * 
   * @param name
   *          the counter name
   * @param inc
   *          the incremental value
   * @return the updated value.
   */
  protected Long addLongValue(Object name, long inc) {
    Long val = this.longCounters.get(name);
    Long retv = null;
    if (val == null) {
      retv = new Long(inc);
    } else {
      retv = new Long(val.longValue() + inc);
    }
    this.longCounters.put(name, retv);
    return retv;
  }

  /**
   * Increment the given counter by the given incremental value If the counter
   * does not exist, one is created with value 0.
   * 
   * @param name
   *          the counter name
   * @param inc
   *          the incremental value
   * @return the updated value.
   */
  protected Double addDoubleValue(Object name, double inc) {
    Double val = this.doubleCounters.get(name);
    Double retv = null;
    if (val == null) {
      retv = new Double(inc);
    } else {
      retv = new Double(val.doubleValue() + inc);
    }
    this.doubleCounters.put(name, retv);
    return retv;
  }

  /**
   * log the counters
   * 
   */
  protected void report() {
    LOG.info(getReport());
  }

  /**
   * log the counters
   * 
   */
  protected String getReport() {
    StringBuffer sb = new StringBuffer();

    Iterator iter = this.longCounters.entrySet().iterator();
    while (iter.hasNext()) {
      Entry e = (Entry) iter.next();
      sb.append(e.getKey().toString()).append("\t").append(e.getValue())
        .append("\n");
    }
    iter = this.doubleCounters.entrySet().iterator();
    while (iter.hasNext()) {
      Entry e = (Entry) iter.next();
      sb.append(e.getKey().toString()).append("\t").append(e.getValue())
        .append("\n");
    }
    return sb.toString();
  }

  /**
   * Initializes a new instance from a {@link JobConf}.
   * 
   * @param job
   *          the configuration
   */
  public void configure(JobConf job) {
    this.longCounters = new TreeMap<Object, Long>();
    this.doubleCounters = new TreeMap<Object, Double>();
  }
}
