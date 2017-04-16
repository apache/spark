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
package org.apache.hadoop.metrics.util;

import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.util.StringUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The MetricsIntValue class is for a metric that is not time varied
 * but changes only when it is set. 
 * Each time its value is set, it is published only *once* at the next update
 * call.
 *
 */
public class MetricsIntValue extends MetricsBase {  

  private static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.metrics.util");

  private int value;
  private boolean changed;
  
  
  /**
   * Constructor - create a new metric
   * @param nam the name of the metrics to be used to publish the metric
   * @param registry - where the metrics object will be registered
   */
  public MetricsIntValue(final String nam, final MetricsRegistry registry, final String description) {
    super(nam, description);
    value = 0;
    changed = false;
    registry.add(nam, this);
  }
  
  /**
   * Constructor - create a new metric
   * @param nam the name of the metrics to be used to publish the metric
   * @param registry - where the metrics object will be registered
   * A description of {@link #NO_DESCRIPTION} is used
   */
  public MetricsIntValue(final String nam, MetricsRegistry registry) {
    this(nam, registry, NO_DESCRIPTION);
  }
  
  
  
  /**
   * Set the value
   * @param newValue
   */
  public synchronized void set(final int newValue) {
    value = newValue;
    changed = true;
  }
  
  /**
   * Get value
   * @return the value last set
   */
  public synchronized int get() { 
    return value;
  } 
  

  /**
   * Push the metric to the mr.
   * The metric is pushed only if it was updated since last push
   * 
   * Note this does NOT push to JMX
   * (JMX gets the info via {@link #get()}
   *
   * @param mr
   */
  public synchronized void pushMetric(final MetricsRecord mr) {
    if (changed) {
      try {
        mr.setMetric(getName(), value);
      } catch (Exception e) {
        LOG.info("pushMetric failed for " + getName() + "\n" +
            StringUtils.stringifyException(e));
      }
    }
    changed = false;
  }
}
