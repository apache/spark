/*
 * MetricsRecordImpl.java
 *
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

package org.apache.hadoop.metrics.spi;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.spi.AbstractMetricsContext.TagMap;

/**
 * An implementation of MetricsRecord.  Keeps a back-pointer to the context
 * from which it was created, and delegates back to it on <code>update</code>
 * and <code>remove()</code>.
 */
public class MetricsRecordImpl implements MetricsRecord {
    
  private TagMap tagTable = new TagMap();
  private Map<String,MetricValue> metricTable = new LinkedHashMap<String,MetricValue>();
    
  private String recordName;
  private AbstractMetricsContext context;
    
    
  /** Creates a new instance of FileRecord */
  protected MetricsRecordImpl(String recordName, AbstractMetricsContext context)
  {
    this.recordName = recordName;
    this.context = context;
  }
    
  /**
   * Returns the record name. 
   *
   * @return the record name
   */
  public String getRecordName() {
    return recordName;
  }
    
  /**
   * Sets the named tag to the specified value.
   *
   * @param tagName name of the tag
   * @param tagValue new value of the tag
   * @throws MetricsException if the tagName conflicts with the configuration
   */
  public void setTag(String tagName, String tagValue) {
    if (tagValue == null) {
      tagValue = "";
    }
    tagTable.put(tagName, tagValue);
  }
    
  /**
   * Sets the named tag to the specified value.
   *
   * @param tagName name of the tag
   * @param tagValue new value of the tag
   * @throws MetricsException if the tagName conflicts with the configuration
   */
  public void setTag(String tagName, int tagValue) {
    tagTable.put(tagName, Integer.valueOf(tagValue));
  }
    
  /**
   * Sets the named tag to the specified value.
   *
   * @param tagName name of the tag
   * @param tagValue new value of the tag
   * @throws MetricsException if the tagName conflicts with the configuration
   */
  public void setTag(String tagName, long tagValue) {
    tagTable.put(tagName, Long.valueOf(tagValue));
  }
    
  /**
   * Sets the named tag to the specified value.
   *
   * @param tagName name of the tag
   * @param tagValue new value of the tag
   * @throws MetricsException if the tagName conflicts with the configuration
   */
  public void setTag(String tagName, short tagValue) {
    tagTable.put(tagName, Short.valueOf(tagValue));
  }
    
  /**
   * Sets the named tag to the specified value.
   *
   * @param tagName name of the tag
   * @param tagValue new value of the tag
   * @throws MetricsException if the tagName conflicts with the configuration
   */
  public void setTag(String tagName, byte tagValue) {
    tagTable.put(tagName, Byte.valueOf(tagValue));
  }
    
  /**
   * Removes any tag of the specified name.
   */
  public void removeTag(String tagName) {
    tagTable.remove(tagName);
  }
  
  /**
   * Sets the named metric to the specified value.
   *
   * @param metricName name of the metric
   * @param metricValue new value of the metric
   * @throws MetricsException if the metricName or the type of the metricValue 
   * conflicts with the configuration
   */
  public void setMetric(String metricName, int metricValue) {
    setAbsolute(metricName, Integer.valueOf(metricValue));
  }
    
  /**
   * Sets the named metric to the specified value.
   *
   * @param metricName name of the metric
   * @param metricValue new value of the metric
   * @throws MetricsException if the metricName or the type of the metricValue 
   * conflicts with the configuration
   */
  public void setMetric(String metricName, long metricValue) {
    setAbsolute(metricName, Long.valueOf(metricValue));
  }
    
  /**
   * Sets the named metric to the specified value.
   *
   * @param metricName name of the metric
   * @param metricValue new value of the metric
   * @throws MetricsException if the metricName or the type of the metricValue 
   * conflicts with the configuration
   */
  public void setMetric(String metricName, short metricValue) {
    setAbsolute(metricName, Short.valueOf(metricValue));
  }
    
  /**
   * Sets the named metric to the specified value.
   *
   * @param metricName name of the metric
   * @param metricValue new value of the metric
   * @throws MetricsException if the metricName or the type of the metricValue 
   * conflicts with the configuration
   */
  public void setMetric(String metricName, byte metricValue) {
    setAbsolute(metricName, Byte.valueOf(metricValue));
  }
    
  /**
   * Sets the named metric to the specified value.
   *
   * @param metricName name of the metric
   * @param metricValue new value of the metric
   * @throws MetricsException if the metricName or the type of the metricValue 
   * conflicts with the configuration
   */
  public void setMetric(String metricName, float metricValue) {
    setAbsolute(metricName, new Float(metricValue));
  }
    
  /**
   * Increments the named metric by the specified value.
   *
   * @param metricName name of the metric
   * @param metricValue incremental value
   * @throws MetricsException if the metricName or the type of the metricValue 
   * conflicts with the configuration
   */
  public void incrMetric(String metricName, int metricValue) {
    setIncrement(metricName, Integer.valueOf(metricValue));
  }
    
  /**
   * Increments the named metric by the specified value.
   *
   * @param metricName name of the metric
   * @param metricValue incremental value
   * @throws MetricsException if the metricName or the type of the metricValue 
   * conflicts with the configuration
   */
  public void incrMetric(String metricName, long metricValue) {
    setIncrement(metricName, Long.valueOf(metricValue));
  }
    
  /**
   * Increments the named metric by the specified value.
   *
   * @param metricName name of the metric
   * @param metricValue incremental value
   * @throws MetricsException if the metricName or the type of the metricValue 
   * conflicts with the configuration
   */
  public void incrMetric(String metricName, short metricValue) {
    setIncrement(metricName, Short.valueOf(metricValue));
  }
    
  /**
   * Increments the named metric by the specified value.
   *
   * @param metricName name of the metric
   * @param metricValue incremental value
   * @throws MetricsException if the metricName or the type of the metricValue 
   * conflicts with the configuration
   */
  public void incrMetric(String metricName, byte metricValue) {
    setIncrement(metricName, Byte.valueOf(metricValue));
  }
    
  /**
   * Increments the named metric by the specified value.
   *
   * @param metricName name of the metric
   * @param metricValue incremental value
   * @throws MetricsException if the metricName or the type of the metricValue 
   * conflicts with the configuration
   */
  public void incrMetric(String metricName, float metricValue) {
    setIncrement(metricName, new Float(metricValue));
  }
    
  private void setAbsolute(String metricName, Number metricValue) {
    metricTable.put(metricName, new MetricValue(metricValue, MetricValue.ABSOLUTE));
  }
    
  private void setIncrement(String metricName, Number metricValue) {
    metricTable.put(metricName, new MetricValue(metricValue, MetricValue.INCREMENT));
  }
    
  /**
   * Updates the table of buffered data which is to be sent periodically.
   * If the tag values match an existing row, that row is updated; 
   * otherwise, a new row is added.
   */
  public void update() {
    context.update(this);
  }
    
  /**
   * Removes the row, if it exists, in the buffered data table having tags 
   * that equal the tags that have been set on this record. 
   */
  public void remove() {
    context.remove(this);
  }

  TagMap getTagTable() {
    return tagTable;
  }

  Map<String, MetricValue> getMetricTable() {
    return metricTable;
  }
}
