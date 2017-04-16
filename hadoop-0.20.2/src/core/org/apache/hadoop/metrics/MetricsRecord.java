/*
 * MetricsRecord.java
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

package org.apache.hadoop.metrics;

/**
 * A named and optionally tagged set of records to be sent to the metrics
 * system. <p/>
 *
 * A record name identifies the kind of data to be reported. For example, a
 * program reporting statistics relating to the disks on a computer might use
 * a record name "diskStats".<p/>
 *
 * A record has zero or more <i>tags</i>. A tag has a name and a value. To
 * continue the example, the "diskStats" record might use a tag named
 * "diskName" to identify a particular disk.  Sometimes it is useful to have
 * more than one tag, so there might also be a "diskType" with value "ide" or
 * "scsi" or whatever.<p/>
 *
 * A record also has zero or more <i>metrics</i>.  These are the named
 * values that are to be reported to the metrics system.  In the "diskStats"
 * example, possible metric names would be "diskPercentFull", "diskPercentBusy", 
 * "kbReadPerSecond", etc.<p/>
 * 
 * The general procedure for using a MetricsRecord is to fill in its tag and
 * metric values, and then call <code>update()</code> to pass the record to the
 * client library.
 * Metric data is not immediately sent to the metrics system
 * each time that <code>update()</code> is called. 
 * An internal table is maintained, identified by the record name. This
 * table has columns 
 * corresponding to the tag and the metric names, and rows 
 * corresponding to each unique set of tag values. An update
 * either modifies an existing row in the table, or adds a new row with a set of
 * tag values that are different from all the other rows.  Note that if there
 * are no tags, then there can be at most one row in the table. <p/>
 * 
 * Once a row is added to the table, its data will be sent to the metrics system 
 * on every timer period, whether or not it has been updated since the previous
 * timer period.  If this is inappropriate, for example if metrics were being
 * reported by some transient object in an application, the <code>remove()</code>
 * method can be used to remove the row and thus stop the data from being
 * sent.<p/>
 *
 * Note that the <code>update()</code> method is atomic.  This means that it is
 * safe for different threads to be updating the same metric.  More precisely,
 * it is OK for different threads to call <code>update()</code> on MetricsRecord instances 
 * with the same set of tag names and tag values.  Different threads should 
 * <b>not</b> use the same MetricsRecord instance at the same time.
 */
public interface MetricsRecord {
    
  /**
   * Returns the record name. 
   *
   * @return the record name
   */
  public abstract String getRecordName();
    
  /**
   * Sets the named tag to the specified value.  The tagValue may be null, 
   * which is treated the same as an empty String.
   *
   * @param tagName name of the tag
   * @param tagValue new value of the tag
   * @throws MetricsException if the tagName conflicts with the configuration
   */
  public abstract void setTag(String tagName, String tagValue);
    
  /**
   * Sets the named tag to the specified value.
   *
   * @param tagName name of the tag
   * @param tagValue new value of the tag
   * @throws MetricsException if the tagName conflicts with the configuration
   */
  public abstract void setTag(String tagName, int tagValue);
    
  /**
   * Sets the named tag to the specified value.
   *
   * @param tagName name of the tag
   * @param tagValue new value of the tag
   * @throws MetricsException if the tagName conflicts with the configuration
   */
  public abstract void setTag(String tagName, long tagValue);
    
  /**
   * Sets the named tag to the specified value.
   *
   * @param tagName name of the tag
   * @param tagValue new value of the tag
   * @throws MetricsException if the tagName conflicts with the configuration
   */
  public abstract void setTag(String tagName, short tagValue);
    
  /**
   * Sets the named tag to the specified value.
   *
   * @param tagName name of the tag
   * @param tagValue new value of the tag
   * @throws MetricsException if the tagName conflicts with the configuration
   */
  public abstract void setTag(String tagName, byte tagValue);
    
  /**
   * Removes any tag of the specified name.
   *
   * @param tagName name of a tag
   */
  public abstract void removeTag(String tagName);
  
  /**
   * Sets the named metric to the specified value.
   *
   * @param metricName name of the metric
   * @param metricValue new value of the metric
   * @throws MetricsException if the metricName or the type of the metricValue 
   * conflicts with the configuration
   */
  public abstract void setMetric(String metricName, int metricValue);
    
  /**
   * Sets the named metric to the specified value.
   *
   * @param metricName name of the metric
   * @param metricValue new value of the metric
   * @throws MetricsException if the metricName or the type of the metricValue 
   * conflicts with the configuration
   */
  public abstract void setMetric(String metricName, long metricValue);
    
  /**
   * Sets the named metric to the specified value.
   *
   * @param metricName name of the metric
   * @param metricValue new value of the metric
   * @throws MetricsException if the metricName or the type of the metricValue 
   * conflicts with the configuration
   */
  public abstract void setMetric(String metricName, short metricValue);
    
  /**
   * Sets the named metric to the specified value.
   *
   * @param metricName name of the metric
   * @param metricValue new value of the metric
   * @throws MetricsException if the metricName or the type of the metricValue 
   * conflicts with the configuration
   */
  public abstract void setMetric(String metricName, byte metricValue);
    
  /**
   * Sets the named metric to the specified value.
   *
   * @param metricName name of the metric
   * @param metricValue new value of the metric
   * @throws MetricsException if the metricName or the type of the metricValue 
   * conflicts with the configuration
   */
  public abstract void setMetric(String metricName, float metricValue);
    
  /**
   * Increments the named metric by the specified value.
   *
   * @param metricName name of the metric
   * @param metricValue incremental value
   * @throws MetricsException if the metricName or the type of the metricValue 
   * conflicts with the configuration
   */
  public abstract void incrMetric(String metricName, int metricValue);
    
  /**
   * Increments the named metric by the specified value.
   *
   * @param metricName name of the metric
   * @param metricValue incremental value
   * @throws MetricsException if the metricName or the type of the metricValue 
   * conflicts with the configuration
   */
  public abstract void incrMetric(String metricName, long metricValue);
    
  /**
   * Increments the named metric by the specified value.
   *
   * @param metricName name of the metric
   * @param metricValue incremental value
   * @throws MetricsException if the metricName or the type of the metricValue 
   * conflicts with the configuration
   */
  public abstract void incrMetric(String metricName, short metricValue);
    
  /**
   * Increments the named metric by the specified value.
   *
   * @param metricName name of the metric
   * @param metricValue incremental value
   * @throws MetricsException if the metricName or the type of the metricValue 
   * conflicts with the configuration
   */
  public abstract void incrMetric(String metricName, byte metricValue);
    
  /**
   * Increments the named metric by the specified value.
   *
   * @param metricName name of the metric
   * @param metricValue incremental value
   * @throws MetricsException if the metricName or the type of the metricValue 
   * conflicts with the configuration
   */
  public abstract void incrMetric(String metricName, float metricValue);
    
  /**
   * Updates the table of buffered data which is to be sent periodically.
   * If the tag values match an existing row, that row is updated; 
   * otherwise, a new row is added.
   */
  public abstract void update();
    
  /**
   * Removes, from the buffered data table, all rows having tags 
   * that equal the tags that have been set on this record. For example,
   * if there are no tags on this record, all rows for this record name
   * would be removed.  Or, if there is a single tag on this record, then
   * just rows containing a tag with the same name and value would be removed.
   */
  public abstract void remove();
    
}
