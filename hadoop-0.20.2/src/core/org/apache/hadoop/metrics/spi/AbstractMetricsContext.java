/*
 * AbstractMetricsContext.java
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsException;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.Updater;

/**
 * The main class of the Service Provider Interface.  This class should be
 * extended in order to integrate the Metrics API with a specific metrics
 * client library. <p/>
 *
 * This class implements the internal table of metric data, and the timer
 * on which data is to be sent to the metrics system.  Subclasses must
 * override the abstract <code>emitRecord</code> method in order to transmit
 * the data. <p/>
 */
public abstract class AbstractMetricsContext implements MetricsContext {
    
  private int period = MetricsContext.DEFAULT_PERIOD;
  private Timer timer = null;
    
  private Set<Updater> updaters = new HashSet<Updater>(1);
  private volatile boolean isMonitoring = false;
    
  private ContextFactory factory = null;
  private String contextName = null;
    
  public static class TagMap extends TreeMap<String,Object> {
    private static final long serialVersionUID = 3546309335061952993L;
    TagMap() {
      super();
    }
    TagMap(TagMap orig) {
      super(orig);
    }
    /**
     * Returns true if this tagmap contains every tag in other.
     */
    public boolean containsAll(TagMap other) {
      for (Map.Entry<String,Object> entry : other.entrySet()) {
        Object value = get(entry.getKey());
        if (value == null || !value.equals(entry.getValue())) {
          // either key does not exist here, or the value is different
          return false;
        }
      }
      return true;
    }
  }
  
  public static class MetricMap extends TreeMap<String,Number> {
    private static final long serialVersionUID = -7495051861141631609L;
    MetricMap() {
      super();
    }
    MetricMap(MetricMap orig) {
      super(orig);
    }
  }
            
  static class RecordMap extends HashMap<TagMap,MetricMap> {
    private static final long serialVersionUID = 259835619700264611L;
  }
    
  private Map<String,RecordMap> bufferedData = new HashMap<String,RecordMap>();
    

  /**
   * Creates a new instance of AbstractMetricsContext
   */
  protected AbstractMetricsContext() {
  }
    
  /**
   * Initializes the context.
   */
  public void init(String contextName, ContextFactory factory) 
  {
    this.contextName = contextName;
    this.factory = factory;
  }
    
  /**
   * Convenience method for subclasses to access factory attributes.
   */
  protected String getAttribute(String attributeName) {
    String factoryAttribute = contextName + "." + attributeName;
    return (String) factory.getAttribute(factoryAttribute);  
  }
    
  /**
   * Returns an attribute-value map derived from the factory attributes
   * by finding all factory attributes that begin with 
   * <i>contextName</i>.<i>tableName</i>.  The returned map consists of
   * those attributes with the contextName and tableName stripped off.
   */
  protected Map<String,String> getAttributeTable(String tableName) {
    String prefix = contextName + "." + tableName + ".";
    Map<String,String> result = new HashMap<String,String>();
    for (String attributeName : factory.getAttributeNames()) {
      if (attributeName.startsWith(prefix)) {
        String name = attributeName.substring(prefix.length());
        String value = (String) factory.getAttribute(attributeName);
        result.put(name, value);
      }
    }
    return result;
  }
    
  /**
   * Returns the context name.
   */
  public String getContextName() {
    return contextName;
  }
    
  /**
   * Returns the factory by which this context was created.
   */
  public ContextFactory getContextFactory() {
    return factory;
  }
    
  /**
   * Starts or restarts monitoring, the emitting of metrics records.
   */
  public synchronized void startMonitoring()
    throws IOException {
    if (!isMonitoring) {
      startTimer();
      isMonitoring = true;
    }
  }
    
  /**
   * Stops monitoring.  This does not free buffered data. 
   * @see #close()
   */
  public synchronized void stopMonitoring() {
    if (isMonitoring) {
      stopTimer();
      isMonitoring = false;
    }
  }
    
  /**
   * Returns true if monitoring is currently in progress.
   */
  public boolean isMonitoring() {
    return isMonitoring;
  }
    
  /**
   * Stops monitoring and frees buffered data, returning this
   * object to its initial state.  
   */
  public synchronized void close() {
    stopMonitoring();
    clearUpdaters();
  } 
    
  /**
   * Creates a new AbstractMetricsRecord instance with the given <code>recordName</code>.
   * Throws an exception if the metrics implementation is configured with a fixed
   * set of record names and <code>recordName</code> is not in that set.
   * 
   * @param recordName the name of the record
   * @throws MetricsException if recordName conflicts with configuration data
   */
  public final synchronized MetricsRecord createRecord(String recordName) {
    if (bufferedData.get(recordName) == null) {
      bufferedData.put(recordName, new RecordMap());
    }
    return newRecord(recordName);
  }
    
  /**
   * Subclasses should override this if they subclass MetricsRecordImpl.
   * @param recordName the name of the record
   * @return newly created instance of MetricsRecordImpl or subclass
   */
  protected MetricsRecord newRecord(String recordName) {
    return new MetricsRecordImpl(recordName, this);
  }
    
  /**
   * Registers a callback to be called at time intervals determined by
   * the configuration.
   *
   * @param updater object to be run periodically; it should update
   * some metrics records 
   */
  public synchronized void registerUpdater(final Updater updater) {
    if (!updaters.contains(updater)) {
      updaters.add(updater);
    }
  }
    
  /**
   * Removes a callback, if it exists.
   *
   * @param updater object to be removed from the callback list
   */
  public synchronized void unregisterUpdater(Updater updater) {
    updaters.remove(updater);
  }
    
  private synchronized void clearUpdaters() {
    updaters.clear();
  }
    
  /**
   * Starts timer if it is not already started
   */
  private synchronized void startTimer() {
    if (timer == null) {
      timer = new Timer("Timer thread for monitoring " + getContextName(), 
                        true);
      TimerTask task = new TimerTask() {
          public void run() {
            try {
              timerEvent();
            }
            catch (IOException ioe) {
              ioe.printStackTrace();
            }
          }
        };
      long millis = period * 1000;
      timer.scheduleAtFixedRate(task, millis, millis);
    }
  }
    
  /**
   * Stops timer if it is running
   */
  private synchronized void stopTimer() {
    if (timer != null) {
      timer.cancel();
      timer = null;
    }
  }
    
  /**
   * Timer callback.
   */
  private void timerEvent() throws IOException {
    if (isMonitoring) {
      Collection<Updater> myUpdaters;
      synchronized (this) {
        myUpdaters = new ArrayList<Updater>(updaters);
      }     
      // Run all the registered updates without holding a lock
      // on this context
      for (Updater updater : myUpdaters) {
        try {
          updater.doUpdates(this);
        }
        catch (Throwable throwable) {
          throwable.printStackTrace();
        }
      }
      emitRecords();
    }
  }
    
  /**
   *  Emits the records.
   */
  private synchronized void emitRecords() throws IOException {
    for (String recordName : bufferedData.keySet()) {
      RecordMap recordMap = bufferedData.get(recordName);
      synchronized (recordMap) {
        Set<Entry<TagMap, MetricMap>> entrySet = recordMap.entrySet ();
        for (Entry<TagMap, MetricMap> entry : entrySet) {
          OutputRecord outRec = new OutputRecord(entry.getKey(), entry.getValue());
          emitRecord(contextName, recordName, outRec);
        }
      }
    }
    flush();
  }
  
  /**
   * Retrieves all the records managed by this MetricsContext.
   * Useful for monitoring systems that are polling-based.
   * @return A non-null collection of all monitoring records.
   */
  public synchronized Map<String, Collection<OutputRecord>> getAllRecords() {
    Map<String, Collection<OutputRecord>> out = new TreeMap<String, Collection<OutputRecord>>();
    for (String recordName : bufferedData.keySet()) {
      RecordMap recordMap = bufferedData.get(recordName);
      synchronized (recordMap) {
        List<OutputRecord> records = new ArrayList<OutputRecord>();
        Set<Entry<TagMap, MetricMap>> entrySet = recordMap.entrySet();
        for (Entry<TagMap, MetricMap> entry : entrySet) {
          OutputRecord outRec = new OutputRecord(entry.getKey(), entry.getValue());
          records.add(outRec);
        }
        out.put(recordName, records);
      }
    }
    return out;
  }

  /**
   * Sends a record to the metrics system.
   */
  protected abstract void emitRecord(String contextName, String recordName, 
                                     OutputRecord outRec) throws IOException;
    
  /**
   * Called each period after all records have been emitted, this method does nothing.
   * Subclasses may override it in order to perform some kind of flush.
   */
  protected void flush() throws IOException {
  }
    
  /**
   * Called by MetricsRecordImpl.update().  Creates or updates a row in
   * the internal table of metric data.
   */
  protected void update(MetricsRecordImpl record) {
    String recordName = record.getRecordName();
    TagMap tagTable = record.getTagTable();
    Map<String,MetricValue> metricUpdates = record.getMetricTable();
        
    RecordMap recordMap = getRecordMap(recordName);
    synchronized (recordMap) {
      MetricMap metricMap = recordMap.get(tagTable);
      if (metricMap == null) {
        metricMap = new MetricMap();
        TagMap tagMap = new TagMap(tagTable); // clone tags
        recordMap.put(tagMap, metricMap);
      }

      Set<Entry<String, MetricValue>> entrySet = metricUpdates.entrySet();
      for (Entry<String, MetricValue> entry : entrySet) {
        String metricName = entry.getKey ();
        MetricValue updateValue = entry.getValue ();
        Number updateNumber = updateValue.getNumber();
        Number currentNumber = metricMap.get(metricName);
        if (currentNumber == null || updateValue.isAbsolute()) {
          metricMap.put(metricName, updateNumber);
        }
        else {
          Number newNumber = sum(updateNumber, currentNumber);
          metricMap.put(metricName, newNumber);
        }
      }
    }
  }
    
  private synchronized RecordMap getRecordMap(String recordName) {
    return bufferedData.get(recordName);
  }
    
  /**
   * Adds two numbers, coercing the second to the type of the first.
   *
   */
  private Number sum(Number a, Number b) {
    if (a instanceof Integer) {
      return Integer.valueOf(a.intValue() + b.intValue());
    }
    else if (a instanceof Float) {
      return new Float(a.floatValue() + b.floatValue());
    }
    else if (a instanceof Short) {
      return Short.valueOf((short)(a.shortValue() + b.shortValue()));
    }
    else if (a instanceof Byte) {
      return Byte.valueOf((byte)(a.byteValue() + b.byteValue()));
    }
    else if (a instanceof Long) {
      return Long.valueOf((a.longValue() + b.longValue()));
    }
    else {
      // should never happen
      throw new MetricsException("Invalid number type");
    }
            
  }
    
  /**
   * Called by MetricsRecordImpl.remove().  Removes all matching rows in
   * the internal table of metric data.  A row matches if it has the same
   * tag names and values as record, but it may also have additional
   * tags.
   */    
  protected void remove(MetricsRecordImpl record) {
    String recordName = record.getRecordName();
    TagMap tagTable = record.getTagTable();
        
    RecordMap recordMap = getRecordMap(recordName);
    synchronized (recordMap) {
      Iterator<TagMap> it = recordMap.keySet().iterator();
      while (it.hasNext()) {
        TagMap rowTags = it.next();
        if (rowTags.containsAll(tagTable)) {
          it.remove();
        }
      }
    }
  }
    
  /**
   * Returns the timer period.
   */
  public int getPeriod() {
    return period;
  }
    
  /**
   * Sets the timer period
   */
  protected void setPeriod(int period) {
    this.period = period;
  }
  
  /**
   * If a period is set in the attribute passed in, override
   * the default with it.
   */
  protected void parseAndSetPeriod(String attributeName) {
    String periodStr = getAttribute(attributeName);
    if (periodStr != null) {
      int period = 0;
      try {
        period = Integer.parseInt(periodStr);
      } catch (NumberFormatException nfe) {
      }
      if (period <= 0) {
        throw new MetricsException("Invalid period: " + periodStr);
      }
      setPeriod(period);
    }
  }
}
