/*
 * MetricsContext.java
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

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.metrics.spi.OutputRecord;

/**
 * The main interface to the metrics package. 
 */
public interface MetricsContext {
    
  /**
   * Default period in seconds at which data is sent to the metrics system.
   */
  public static final int DEFAULT_PERIOD = 5;

  /**
   * Initialize this context.
   * @param contextName The given name for this context
   * @param factory The creator of this context
   */
  public void init(String contextName, ContextFactory factory);

  /**
   * Returns the context name.
   *
   * @return the context name
   */
  public abstract String getContextName();
    
  /**
   * Starts or restarts monitoring, the emitting of metrics records as they are 
   * updated. 
   */
  public abstract void startMonitoring()
    throws IOException;

  /**
   * Stops monitoring.  This does not free any data that the implementation
   * may have buffered for sending at the next timer event. It
   * is OK to call <code>startMonitoring()</code> again after calling 
   * this.
   * @see #close()
   */
  public abstract void stopMonitoring();
    
  /**
   * Returns true if monitoring is currently in progress.
   */
  public abstract boolean isMonitoring();
    
  /**
   * Stops monitoring and also frees any buffered data, returning this 
   * object to its initial state.  
   */
  public abstract void close();
    
  /**
   * Creates a new MetricsRecord instance with the given <code>recordName</code>.
   * Throws an exception if the metrics implementation is configured with a fixed
   * set of record names and <code>recordName</code> is not in that set.
   *
   * @param recordName the name of the record
   * @throws MetricsException if recordName conflicts with configuration data
   */
  public abstract MetricsRecord createRecord(String recordName);
    
  /**
   * Registers a callback to be called at regular time intervals, as 
   * determined by the implementation-class specific configuration.
   *
   * @param updater object to be run periodically; it should updated
   * some metrics records and then return
   */
  public abstract void registerUpdater(Updater updater);

  /**
   * Removes a callback, if it exists.
   * 
   * @param updater object to be removed from the callback list
   */
  public abstract void unregisterUpdater(Updater updater);
  
  /**
   * Returns the timer period.
   */
  public abstract int getPeriod();
  
  /**
   * Retrieves all the records managed by this MetricsContext.
   * Useful for monitoring systems that are polling-based.
   * 
   * @return A non-null map from all record names to the records managed.
   */
   Map<String, Collection<OutputRecord>> getAllRecords();
}
