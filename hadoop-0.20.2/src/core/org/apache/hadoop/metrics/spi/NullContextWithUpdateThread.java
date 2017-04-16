/*
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

import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.MetricsException;

/**
 * A null context which has a thread calling 
 * periodically when monitoring is started. This keeps the data sampled 
 * correctly.
 * In all other respects, this is like the NULL context: No data is emitted.
 * This is suitable for Monitoring systems like JMX which reads the metrics
 *  when someone reads the data from JMX.
 * 
 * The default impl of start and stop monitoring:
 *  is the AbstractMetricsContext is good enough.
 * 
 */

public class NullContextWithUpdateThread extends AbstractMetricsContext {
  
  private static final String PERIOD_PROPERTY = "period";
    
  /** Creates a new instance of NullContextWithUpdateThread */
  public NullContextWithUpdateThread() {
  }
  
  public void init(String contextName, ContextFactory factory) {
    super.init(contextName, factory);
    parseAndSetPeriod(PERIOD_PROPERTY);
  }
   
    
  /**
   * Do-nothing version of emitRecord
   */
  protected void emitRecord(String contextName, String recordName,
                            OutputRecord outRec) 
  {}
    
  /**
   * Do-nothing version of update
   */
  protected void update(MetricsRecordImpl record) {
  }
    
  /**
   * Do-nothing version of remove
   */
  protected void remove(MetricsRecordImpl record) {
  }
}
