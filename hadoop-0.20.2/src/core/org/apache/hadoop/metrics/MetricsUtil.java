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
package org.apache.hadoop.metrics;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Utility class to simplify creation and reporting of hadoop metrics.
 *
 * For examples of usage, see NameNodeMetrics.
 * @see org.apache.hadoop.metrics.MetricsRecord
 * @see org.apache.hadoop.metrics.MetricsContext
 * @see org.apache.hadoop.metrics.ContextFactory
 */
public class MetricsUtil {
    
  public static final Log LOG =
    LogFactory.getLog(MetricsUtil.class);

  /**
   * Don't allow creation of a new instance of Metrics
   */
  private MetricsUtil() {}
    
  public static MetricsContext getContext(String contextName) {
    return getContext(contextName, contextName);
  }

  /**
   * Utility method to return the named context.
   * If the desired context cannot be created for any reason, the exception
   * is logged, and a null context is returned.
   */
  public static MetricsContext getContext(String refName, String contextName) {
    MetricsContext metricsContext;
    try {
      metricsContext =
        ContextFactory.getFactory().getContext(refName, contextName);
      if (!metricsContext.isMonitoring()) {
        metricsContext.startMonitoring();
      }
    } catch (Exception ex) {
      LOG.error("Unable to create metrics context " + contextName, ex);
      metricsContext = ContextFactory.getNullContext(contextName);
    }
    return metricsContext;
  }

  /**
   * Utility method to create and return new metrics record instance within the
   * given context. This record is tagged with the host name.
   *
   * @param context the context
   * @param recordName name of the record
   * @return newly created metrics record
   */
  public static MetricsRecord createRecord(MetricsContext context, 
                                           String recordName) 
  {
    MetricsRecord metricsRecord = context.createRecord(recordName);
    metricsRecord.setTag("hostName", getHostName());
    return metricsRecord;        
  }
    
  /**
   * Returns the host name.  If the host name is unobtainable, logs the
   * exception and returns "unknown".
   */
  private static String getHostName() {
    String hostName = null;
    try {
      hostName = InetAddress.getLocalHost().getHostName();
    } 
    catch (UnknownHostException ex) {
      LOG.info("Unable to obtain hostName", ex);
      hostName = "unknown";
    }
    return hostName;
  }

}
