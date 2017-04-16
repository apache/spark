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
package org.apache.hadoop.ipc.metrics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

/**
 * 
 * This class is for maintaining  the various RPC method related statistics
 * and publishing them through the metrics interfaces.
 * This also registers the JMX MBean for RPC.
 */
public class RpcDetailedMetrics implements Updater {
  public final MetricsRegistry registry = new MetricsRegistry();
  private final MetricsRecord metricsRecord;
  private static final Log LOG = LogFactory.getLog(RpcDetailedMetrics.class);
  RpcDetailedActivityMBean rpcMBean;
  
  /**
   * Statically added metrics to expose at least one metrics, without
   * which other dynamically added metrics are not exposed over JMX.
   */
  final MetricsTimeVaryingRate getProtocolVersion = 
    new MetricsTimeVaryingRate("getProtocolVersion", registry);
  
  public RpcDetailedMetrics(final String hostName, final String port) {
    MetricsContext context = MetricsUtil.getContext("rpc");
    metricsRecord = MetricsUtil.createRecord(context, "detailed-metrics");

    metricsRecord.setTag("port", port);

    LOG.info("Initializing RPC Metrics with hostName=" 
        + hostName + ", port=" + port);

    context.registerUpdater(this);
    
    // Need to clean up the interface to RpcMgt - don't need both metrics and server params
    rpcMBean = new RpcDetailedActivityMBean(registry, hostName, port);
  }
  
  
  /**
   * Push the metrics to the monitoring subsystem on doUpdate() call.
   */
  public void doUpdates(final MetricsContext context) {
    
    synchronized (this) {
      for (MetricsBase m : registry.getMetricsList()) {
        m.pushMetric(metricsRecord);
      }
    }
    metricsRecord.update();
  }

  public void shutdown() {
    if (rpcMBean != null) 
      rpcMBean.shutdown();
  }
}
