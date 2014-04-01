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
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsIntValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingInt;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingLong;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

/**
 * 
 * This class is for maintaining  the various RPC statistics
 * and publishing them through the metrics interfaces.
 * This also registers the JMX MBean for RPC.
 * <p>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values;
 * for example:
 *  <p> {@link #rpcQueueTime}.inc(time)
 *
 */
public class RpcMetrics implements Updater {
  private final MetricsRegistry registry = new MetricsRegistry();
  private final MetricsRecord metricsRecord;
  private final Server myServer;
  private static final Log LOG = LogFactory.getLog(RpcMetrics.class);
  RpcActivityMBean rpcMBean;
  
  public RpcMetrics(final String hostName, final String port,
      final Server server) {
    myServer = server;
    MetricsContext context = MetricsUtil.getContext("rpc");
    metricsRecord = MetricsUtil.createRecord(context, "metrics");

    metricsRecord.setTag("port", port);

    LOG.info("Initializing RPC Metrics with hostName=" 
        + hostName + ", port=" + port);

    context.registerUpdater(this);
    
    // Need to clean up the interface to RpcMgt - don't need both metrics and server params
    rpcMBean = new RpcActivityMBean(registry, hostName, port);
  }
  
  
  /**
   * The metrics variables are public:
   *  - they can be set directly by calling their set/inc methods
   *  -they can also be read directly - e.g. JMX does this.
   */

  /**
   * metrics - number of bytes received
   */
  public final MetricsTimeVaryingLong receivedBytes = 
         new MetricsTimeVaryingLong("ReceivedBytes", registry);
  /**
   * metrics - number of bytes sent
   */
  public final MetricsTimeVaryingLong sentBytes = 
         new MetricsTimeVaryingLong("SentBytes", registry);
  /**
   * metrics - rpc queue time
   */
  public final MetricsTimeVaryingRate rpcQueueTime =
          new MetricsTimeVaryingRate("RpcQueueTime", registry);
  /**
   * metrics - rpc processing time
   */
  public final MetricsTimeVaryingRate rpcProcessingTime =
          new MetricsTimeVaryingRate("RpcProcessingTime", registry);
  /**
   * metrics - number of open connections
   */
  public final MetricsIntValue numOpenConnections = 
          new MetricsIntValue("NumOpenConnections", registry);
  /**
   * metrics - length of the queue
   */
  public final MetricsIntValue callQueueLen = 
          new MetricsIntValue("callQueueLen", registry);
  /**
   * metrics - number of failed authentications
   */
  public final MetricsTimeVaryingInt authenticationFailures = 
          new MetricsTimeVaryingInt("rpcAuthenticationFailures", registry);
  /**
   * metrics - number of successful authentications
   */
  public final MetricsTimeVaryingInt authenticationSuccesses = 
          new MetricsTimeVaryingInt("rpcAuthenticationSuccesses", registry);
  /**
   * metrics - number of failed authorizations
   */
  public final MetricsTimeVaryingInt authorizationFailures = 
          new MetricsTimeVaryingInt("rpcAuthorizationFailures", registry);
  /**
   * metrics - number of successful authorizations
   */
  public final MetricsTimeVaryingInt authorizationSuccesses = 
         new MetricsTimeVaryingInt("rpcAuthorizationSuccesses", registry);
  
  /**
   * Push the metrics to the monitoring subsystem on doUpdate() call.
   */
  public void doUpdates(final MetricsContext context) {
    
    synchronized (this) {
      // ToFix - fix server to use the following two metrics directly so
      // the metrics do not have be copied here.
      numOpenConnections.set(myServer.getNumOpenConnections());
      callQueueLen.set(myServer.getCallQueueLen());
      for (MetricsBase m : registry.getMetricsList()) {
        m.pushMetric(metricsRecord);
      }
    }
    metricsRecord.update();
  }

  /**
   * shutdown the metrics
   */
  public void shutdown() {
    if (rpcMBean != null) 
      rpcMBean.shutdown();
  }
}
