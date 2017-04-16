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


import javax.management.ObjectName;

import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics.util.MBeanUtil;


/**
 * This class implements the RpcMgt MBean
 *
 */
class RpcMgt implements RpcMgtMBean {
  private RpcMetrics myMetrics;
  private Server myServer;
  private ObjectName mbeanName;
  
  RpcMgt(final String serviceName, final String port,
                final RpcMetrics metrics, Server server) {
    myMetrics = metrics;
    myServer = server;
    mbeanName = MBeanUtil.registerMBean(serviceName,
                    "RpcStatisticsForPort" + port, this);
  }

  public void shutdown() {
    if (mbeanName != null)
      MBeanUtil.unregisterMBean(mbeanName);
  }
  
  /**
   * @inheritDoc
   */
  public long getRpcOpsAvgProcessingTime() {
    return myMetrics.rpcProcessingTime.getPreviousIntervalAverageTime();
  }
  
  /**
   * @inheritDoc
   */
  public long getRpcOpsAvgProcessingTimeMax() {
    return myMetrics.rpcProcessingTime.getMaxTime();
  }

  /**
   * @inheritDoc
   */
  public long getRpcOpsAvgProcessingTimeMin() {
    return myMetrics.rpcProcessingTime.getMinTime();
  }

  /**
   * @inheritDoc
   */
  public long getRpcOpsAvgQueueTime() {
    return myMetrics.rpcQueueTime.getPreviousIntervalAverageTime();
  }
  
  /**
   * @inheritDoc
   */
  public long getRpcOpsAvgQueueTimeMax() {
    return myMetrics.rpcQueueTime.getMaxTime();
  }

  /**
   * @inheritDoc
   */
  public long getRpcOpsAvgQueueTimeMin() {
    return myMetrics.rpcQueueTime.getMinTime();
  }

  /**
   * @inheritDoc
   */
  public int getRpcOpsNumber() {
    return myMetrics.rpcProcessingTime.getPreviousIntervalNumOps() ;
  }

  /**
   * @inheritDoc
   */
  public int getNumOpenConnections() {
    return myServer.getNumOpenConnections();
  }
  
  /**
   * @inheritDoc
   */
  public int getCallQueueLen() {
    return myServer.getCallQueueLen();
  }

  /**
   * @inheritDoc
   */
  public void resetAllMinMax() {
    myMetrics.rpcProcessingTime.resetMinMax();
    myMetrics.rpcQueueTime.resetMinMax();
  }
}
