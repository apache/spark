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
package org.apache.hadoop.hdfs.server.namenode.metrics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.metrics.*;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsIntValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingInt;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

/**
 * 
 * This class is for maintaining  the various NameNode activity statistics
 * and publishing them through the metrics interfaces.
 * This also registers the JMX MBean for RPC.
 * <p>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values;
 *  for example:
 *  <p> {@link #syncs}.inc()
 *
 */
public class NameNodeMetrics implements Updater {
    private static Log log = LogFactory.getLog(NameNodeMetrics.class);
    private final MetricsRecord metricsRecord;
    public MetricsRegistry registry = new MetricsRegistry();
    
    private NameNodeActivtyMBean namenodeActivityMBean;
    
    public MetricsTimeVaryingInt numFilesCreated =
                          new MetricsTimeVaryingInt("FilesCreated", registry);
    public MetricsTimeVaryingInt numFilesAppended =
                          new MetricsTimeVaryingInt("FilesAppended", registry);
    public MetricsTimeVaryingInt numGetBlockLocations = 
                    new MetricsTimeVaryingInt("GetBlockLocations", registry);
    public MetricsTimeVaryingInt numFilesRenamed =
                    new MetricsTimeVaryingInt("FilesRenamed", registry);
    public MetricsTimeVaryingInt numGetListingOps = 
                    new MetricsTimeVaryingInt("GetListingOps", registry);
    public MetricsTimeVaryingInt numCreateFileOps = 
                    new MetricsTimeVaryingInt("CreateFileOps", registry);
    public MetricsTimeVaryingInt numDeleteFileOps = 
                          new MetricsTimeVaryingInt("DeleteFileOps", registry);
    public MetricsTimeVaryingInt numFileInfoOps =
                          new MetricsTimeVaryingInt("FileInfoOps", registry);
    public MetricsTimeVaryingInt numAddBlockOps = 
                          new MetricsTimeVaryingInt("AddBlockOps", registry);

    public MetricsTimeVaryingRate transactions =
                    new MetricsTimeVaryingRate("Transactions", registry, "Journal Transaction");
    public MetricsTimeVaryingRate syncs =
                    new MetricsTimeVaryingRate("Syncs", registry, "Journal Sync");
    public MetricsTimeVaryingInt transactionsBatchedInSync = 
                    new MetricsTimeVaryingInt("JournalTransactionsBatchedInSync", registry, "Journal Transactions Batched In Sync");
    public MetricsTimeVaryingRate blockReport =
                    new MetricsTimeVaryingRate("blockReport", registry, "Block Report");
    public MetricsIntValue safeModeTime =
                    new MetricsIntValue("SafemodeTime", registry, "Duration in SafeMode at Startup");
    public MetricsIntValue fsImageLoadTime = 
                    new MetricsIntValue("fsImageLoadTime", registry, "Time loading FS Image at Startup");
    public MetricsIntValue numBlocksCorrupted =
                    new MetricsIntValue("BlocksCorrupted", registry);
    public MetricsTimeVaryingInt numFilesInGetListingOps = 
                    new MetricsTimeVaryingInt("FilesInGetListingOps", registry);

      
    public NameNodeMetrics(Configuration conf, NameNode nameNode) {
      String sessionId = conf.get("session.id");
      // Initiate Java VM metrics
      JvmMetrics.init("NameNode", sessionId);

      
      // Now the Mbean for the name node - this alos registers the MBean
      namenodeActivityMBean = new NameNodeActivtyMBean(registry);
      
      // Create a record for NameNode metrics
      MetricsContext metricsContext = MetricsUtil.getContext("dfs");
      metricsRecord = MetricsUtil.createRecord(metricsContext, "namenode");
      metricsRecord.setTag("sessionId", sessionId);
      metricsContext.registerUpdater(this);
      log.info("Initializing NameNodeMeterics using context object:" +
                metricsContext.getClass().getName());
    }
    

    
    public void shutdown() {
      if (namenodeActivityMBean != null) 
        namenodeActivityMBean.shutdown();
    }
      
    /**
     * Since this object is a registered updater, this method will be called
     * periodically, e.g. every 5 seconds.
     */
    public void doUpdates(MetricsContext unused) {
      synchronized (this) {
        for (MetricsBase m : registry.getMetricsList()) {
          m.pushMetric(metricsRecord);
        }
      }
      metricsRecord.update();
    }

    public void resetAllMinMax() {
      transactions.resetMinMax();
      syncs.resetMinMax();
      blockReport.resetMinMax();
    }
}
