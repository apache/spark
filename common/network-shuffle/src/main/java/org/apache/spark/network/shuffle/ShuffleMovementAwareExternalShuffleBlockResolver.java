/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.network.shuffle;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentSkipListSet;
/**
 * This resolver extends ExternalShuffleBlockResolver to monitor the status of shuffle data transfer

 * of executors on the shuffle host.
 */
public class ShuffleMovementAwareExternalShuffleBlockResolver
        extends ExternalShuffleBlockResolver
        implements ShuffleMovementAwareExternalShuffleBlockResolverMBean {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ShuffleMovementAwareExternalShuffleBlockResolver.class);
    private final ConcurrentSkipListSet<String> executorShuffleSet =

            new ConcurrentSkipListSet<>();
    public ShuffleMovementAwareExternalShuffleBlockResolver(
            TransportConf conf,
            File registeredExecutorFile)
            throws IOException {
        super(conf, registeredExecutorFile);
        initializeMetrics();
        LOGGER.info("Using ShuffleMovementAwareExternalShuffleBlockResolver");

    }
    @Override
    public void registerExecutor(
            String appId,
            String execId,
            ExecutorShuffleInfo executorInfo) {
        super.registerExecutor(appId, execId, executorInfo);
        AppExecId fullId = new AppExecId(appId, execId);
        executorShuffleSet.add(fullId.toString());
    }
    @Override
    public void setExecutorDecommissioned(String appId, String execId) {
        AppExecId fullId = new AppExecId(appId, execId);
        LOGGER.info("Marking shuffle as decommissioned for {}, {}", appId, execId);
        executorShuffleSet.remove(fullId.toString());
        LOGGER.info("Executors with shuffle data {}", executorShuffleSet.toString());
    }
    @Override
    public void applicationRemoved(String appId, boolean cleanupLocalDirs) {
        super.applicationRemoved(appId, cleanupLocalDirs);
        executorShuffleSet.removeIf(fullIdStr -> fullIdStr.contains(appId));
    }

    @Override
    public boolean isShuffleFileTransferComplete() {
        return executorShuffleSet.isEmpty();
    }

    @Override
    public boolean getCompletionState() {
        return isShuffleFileTransferComplete();
    }

    private void initializeMetrics() {
        try {
            StandardMBean bean =
                    new StandardMBean(this, ShuffleMovementAwareExternalShuffleBlockResolverMBean.class);
            MBeans
                    .register("YarnShuffleService", "ExternalFsFileMonitorMetrics", bean);
        } catch (NotCompliantMBeanException e) {
            LOGGER.error("Error registering Shuffle Metrics MBean", e);
        }
    }
}