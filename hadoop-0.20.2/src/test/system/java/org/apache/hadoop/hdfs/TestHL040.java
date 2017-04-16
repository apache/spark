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

package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.test.system.DNClient;
import org.apache.hadoop.hdfs.test.system.HDFSCluster;
import org.apache.hadoop.hdfs.test.system.NNClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestHL040 {
  private HDFSCluster cluster = null;
  private static final Log LOG = LogFactory.getLog(TestHL040.class);

  public TestHL040() throws Exception {
  }

  @Before
  public void setupUp() throws Exception {
    cluster = HDFSCluster.createCluster(new Configuration());
    cluster.setUp();
  }

  @After
  public void tearDown() throws Exception {
    cluster.tearDown();
  }

  @Test
  public void testConnect() throws IOException {
    LOG.info("Staring TestHL040: connecting to the HDFSCluster ");
    LOG.info("================ Getting namenode info ================");
    NNClient dfsMaster = cluster.getNNClient();
    LOG.info("Process info of namenode " + dfsMaster.getHostName() + " is: " +
        dfsMaster.getProcessInfo());
    LOG.info("================ Getting datanode info ================");
    Collection<DNClient> clients = cluster.getDNClients();
    for (DNClient dnC : clients) {
      LOG.info("Process info of datanode " + dnC.getHostName() + " is: " +
          dnC.getProcessInfo());
      Assert.assertNotNull("Datanode process info isn't suppose to be null",
          dnC.getProcessInfo());
    }
  }
}
