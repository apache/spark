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

package org.apache.spark.streaming.flume.sink.utils;

import com.google.common.io.Files;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class TestLogicalHostRouter {
    private TestingServer zkServer;
    private final int port = 2284;
    private final String zkPath = "/router";

    @Before
    public void setUp() throws Exception {
        File tempDir = Files.createTempDir();
        zkServer = new TestingServer(port, tempDir);
    }

    @After
    public void tearDown() throws Exception {
        zkServer.stop();
    }

    private LogicalHostRouter getRouter() {
        String zkAddr = zkServer.getConnectString();
        LogicalHostRouter.Conf conf = new LogicalHostRouter.Conf().setZkAddress(zkAddr)
                .setZkPath(zkPath);
        return new LogicalHostRouter(conf);
    }

    private ZkProxy getZkProxy() {
        return ZkProxy.get(new ZkProxy.Conf(zkServer.getConnectString(), 1, 1000));
    }

    @Test
    public void testStartStop() throws IOException {
        LogicalHostRouter router = getRouter();
        router.start();
        router.stop();
    }

    @Test
    public void testGetLogicalHosts() throws Exception {
        ZkProxy zkProxy = getZkProxy();
        try {
            zkProxy.start();
            zkProxy.create(zkPath + "/A/phy1:2012", null, ZkProxy.ZkNodeMode.EPHEMERAL, true);
            zkProxy.create(zkPath + "/B/phy1:2012", null, ZkProxy.ZkNodeMode.EPHEMERAL, true);
        } finally {
            zkProxy.stop();
        }
        LogicalHostRouter router = getRouter();
        try {
            router.start();
            List<String> logicalHosts = router.getLogicalHosts();
            Assert.assertEquals("A", logicalHosts.get(0));
            Assert.assertEquals("B", logicalHosts.get(1));
        } finally {
            router.stop();
        }
    }

    @Test
    public void testGetPhysicalHosts() throws Exception {
        ZkProxy zkProxy = getZkProxy();
        LogicalHostRouter router = getRouter();
        try {
            zkProxy.start();
            zkProxy.create(zkPath + "/A/phy1:2012", null, ZkProxy.ZkNodeMode.EPHEMERAL, true);
            router.start();
            List<LogicalHostRouter.PhysicalHost> hosts = router.getPhysicalHosts("A");
            Assert.assertEquals(1, hosts.size());
            Assert.assertEquals("phy1", hosts.get(0).getIp());
            Assert.assertEquals(2012, hosts.get(0).getPort());
        } finally {
            zkProxy.stop();
            router.stop();
        }
    }

    @Test
    public void testRegisterPhysicalHost() throws IOException {
        LogicalHostRouter router = getRouter();
        router.start();
        try {
            String logicalHost = "spark";
            List<LogicalHostRouter.PhysicalHost> hosts = router.getPhysicalHosts(logicalHost);
            Assert.assertEquals(0, hosts.size());
            router.registerPhysicalHost(logicalHost, new LogicalHostRouter.PhysicalHost("127.0.0.1", 80));
            hosts = router.getPhysicalHosts(logicalHost);
            Assert.assertEquals(1, hosts.size());
            Assert.assertEquals("127.0.0.1", hosts.get(0).getIp());
            Assert.assertEquals(80, hosts.get(0).getPort());
        } finally {
            router.stop();
        }
    }
}
