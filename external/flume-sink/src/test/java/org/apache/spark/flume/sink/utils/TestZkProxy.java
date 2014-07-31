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
package org.apache.spark.flume.sink.utils;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.apache.curator.test.TestingServer;

import java.io.File;
import java.io.Serializable;
import java.util.List;

public class TestZkProxy implements Serializable {
    private TestingServer server;
    private int port = 2283;

    @Before
    public void setUp() throws Exception {
        File file = Files.createTempDir();
        System.err.println("tempPath:" + file.getAbsolutePath());
        server = new TestingServer(port, file);
    }

    private ZkProxy getZkProxy() {
        return ZkProxy.get(new ZkProxy.Conf(server.getConnectString(), 1, 1000));
    }

    @After
    public void tearDown() throws Exception {
        server.stop();
    }

    @Test
    public void testState() {
        ZkProxy proxy = getZkProxy();
        Assert.assertEquals(false, proxy.isStarted());
        proxy.start();
        Assert.assertEquals(true, proxy.isStarted());
        proxy.stop();
        Assert.assertEquals(false, proxy.isStarted());
    }

    @Test
    public void testCreateNormal() throws Exception {
        ZkProxy proxy = getZkProxy();
        proxy.start();
        String path = "/proxy";
        proxy.create(path, null, ZkProxy.ZkNodeMode.EPHEMERAL, true);
        Assert.assertTrue(proxy.checkExists(path));
        proxy.stop();
    }

    @Test
    public void testCreateWithValue() throws Exception {
        ZkProxy proxy = getZkProxy();
        proxy.start();
        String path = "/path";
        String value = "my value";
        proxy.create(path, value.getBytes(), ZkProxy.ZkNodeMode.PERSISTENT, true);
        String realValue = new String(proxy.get(path));
        Assert.assertEquals(value, realValue);
        proxy.stop();
    }

    @Test
    public void testCreateCascade() throws Exception {
        ZkProxy proxy = getZkProxy();
        proxy.start();
        String path = "/a/b/c";
        String value = "my value";
        boolean occureException = false;
        try {
            proxy.create(path, value.getBytes(), ZkProxy.ZkNodeMode.PERSISTENT, false);
        } catch (Exception e) {
            occureException = true;
        }
        Assert.assertTrue(occureException);

        proxy.create(path, value.getBytes(), ZkProxy.ZkNodeMode.PERSISTENT, true);
        Assert.assertTrue(proxy.checkExists(path));
        proxy.stop();
    }

    @Test
    public void testSet() throws Exception {
        ZkProxy proxy = getZkProxy();
        proxy.start();
        String path = "/p";
        String value = "value1";
        proxy.create(path, value.getBytes(), ZkProxy.ZkNodeMode.PERSISTENT, true);
        Assert.assertEquals(value, new String(proxy.get(path)));
        value = "value2";
        proxy.set(path, value.getBytes());
        Assert.assertEquals(value, new String(proxy.get(path)));
        proxy.stop();
    }

    @Test
    public void testDelete() throws Exception {
        ZkProxy proxy = getZkProxy();
        proxy.start();
        String path = "/proxy";
        proxy.create(path, null, ZkProxy.ZkNodeMode.EPHEMERAL, true);
        proxy.delete(path);
        Assert.assertTrue(!proxy.checkExists(path));
        proxy.stop();
    }

    @Test
    public void testGetChildren() throws Exception {
        ZkProxy proxy = getZkProxy();
        proxy.start();
        proxy.create("/root/child1", null, ZkProxy.ZkNodeMode.PERSISTENT, true);
        proxy.create("/root/child2", null, ZkProxy.ZkNodeMode.PERSISTENT, false);
        List<String> children = proxy.getChildren("/root");
        Assert.assertTrue(children.size() == 2);
        Assert.assertTrue(children.contains("child1"));
        Assert.assertTrue(children.contains("child2"));
        proxy.stop();
    }
}