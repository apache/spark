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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.curator.framework.recipes.cache.*;

public class ZkProxy {
    private static final Logger logger = LoggerFactory.getLogger(ZkProxy.class);
    private static final byte[] BYTE_NULL = new byte[0];
    // use Conf as key ?
    private static final ConcurrentHashMap<String, ZkProxy> zkProxies = new ConcurrentHashMap<String, ZkProxy>();
    private final Conf conf;
    private final ConcurrentHashMap<String, PathChildrenEventDispatch> listeners = new ConcurrentHashMap<String, PathChildrenEventDispatch>();
    private final AtomicInteger referenceNumber = new AtomicInteger(0);
    private CuratorFramework curatorClient;
    private AtomicBoolean started = new AtomicBoolean(false);
//    private static LoadingCache<Conf, ZkProxy> zkProxys = CacheBuilder.newBuilder()
//            .weakValues().removalListener(new RemovalListener<Conf, ZkProxy>() {
//                @Override
//                public void onRemoval(RemovalNotification<Conf, ZkProxy> objectObjectRemovalNotification) {
//                    objectObjectRemovalNotification.getValue().stop();
//                }
//            }).build(new CacheLoader<Conf, ZkProxy>() {
//                @Override
//                public ZkProxy load(Conf zkConf) throws Exception {
//                    return new ZkProxy(zkConf);
//                }
//            });

    //initialize assignments cache
    public static class Conf {
        public Conf(String zkAddress) {
            this(zkAddress, 3, 1000);
        }

        public Conf(String zkAddress, int retryTimes, int retryIntervalInMs) {
            this.address = zkAddress;
            this.retryTimes = retryTimes;
            this.retryIntervalInMs = retryIntervalInMs;
        }

        private String address;
        private int retryTimes;
        private int retryIntervalInMs;
    }

    public enum ZkNodeMode {
        PERSISTENT,
        EPHEMERAL,
    }

    public interface ChildrenEventListener {
        public void childAddedEvent(String nodePath, byte[] data);
        public void childRemovedEvent(String nodePath, byte[] data);
        public void childUpdatedEvent(String nodePath, byte[] data);
    }

    public class PathChildrenEventDispatch implements PathChildrenCacheListener {
        private final PathChildrenCache pathCache;
        private final List<ChildrenEventListener> listeners = new CopyOnWriteArrayList<ChildrenEventListener>();
        private AtomicBoolean started = new AtomicBoolean(false);

        public PathChildrenEventDispatch(String zkNodePath) {
            pathCache = new PathChildrenCache(curatorClient, zkNodePath, false);
            pathCache.getListenable().addListener(this);
        }

        public void addListener(ChildrenEventListener listener) {
            logger.info("pathCache add listener ");
            this.listeners.add(listener);
        }

        public void start() throws Exception {
            if (started.compareAndSet(false, true)) {
                pathCache.start();
                logger.info("pathCache started");
            }
        }

        public void close() {
            if (started.compareAndSet(true, false)) {
                try {
                    pathCache.close();
                } catch (Exception e) {
                    logger.error(e.getMessage());
                }
            }
        }

        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
            String path = event.getData().getPath();
            byte[] bytes = event.getData().getData();
            logger.debug("receive event " + path + " type:" + event.getType());
            switch (event.getType()) {
                case CHILD_ADDED: {
                    for (ChildrenEventListener listener : listeners) {
                        listener.childAddedEvent(path, bytes);
                    }
                    break;
                }
                case CHILD_UPDATED: {
                    for (ChildrenEventListener listener : listeners) {
                        listener.childUpdatedEvent(path, bytes);
                    }
                    break;
                }
                case CHILD_REMOVED: {
                    for (ChildrenEventListener listener : listeners) {
                        listener.childRemovedEvent(path, bytes);
                    }
                    break;
                }
                default:
                    break;
            }
        }
    }

    private ZkProxy(Conf conf) {
        this.conf = conf;
        logger.info("new zkProxy " + conf.address);
    }

    public static ZkProxy get(Conf conf) {
        if (!zkProxies.containsKey(conf.address)) {
            zkProxies.putIfAbsent(conf.address, new ZkProxy(conf));
        }
        return zkProxies.get(conf.address);
    }

    public synchronized void start() {
        logger.info("ZkProxy starting ...");
        if (referenceNumber.getAndIncrement() == 0) {
            curatorClient = CuratorFrameworkFactory.newClient(conf.address,
                    new RetryNTimes(conf.retryTimes, conf.retryIntervalInMs));
            curatorClient.start();
            started.compareAndSet(false, true);
            logger.info("ZkProxy started success!");
        }
    }

    public boolean isStarted() {
       return started.get();
    }

    public synchronized void stop() {
        logger.info("ZkProxy Stop called...");
        if (referenceNumber.decrementAndGet() == 0) {
            started.compareAndSet(true, false);
            for (Map.Entry<String, PathChildrenEventDispatch> entry : listeners.entrySet()) {
                entry.getValue().close();
            }
            listeners.clear();
            curatorClient.close();
            logger.info("ZkProxy is Stoped!");
        }
    }

    public void create(String zkNodePath, byte[] value, ZkNodeMode mode,
                       boolean creatingParentsIfNeeded) throws Exception {
        Preconditions.checkState(started.get());
        if (curatorClient.checkExists().forPath(zkNodePath) != null) {
            logger.error(zkNodePath + " was already exists!");
            throw new Exception(zkNodePath + " was already exists!");
        }

        // If value = null, Curator will set zkNode a default value: IP addr.
        // But it's not expected, so we set value to byte[0] if value is null
        byte[] bytes = value == null ? BYTE_NULL : value;
        if (creatingParentsIfNeeded) {
            curatorClient.create().creatingParentsIfNeeded().withMode(getZkCreateMode(mode)).forPath(zkNodePath, bytes);
        } else {
            curatorClient.create().withMode(getZkCreateMode(mode)).forPath(zkNodePath, bytes);
        }
    }

    public boolean checkExists(String zkNodePath) throws Exception {
        Preconditions.checkState(started.get());
        return curatorClient.checkExists().forPath(zkNodePath) != null;
    }

    // Delete a node and return its value
    // Notice: The deleted zkNodePath will not be listened any more.
    // if you want listen zkNodePath after deleted, you need call addEventListener() again.
    public byte[] delete(String zkNodePath) throws Exception {
        Preconditions.checkState(started.get());
        byte[] value = BYTE_NULL;
        if (curatorClient.checkExists().forPath(zkNodePath) != null) {
            try {
                value = get(zkNodePath);
            } catch (Exception e) {
                logger.warn("get({}) in delete() Exception {}", zkNodePath, e);
            }
            curatorClient.delete().guaranteed().forPath(zkNodePath);
        } else {
            logger.warn("Failed to remove {}, path does not exist.", zkNodePath);
        }
        return value;
    }

    public byte[] get(String zkNodePath) throws Exception {
        Preconditions.checkState(started.get());
        return curatorClient.getData().forPath(zkNodePath);
    }

    public List<String> getChildren(String zkNodePath) throws Exception {
        Preconditions.checkState(started.get());
        return curatorClient.getChildren().forPath(zkNodePath);
    }

    public void set(String zkNodePath, byte[] value) throws Exception {
        Preconditions.checkState(started.get());
        curatorClient.setData().forPath(zkNodePath, value);
    }

    // Add a listener to a zkNode, which will Keep watching the change of the zkNode.
    // When the zkNodePath is deleted, the listener will be invalid.
    public synchronized void addEventListener(String zkNodePath, final ChildrenEventListener eventListener) {
        try {
            logger.info("addEventListener on path " + zkNodePath);
            PathChildrenEventDispatch dispatch = listeners.get(zkNodePath);
            if (dispatch == null) {
                dispatch = new PathChildrenEventDispatch(zkNodePath);
                dispatch.start();
                listeners.put(zkNodePath, dispatch);
            }
            dispatch.addListener(eventListener);
        } catch (Exception e) {
            logger.error("checkExists {} Exception.{}" + zkNodePath, e);
        }
    }

    private CreateMode getZkCreateMode(ZkNodeMode mode) {
        return mode == ZkNodeMode.EPHEMERAL ? CreateMode.EPHEMERAL : CreateMode.PERSISTENT;
    }
}

