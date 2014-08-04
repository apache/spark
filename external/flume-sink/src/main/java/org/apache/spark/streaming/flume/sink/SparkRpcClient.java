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

package org.apache.spark.streaming.flume.sink;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.api.*;
import org.apache.spark.streaming.flume.sink.utils.LogicalHostRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/*
 * configuration example:
 * agent.sinks.ls1.hostname = benchmark
 * agent.sinks.ls1.router.path=192.168.59.128:2181/spark  [zookeeper path to logical host]
 * agent.sinks.ls1.port = 0
 * agent.sinks.ls1.router.retry.times=1  [optional]
 * agent.sinks.ls1.router.retry.interval=1000 [optional]
 */
public class SparkRpcClient extends AbstractRpcClient implements RpcClient {
    private static final Logger logger = LoggerFactory.getLogger(SparkRpcClient.class);
    private static final String HOSTNAME_KEY = "hostname";
    private static final String HOST_ROUTER_PATH = "router.path";
    private static final String HOST_ROUTER_RETRY_TIMES = "router.retry.times";
    private static final String HOST_ROUTER_RETRY_INTERVAL = "router.retry.interval";
    private LogicalHostRouter router;
    private Integer maxTries = 1;
    private volatile boolean isActive = false;
    private Properties configurationProperties;
    private final ClientPool clientPool = new ClientPool();

    private class ClientHandler {
        private HostInfo hostInfo = null;
        private Properties props = null;
        private RpcClient client = null;
        private volatile boolean isConnected = false;

        public ClientHandler(HostInfo hostInfo, Properties props) {
            this.hostInfo = hostInfo;
            this.props = props;
        }

        public synchronized void close() {
            if (isConnected) {
                client.close();
                isConnected = false;
                client = null;
                logger.info("closed client");
            }
        }

        public synchronized RpcClient getClient() {
            if (client == null) {
                Properties props = new Properties();
                props.putAll(configurationProperties);
                props.put(RpcClientConfigurationConstants.CONFIG_CLIENT_TYPE,
                        RpcClientFactory.ClientType.DEFAULT.name());
                props.put(RpcClientConfigurationConstants.CONFIG_HOSTS,
                        hostInfo.getReferenceName());
                props.put(
                        RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX
                                + hostInfo.getReferenceName(),
                        hostInfo.getHostName() + ":" + hostInfo.getPortNumber());
                props.put(RpcClientConfigurationConstants.CONFIG_CLIENT_TYPE,
                        RpcClientConfigurationConstants.DEFAULT_CLIENT_TYPE);
                client = RpcClientFactory.getInstance(props);
                isConnected = true;
                logger.debug("create new RpcClient:" + hostInfo.getHostName() + ":" + hostInfo.getPortNumber());
            }
            return client;
        }
    }

    private class ClientPool implements LogicalHostRouter.LogicalHostRouterListener {
        private List<ClientHandler> clientHandlers = new CopyOnWriteArrayList<ClientHandler>();
        private CountDownLatch emptyLatch = new CountDownLatch(1);
        private AtomicInteger currentClientIndex = new AtomicInteger(0);

        @Override
        public void physicalHostAdded(String logicalHost, LogicalHostRouter.PhysicalHost physicalHost) {
            logger.info("receive host added info " + physicalHost.toString());
            HostInfo hostInfo = new HostInfo("h1", physicalHost.getIp(), physicalHost.getPort());
            addHost(hostInfo);
        }

        @Override
        public void physicalHostRemoved(String logicalHost, LogicalHostRouter.PhysicalHost physicalHost) {
            logger.info("receive host removed info " + physicalHost.toString());
            HostInfo hostInfo = new HostInfo("h1", physicalHost.getIp(), physicalHost.getPort());
            removeHost(hostInfo);
        }

        private boolean isSameHost(HostInfo left, HostInfo right) {
            return left.getHostName().equals(right.getHostName()) && left.getPortNumber() == right.getPortNumber();
        }

        public void addHost(HostInfo hostInfo) {
            logger.info("add host " + hostInfo.getHostName() + ":" + hostInfo.getPortNumber());
            for (ClientHandler handler : clientHandlers) {
                if (isSameHost(handler.hostInfo, hostInfo)) {
                    return;
                }
            }
            clientHandlers.add(new ClientHandler(hostInfo, configurationProperties));
            emptyLatch.countDown();
            logger.info("host added");
        }

        public void removeHost(HostInfo hostInfo) {
            for (ClientHandler handler : clientHandlers) {
                if (isSameHost(handler.hostInfo, hostInfo)) {
                    clientHandlers.remove(handler);
                    return;
                }
            }
        }

        public ClientHandler getClientHandler() {
            int index = currentClientIndex.getAndIncrement();
            if (currentClientIndex.get() >= clientHandlers.size()) {
                currentClientIndex.set(0);
            }
            if (index >= clientHandlers.size()) {
                index = 0;
            }
            try {
                emptyLatch.await();
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
            Preconditions.checkElementIndex(index, clientHandlers.size());
            return clientHandlers.get(index);
        }

        public void close() {
            FlumeException closeException = null;
            int nSuccess = clientHandlers.size();
            for (ClientHandler handler : clientHandlers) {
                try {
                    handler.close();
                    ++nSuccess;
                } catch (FlumeException e) {
                    closeException = e;
                }
            }
            clientHandlers.clear();
            if (closeException != null) {
                throw new FlumeException("Close Exception total: " + clientHandlers.size()
                        + " success: " + nSuccess, closeException);
            }
        }
    }

    public SparkRpcClient(Properties props) {
        this.configurationProperties = props;
    }

    private int getInteger(Properties prop, String key, int defaultValue) {
        String value = prop.getProperty(key);
        if (value != null && value.trim().length() > 0) {
            try {
                return Integer.parseInt(value.trim());
            } catch (NumberFormatException e) {
                logger.warn("invalid " + key + " is set, value: " + value);
            }
        }
        return defaultValue;
    }

    //This function has to be synchronized to establish a happens-before
    //relationship for different threads that access this object
    //since shared data structures are created here.
    private synchronized void configureHosts(Properties properties)
            throws FlumeException {
        if (isActive) {
            logger.error("This client was already configured, " +
                    "cannot reconfigure.");
            throw new FlumeException("This client was already configured, " +
                    "cannot reconfigure.");
        }

        String routerPath = properties.getProperty(HOST_ROUTER_PATH);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(routerPath), HOST_ROUTER_PATH + " is empty");
        LogicalHostRouter.Conf conf =  LogicalHostRouter.Conf.fromRouterPath(routerPath);
        String routerRetryTimes = properties.getProperty(HOST_ROUTER_RETRY_TIMES);
        if (routerRetryTimes != null) {
            conf.setRetryTimes(Integer.parseInt(routerRetryTimes));
        }
        String routerRetryInterval = properties.getProperty(HOST_ROUTER_RETRY_INTERVAL);
        if (routerRetryInterval != null) {
            conf.setRetryInterval(Integer.parseInt(routerRetryInterval));
        }
        if (this.router != null) {
            this.router.stop();
        }
        try {
            router = new LogicalHostRouter(conf);
            router.start();
            String logicalHost = properties.getProperty(HOSTNAME_KEY);
            List<String> logicalHosts = new ArrayList<String>();
            logicalHosts.add(logicalHost);
            router.registerListener(clientPool, logicalHosts);
            List<LogicalHostRouter.PhysicalHost> physicalHosts = router.getPhysicalHosts(logicalHost);
            maxTries = Math.max(1, physicalHosts.size());
            for (LogicalHostRouter.PhysicalHost host : physicalHosts) {
                HostInfo hostInfo = new HostInfo("h1", host.getIp(), host.getPort());
                clientPool.addHost(hostInfo);
            }
        } catch (IOException e) {
            logger.error("failed to read hosts ", e);
            throw new FlumeException("This client read hosts failed " + e.getMessage());
        }
        maxTries = getInteger(properties, RpcClientConfigurationConstants.CONFIG_MAX_ATTEMPTS, maxTries);
        batchSize = getInteger(properties, RpcClientConfigurationConstants.CONFIG_BATCH_SIZE,
                RpcClientConfigurationConstants.DEFAULT_BATCH_SIZE);
        if (batchSize < 1) {
            logger.warn("A batch-size less than 1 was specified: " + batchSize
                    + ". Using default instead.");
            batchSize = RpcClientConfigurationConstants.DEFAULT_BATCH_SIZE;
        }
        isActive = true;
    }

    /**
     * Tries to append an event to the currently connected client. If it cannot
     * send the event, it tries to send to next available host
     *
     * @param event The event to be appended.
     * @throws org.apache.flume.EventDeliveryException
     */
    @Override
    public void append(Event event) throws EventDeliveryException {
        synchronized (this) {
            if (!isActive) {
                logger.error("Attempting to append to an already closed client.");
                throw new EventDeliveryException(
                        "Attempting to append to an already closed client.");
            }
        }
        // Sit in an finite loop and try to append!
        ClientHandler clientHandler = null;
        for (int tries = 0; tries < maxTries; ++tries) {
            try {
                clientHandler = clientPool.getClientHandler();
                clientHandler.getClient().append(event);
                return;
            } catch (EventDeliveryException e) {
                // Could not send event through this client, try to pick another client.
                logger.warn("Client failed. Exception follows: ", e);
                clientHandler.close();
            } catch (Exception e2) {
                logger.error("Failed to send event: " + e2.getMessage());
                if (clientHandler != null) {
                    clientHandler.close();
                }
                throw new EventDeliveryException(
                        "Failed to send event. Exception follows: ", e2);
            }
        }
        logger.error("Tried many times, could not send event.");
        throw new EventDeliveryException("Failed to send the event!");
    }

    /**
     * Tries to append a list of events to the currently connected client. If it
     * cannot send the event, it tries to send to next available host
     *
     * @param events The events to be appended.
     * @throws EventDeliveryException
     */
    @Override
    public void appendBatch(List<Event> events)
            throws EventDeliveryException {
        synchronized (this) {
            if (!isActive) {
                logger.error("Attempting to append to an already closed client.");
                throw new EventDeliveryException(
                        "Attempting to append to an already closed client!");
            }
        }
        ClientHandler clientHandler = null;
        for (int tries = 0; tries < maxTries; ++tries) {
            try {
                clientHandler = clientPool.getClientHandler();
                clientHandler.getClient().appendBatch(events);
                return;
            } catch (EventDeliveryException e) {
                // Could not send event through this client, try to pick another client.
                logger.warn("Client failed. Exception follows: " + e.getMessage());
                clientHandler.close();
            } catch (Exception e1) {
                logger.error("No clients active: " + e1.getMessage());
                if (clientHandler != null) {
                    clientHandler.close();
                }
                throw new EventDeliveryException("No clients currently active. " +
                        "Exception follows: ", e1);
            }
        }
        logger.error("Tried many times, could not send event.");
        throw new EventDeliveryException("Failed to send the event!");
    }

    // Returns false if and only if this client has been closed explicitly.
    // Should we check if any clients are active, if none are then return false?
    // This method has to be lightweight, so not checking if hosts are active.
    @Override
    public synchronized boolean isActive() {
        return isActive;
    }

    /**
     * Close the connection. This function is safe to call over and over.
     */
    @Override
    public synchronized void close() throws FlumeException {
        clientPool.close();
        this.router.unregisterListener(clientPool);
        this.router.stop();
    }

    @Override
    public void configure(Properties properties) throws FlumeException {
        configurationProperties = new Properties();
        configurationProperties.putAll(properties);
        configureHosts(configurationProperties);
    }
}
