/*
 * This file is copied from Uber Remote Shuffle Service
(https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.remoteshuffle.testutil;

import org.apache.spark.remoteshuffle.StreamServer;
import org.apache.spark.remoteshuffle.StreamServerConfig;
import org.apache.spark.remoteshuffle.clients.RegistryClient;
import org.apache.spark.remoteshuffle.metadata.InMemoryServiceRegistry;
import org.apache.spark.remoteshuffle.metadata.ServiceRegistry;
import org.apache.spark.remoteshuffle.util.ExceptionUtils;
import org.apache.spark.remoteshuffle.util.RetryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestStreamServer extends StreamServer {
  private static final Logger logger = LoggerFactory.getLogger(TestStreamServer.class);
  private static final Pattern PORT_PATTERN = Pattern.compile("(?<=:)\\d+");

  private int extractPort(String serverAddress, int defaultPort) {
    // The port to extract is the first instance found in the string, otherwise it's the default.
    Matcher matcher = PORT_PATTERN.matcher(serverAddress);
    int port = defaultPort;
    if (matcher.find()) {
      port = Integer.parseInt(matcher.group());
    }
    return port;
  }

  private void startServiceRegistryServer(StreamServerConfig serverConfig) throws Exception {
    String serviceRegistryType = serverConfig.getServiceRegistryType();
    switch (serviceRegistryType) {
      case ServiceRegistry.TYPE_INMEMORY:
        return;
      case ServiceRegistry.TYPE_STANDALONE:
        return;
      default:
        throw new RuntimeException("Unsupported service registry type: " + serviceRegistryType);
    }
  }

  private TestStreamServer(StreamServerConfig serverConfig) throws Exception {
    super(serverConfig);
    startServiceRegistryServer(serverConfig);
  }

  public TestStreamServer(StreamServerConfig serverConfig, ServiceRegistry serviceRegistry)
      throws Exception {
    super(serverConfig, serviceRegistry);
    startServiceRegistryServer(serverConfig);
  }

  @Override
  public void shutdown() {
    super.shutdown(true);

    // use a socket to test and wait until server is closed
    RetryUtils.retryUntilTrue(10, TestConstants.NETWORK_TIMEOUT, () -> {
      try (Socket socket = new Socket()) {
        int timeout = 200;
        socket.connect(new InetSocketAddress("localhost", getShufflePort()), timeout);
        logger.info("Server still connectable on port {}", getShufflePort());
        return false;
      } catch (Throwable e) {
        logger.info(
            "Server not connectable on port {} which is expected due to server shutdown, exception: {}",
            getShufflePort(), ExceptionUtils.getSimpleMessage(e));
        return true;
      }
    });

  }

  public static TestStreamServer createRunningServer() {
    return createRunningServer(null);
  }

  public static TestStreamServer createRunningServer(Consumer<StreamServerConfig> configModifier) {
    // Creates with random ports.
    StreamServerConfig config = new StreamServerConfig();
    config.setShufflePort(0);
    config.setHttpPort(0);
    config.setServiceRegistryType(ServiceRegistry.TYPE_INMEMORY);
    config.setDataCenter(ServiceRegistry.DEFAULT_DATA_CENTER);
    config.setCluster(ServiceRegistry.DEFAULT_TEST_CLUSTER);

    if (configModifier != null) {
      configModifier.accept(config);
    }

    ServiceRegistry serviceRegistry = new InMemoryServiceRegistry();
    return createRunningServer(config, serviceRegistry);
  }

  public static TestStreamServer createRunningServer(StreamServerConfig serverConfig,
                                                     ServiceRegistry serviceRegistry) {
    TestStreamServer server;

    if (serverConfig.getRootDirectory() == null || serverConfig.getRootDirectory().isEmpty()) {
      String rootDir;
      try {
        rootDir = Files.createTempDirectory("StreamServer_").toString();
      } catch (IOException e) {
        throw new RuntimeException("Failed to create temp root dir", e);
      }
      serverConfig.setRootDirectory(rootDir);
    }

    try {
      server = new TestStreamServer(serverConfig, serviceRegistry);
      server.run();
      logger
          .info(String.format("Started test stream server on port: %s", server.getShufflePort()));
      // use a client to test and wait until server is ready
      RetryUtils.retryUntilTrue(10, TestConstants.NETWORK_TIMEOUT, () -> {
        try (RegistryClient registryClient = new RegistryClient("localhost",
            server.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1")) {
          registryClient.connect();
          return true;
        } catch (Throwable ex) {
          return false;
        }
      });
      return server;
    } catch (Throwable e) {
      throw new RuntimeException("Failed to start stream server", e);
    }
  }

  public static TestStreamServer createRunningServerWithLocalStandaloneRegistryServer() {
    return createRunningServerWithLocalStandaloneRegistryServer(
        ServiceRegistry.DEFAULT_TEST_CLUSTER);
  }

  public static TestStreamServer createRunningServerWithLocalStandaloneRegistryServer(
      String cluster) {
    // Creates with random ports.
    StreamServerConfig config = new StreamServerConfig();
    config.setShufflePort(0);
    config.setHttpPort(0);
    config.setServiceRegistryType(ServiceRegistry.TYPE_STANDALONE);
    config.setDataCenter(ServiceRegistry.DEFAULT_DATA_CENTER);
    config.setCluster(cluster);

    return createRunningServer(config, null);
  }
}
