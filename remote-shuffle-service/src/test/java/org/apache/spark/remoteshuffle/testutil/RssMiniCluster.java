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
import org.apache.spark.remoteshuffle.metadata.ServiceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/***
 * This is RSS test cluster without zookeeper dependency.
 */
public class RssMiniCluster {
  private static final Logger logger = LoggerFactory.getLogger(RssMiniCluster.class);

  private List<String> streamServerRootDirs = new ArrayList<>();
  private String cluster;
  private List<StreamServer> streamServers = new ArrayList<>();

  public RssMiniCluster(int numRssServers, String cluster) {
    List<String> rootDirs = StreamServerTestUtils.createTempDirectories(numRssServers);
    construct(rootDirs, cluster);
  }

  public RssMiniCluster(Collection<String> rootDirs, String cluster) {
    construct(rootDirs, cluster);
  }

  public void restartShuffleServers() {
    shutdownStreamServers();
    startStreamServers();
  }

  public void shutdownStreamServers(String serverId) {
    for (int i = 0; i < streamServers.size(); i++) {
      StreamServer server = streamServers.get(i);
      if (server.getServerId().equals(serverId)) {
        shutdownStreamServer(server);
        streamServers.remove(i);
        return;
      }
    }
    throw new IllegalArgumentException(
        String.format("Did not find server (%s) to shut down", serverId));
  }

  public void stop() {
    shutdownStreamServers();
  }

  private void construct(Collection<String> rootDirs, String cluster) {
    streamServerRootDirs = new ArrayList<>(rootDirs);
    this.cluster = cluster;

    startStreamServers();
  }

  private void startStreamServers() {
    streamServers.clear();

    // Start first RSS server which acts as registry server as well
    try {
      StreamServerConfig streamServerConfig = new StreamServerConfig();
      streamServerConfig.setServiceRegistryType(ServiceRegistry.TYPE_STANDALONE);
      streamServerConfig.setDataCenter(ServiceRegistry.DEFAULT_DATA_CENTER);
      streamServerConfig.setCluster(cluster);
      streamServerConfig.setShufflePort(0);
      streamServerConfig.setHttpPort(0);
      streamServerConfig.setRootDirectory(streamServerRootDirs.get(0));
      StreamServer streamServer = new StreamServer(streamServerConfig);
      streamServer.run();

      logger.info(String
          .format("Started stream server on shuffle port %s", streamServer.getShufflePort()));
      streamServers.add(streamServer);
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }

    // Start other RSS servers using first RSS server as registry server
    String registryServer = getRegistryServerConnection();
    for (int i = 1; i < streamServerRootDirs.size(); i++) {
      String rootDir = streamServerRootDirs.get(i);
      try {
        StreamServerConfig streamServerConfig = new StreamServerConfig();
        streamServerConfig.setServiceRegistryType(ServiceRegistry.TYPE_STANDALONE);
        streamServerConfig.setRegistryServer(registryServer);
        streamServerConfig.setDataCenter(ServiceRegistry.DEFAULT_DATA_CENTER);
        streamServerConfig.setCluster(cluster);
        streamServerConfig.setShufflePort(0);
        streamServerConfig.setHttpPort(0);
        streamServerConfig.setRootDirectory(rootDir);
        StreamServer streamServer = new StreamServer(streamServerConfig);
        streamServer.run();

        logger.info(String
            .format("Started stream server on shuffle port %s", streamServer.getShufflePort()));
        streamServers.add(streamServer);
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    }
  }

  public String getRegistryServerConnection() {
    return String.format("localhost:%s", streamServers.get(0).getShufflePort());
  }

  private void shutdownStreamServers() {
    streamServers.forEach(t -> shutdownStreamServer(t));
  }

  private void shutdownStreamServer(StreamServer server) {
    try {
      logger.info(String
          .format("Shutting down stream server on shuffle port %s", server.getShufflePort()));
      server.shutdown();
    } catch (Throwable e) {
      logger.warn(String
          .format("Failed to stop stream server on shuffle port %s", server.getShufflePort()));
    }
  }
}
