/*
 * This file is copied from Uber Remote Shuffle Service
 * (https://github.com/uber/RemoteShuffleService) and modified.
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

package org.apache.spark.remoteshuffle.metadata;

import com.uber.m3.tally.Scope;
import org.apache.spark.remoteshuffle.clients.BusyStatusSocketClient;
import org.apache.spark.remoteshuffle.common.ServerCandidate;
import org.apache.spark.remoteshuffle.common.ServerDetail;
import org.apache.spark.remoteshuffle.exceptions.RssException;
import org.apache.spark.remoteshuffle.exceptions.RssServerDownException;
import org.apache.spark.remoteshuffle.messages.GetBusyStatusResponse;
import org.apache.spark.remoteshuffle.metrics.M3Stats;
import org.apache.spark.remoteshuffle.util.NetworkUtils;
import org.apache.spark.remoteshuffle.util.RetryUtils;
import org.apache.spark.remoteshuffle.util.ServerHostAndPort;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

public class ServiceRegistryUtils {
  private static final Logger logger = LoggerFactory.getLogger(ServiceRegistryUtils.class);

  /***
   * Get all servers from service registry with retry
   * @param serviceRegistry service registry instance
   * @param maxServerCount max server count to return
   * @param maxTryMillis max trying milliseconds
   * @param dataCenter data center
   * @param cluster cluster
   * @return servers
   */
  public static List<ServerDetail> getReachableServers(ServiceRegistry serviceRegistry,
                                                       int maxServerCount,
                                                       long maxTryMillis, String dataCenter,
                                                       String cluster,
                                                       Collection<String> excludeHosts) {
    // TODO make following configurable
    // get extra servers in case there are bad servers, will remove those extra servers in the final server list
    final int extraServerCount = Math.min(5, maxServerCount);

    final int serverCandidateCount = maxServerCount + extraServerCount;

    final Long CONCURRENT_CONNS = new Long(1);

    int retryIntervalMillis = 100;
    List<ServerDetail> serverInfos = RetryUtils.retryUntilNotNull(
        retryIntervalMillis,
        maxTryMillis,
        () -> {
          try {
            logger.info(
                String.format("Trying to get max %s RSS servers, data center: %s, cluster: %s, " +
                        "exclude hosts: %s", serverCandidateCount, dataCenter, cluster,
                    StringUtils.join(excludeHosts, ",")));
            return serviceRegistry
                .getServers(dataCenter, cluster, serverCandidateCount, excludeHosts);
          } catch (Throwable ex) {
            logger.warn("Failed to call ServiceRegistry.getServers", ex);
            return null;
          }
        });
    if (serverInfos == null || serverInfos.isEmpty()) {
      throw new RssException("Failed to get all RSS servers");
    }

    // some hosts may get UnknowHostException sometimes, exclude those hosts
    logger.info(
        String.format("Got %s RSS servers from service registry, checking their connectivity",
            serverInfos.size()));
    ConcurrentLinkedQueue<String> unreachableHosts = new ConcurrentLinkedQueue<>();
    List<ServerCandidate> serverCandidates = serverInfos.parallelStream().map(t -> {
      ServerHostAndPort hostAndPort = ServerHostAndPort.fromString(t.getConnectionString());
      String host = hostAndPort.getHost();
      int port = hostAndPort.getPort();
      long startTime = System.currentTimeMillis();
      try (BusyStatusSocketClient busyStatusSocketClient = new BusyStatusSocketClient(host, port,
          NetworkUtils.DEFAULT_REACHABLE_TIMEOUT, "")) {
        GetBusyStatusResponse getBusyStatusResponse = busyStatusSocketClient.getBusyStatus();
        long requestLatency = System.currentTimeMillis() - startTime;
        return new ServerCandidate(t, requestLatency,
            getBusyStatusResponse.getMetrics().get(CONCURRENT_CONNS));
      } catch (Throwable ex) {
        logger.warn(String.format("Detected unreachable host %s", host), ex);
        unreachableHosts.add(host);
        return null;
      }
    })
        .filter(t -> t != null)
        .sorted((o1, o2) -> {
          // We wanted to keep 500 ms offset to compare
          long offfset = 500;
          Long latency1 = o1.getRequestLatency();
          Long latency2 = o2.getRequestLatency();

          long diff = latency1 - latency2;
          if (diff > offfset || diff < (-1 * offfset)) {
            int comp = Long.compare(latency1, latency2);
            if (comp != 0) {
              return comp;
            }
          }
          Long connections1 = o1.getConcurrentConnections();
          Long connections2 = o2.getConcurrentConnections();
          return Long.compare(connections1, connections2);
        })
        .collect(Collectors.toList());

    for (String unreachableHost : unreachableHosts) {
      Map<String, String> tags = new HashMap<>();
      tags.put("remote", unreachableHost);
      Scope scope = M3Stats.createSubScope(tags);
      scope.counter("unreachableHosts").inc(1);
    }

    serverInfos =
        serverCandidates.stream().limit(maxServerCount).map(ServerCandidate::getServerDetail)
            .collect(Collectors.toList());

    if (serverInfos.size() < serverCandidates.size()) {
      for (int i = serverInfos.size(); i < serverCandidates.size(); i++) {
        ServerCandidate ignoredServerCandidate = serverCandidates.get(i);
        logger.info("Ignore RSS server candidate: {}", ignoredServerCandidate);
      }
    }

    return serverInfos;
  }

  /***
   * Look up servers by server ids.
   * @param serviceRegistry service registry
   * @param maxTryMillis max trying milliseconds
   * @param dataCenter data center
   * @param cluster cluster
   * @param serverIds list of server ids
   * @return servers
   */
  public static List<ServerDetail> lookupServers(ServiceRegistry serviceRegistry,
                                                 long maxTryMillis, String dataCenter,
                                                 String cluster, Collection<String> serverIds) {
    int retryIntervalMillis = 100;
    return RetryUtils.retryUntilNotNull(
        retryIntervalMillis,
        maxTryMillis,
        () -> {
          try {
            logger.info(String.format(
                "Trying to look up RSS servers (data center: %s, cluster: %s) for %s",
                dataCenter, cluster, serverIds.stream().collect(Collectors.joining(","))));
            return serviceRegistry.lookupServers(dataCenter, cluster, serverIds);
          } catch (Throwable ex) {
            logger.warn("Failed to call ServiceRegistry.lookupServers", ex);
            return null;
          }
        });
  }

  /***
   * Check whether the servers are alive
   * @param serviceRegistry service registry
   * @param dataCenter data center
   * @param cluster cluster
   * @param servers servers to check
   */
  public static void checkServersAlive(ServiceRegistry serviceRegistry, String dataCenter,
                                       String cluster, Collection<ServerDetail> servers) {
    List<String> serverIds =
        servers.stream().map(t -> t.getServerId()).collect(Collectors.toList());
    List<ServerDetail> latestServers;
    try {
      latestServers = serviceRegistry.lookupServers(dataCenter, cluster, serverIds);
    } catch (Throwable ex) {
      String serversInfo =
          servers.stream().map(t -> t.toString()).collect(Collectors.joining(", "));
      throw new RssServerDownException(
          String.format("Some of the servers were down: %s", serversInfo));
    }

    List<ServerDetail> oldServers = new ArrayList<>(servers);
    for (int i = 0; i < servers.size(); i++) {
      ServerDetail oldServer = oldServers.get(i);
      ServerDetail latestServer = latestServers.get(i);
      if (!oldServer.equals(latestServer)) {
        throw new RssServerDownException(String.format("Server was restarted: %s", oldServer));
      }
    }
  }

  public static List<ServerDetail> excludeByHosts(List<ServerDetail> servers, int maxCount,
                                                  Collection<String> excludeHosts) {
    return servers.stream().filter(t -> !shouldExclude(t.getConnectionString(), excludeHosts))
        .limit(maxCount)
        .collect(Collectors.toList());
  }

  private static boolean shouldExclude(String connectionString, Collection<String> excludeHosts) {
    for (String str : excludeHosts) {
      if (connectionString.contains(str)) {
        return true;
      }
    }
    return false;
  }
}
