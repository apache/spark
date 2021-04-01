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

package org.apache.spark.remoteshuffle.clients;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.spark.remoteshuffle.exceptions.RssInvalidStateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/***
 * This client factory creates pooled client connection and reuses the connection when possible.
 */
public class PooledWriteClientFactory implements WriteClientFactory {
  private static final Logger logger = LoggerFactory.getLogger(PooledWriteClientFactory.class);

  private static ScheduledExecutorService idleCheckExecutor =
      Executors.newSingleThreadScheduledExecutor(
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("PooledWriteClientFactory-idle-check")
              .build());

  private final static PooledWriteClientFactory instance =
      new PooledWriteClientFactory(ClientConstants.DEFAULT_CONNECTION_IDLE_TIMEOUT_MILLIS);

  public static PooledWriteClientFactory getInstance() {
    return instance;
  }

  static {
    Runtime.getRuntime().addShutdownHook(new Thread(instance::shutdown));
  }

  private final long maxIdleMillis;
  private final Map<ClientKey, ClientPool> pools = new HashMap<>();

  private final IdleCheck idleCheck;

  public PooledWriteClientFactory(long maxIdleMillis) {
    this.maxIdleMillis = maxIdleMillis;

    idleCheck = new IdleCheck(maxIdleMillis);
    schedule(idleCheck, maxIdleMillis);
  }

  @Override
  public ShuffleDataSyncWriteClient getOrCreateClient(String host, int port, int timeoutMillis,
                                                      boolean finishUploadAck, String user,
                                                      String appId, String appAttempt,
                                                      ShuffleWriteConfig shuffleWriteConfig) {
    ClientKey clientKey = new ClientKey(host, port, user, appId, appAttempt);
    ClientPool pool = getPool(clientKey);
    ShuffleDataSyncWriteClient client =
        pool.getOrCreateClient(timeoutMillis, finishUploadAck, shuffleWriteConfig);
    return client;
  }

  /***
   * Return the client to connection pool for later reuse.
   * @param client
   */
  public void returnClientToPool(PooledShuffleDataSyncWriteClient client) {
    if (!client.isReusable()) {
      logger.info(
          String.format("Client %s is not reusable, will close it instead of reuse it", client));
      client.closeWithoutReuse();
      return;
    }

    ClientKey clientKey = new ClientKey(client);
    ClientPool pool = getPool(clientKey);
    pool.returnClientToPool(client);
    logger.debug(String.format("Reuse client %s (%s)", client, clientKey));
  }

  public int getNumIdleClients() {
    synchronized (this) {
      return pools.values().stream().mapToInt(t -> t.idleClients.size()).sum();
    }
  }

  public int getNumCreatedClients() {
    synchronized (this) {
      return pools.values().stream().mapToInt(t -> t.numCreatedClients).sum();
    }
  }

  public void shutdown() {
    idleCheck.cancel();

    synchronized (this) {
      pools.values().stream().forEach(pool -> pool.idleClients.forEach(clientAndState -> {
        try {
          clientAndState.closeClient();
        } catch (Throwable ex) {
          logger
              .warn(String.format("Failed to close pooled client %s", clientAndState.client), ex);
        }
      }));
      pools.clear();
    }
  }

  private ClientPool getPool(ClientKey clientKey) {
    synchronized (this) {
      return pools.computeIfAbsent(clientKey, t -> new ClientPool(clientKey));
    }
  }

  private void closeLongIdleClients() {
    List<ClientPool> poolsToCheck = new ArrayList<>();
    synchronized (this) {
      poolsToCheck.addAll(pools.values());
    }

    for (ClientPool pool : poolsToCheck) {
      pool.closeLongIdleClients();
    }
  }

  private static void schedule(Runnable task, long delayMillis) {
    idleCheckExecutor.schedule(task, delayMillis, TimeUnit.MILLISECONDS);
  }

  private static class ClientKey {
    String host;
    int port;
    String user;
    String appId;
    String appAttempt;

    public ClientKey(PooledShuffleDataSyncWriteClient client) {
      this(client.getHost(), client.getPort(), client.getUser(), client.getAppId(),
          client.getAppAttempt());
    }

    public ClientKey(String host, int port, String user, String appId, String appAttempt) {
      this.host = host;
      this.port = port;
      this.user = user;
      this.appId = appId;
      this.appAttempt = appAttempt;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ClientKey clientKey = (ClientKey) o;
      return port == clientKey.port &&
          Objects.equals(host, clientKey.host) &&
          Objects.equals(user, clientKey.user) &&
          Objects.equals(appId, clientKey.appId) &&
          Objects.equals(appAttempt, clientKey.appAttempt);
    }

    @Override
    public int hashCode() {
      return Objects.hash(host, port, user, appId, appAttempt);
    }

    @Override
    public String toString() {
      return "ClientKey{" +
          "host='" + host + '\'' +
          ", port=" + port +
          ", user='" + user + '\'' +
          ", appId='" + appId + '\'' +
          ", appAttempt='" + appAttempt + '\'' +
          '}';
    }
  }

  private class ClientPool {
    final ClientKey clientKey;

    final int MaxClients = 5000;
    int numCreatedClients = 0;

    final List<ClientAndState> idleClients = new ArrayList<>();

    public ClientPool(ClientKey clientKey) {
      this.clientKey = clientKey;
    }

    public ShuffleDataSyncWriteClient getOrCreateClient(int timeoutMillis, boolean finishUploadAck,
                                                        ShuffleWriteConfig shuffleWriteConfig) {
      ClientAndState clientAndState;
      synchronized (this) {
        if (!idleClients.isEmpty()) {
          clientAndState = idleClients.remove(0);
        } else {
          if (numCreatedClients > MaxClients) {
            throw new RssInvalidStateException(String
                .format("Creating too many clients (current: %s, max: %s)", numCreatedClients,
                    MaxClients));
          }
          PooledShuffleDataSyncWriteClient client =
              createClient(clientKey.host, clientKey.port, timeoutMillis, finishUploadAck,
                  clientKey.user, clientKey.appId, clientKey.appAttempt, shuffleWriteConfig);
          numCreatedClients++;
          clientAndState = new ClientAndState(client);
        }
      }
      return clientAndState.getConnectedClient();
    }

    public void returnClientToPool(PooledShuffleDataSyncWriteClient client) {
      synchronized (this) {
        for (ClientAndState entry : idleClients) {
          if (entry.client.getClientId() == client.getClientId()) {
            return;
          }
        }
        idleClients.add(new ClientAndState(client, System.currentTimeMillis()));
      }
    }

    public void closeLongIdleClients() {
      List<ClientAndState> closeCandidates = new ArrayList<>();

      synchronized (this) {
        long currentTime = System.currentTimeMillis();
        for (int i = 0; i < idleClients.size(); ) {
          ClientAndState entry = idleClients.get(i);
          if (entry.exceedIdleTimeout(currentTime, maxIdleMillis)) {
            closeCandidates.add(entry);
            idleClients.remove(i);
          } else {
            i++;
          }
        }
      }

      for (ClientAndState entry : closeCandidates) {
        entry.closeClient();
      }
    }

    private PooledShuffleDataSyncWriteClient createClient(String host, int port, int timeoutMillis,
                                                          boolean finishUploadAck, String user,
                                                          String appId, String appAttempt,
                                                          ShuffleWriteConfig shuffleWriteConfig) {
      ShuffleDataSyncWriteClient client;
      client =
          new PlainShuffleDataSyncWriteClient(host, port, timeoutMillis, finishUploadAck, user,
              appId, appAttempt, shuffleWriteConfig);
      logger.info(String.format("Created new client: %s", client));
      return new PooledShuffleDataSyncWriteClient(client, PooledWriteClientFactory.this);
    }
  }

  private class ClientAndState {
    final PooledShuffleDataSyncWriteClient client;
    long lastActiveTime = 0;

    public ClientAndState(PooledShuffleDataSyncWriteClient client) {
      this.client = client;
    }

    public ClientAndState(PooledShuffleDataSyncWriteClient client, long lastActiveTime) {
      this.client = client;
      this.lastActiveTime = lastActiveTime;
    }

    public PooledShuffleDataSyncWriteClient getConnectedClient() {
      synchronized (this) {
        if (lastActiveTime > 0) {
          return client;
        }
        client.connect();
        lastActiveTime = System.currentTimeMillis();
        return client;
      }
    }

    public boolean exceedIdleTimeout(long currentTime, long maxIdleMillis) {
      return lastActiveTime > 0 && currentTime - lastActiveTime >= maxIdleMillis;
    }

    public void closeClient() {
      synchronized (this) {
        try {
          client.closeWithoutReuse();
        } catch (Throwable ex) {
          logger.warn(String.format("Failed to close client: %s", client));
        }
      }
    }
  }

  private class IdleCheck implements Runnable {

    private final long idleTimeoutMillis;

    private volatile boolean canceled = false;

    public IdleCheck(long idleTimeoutMillis) {
      this.idleTimeoutMillis = idleTimeoutMillis;
    }

    @Override
    public void run() {
      try {
        if (canceled) {
          return;
        }

        checkIdle();
      } catch (Throwable ex) {
        logger.warn("Failed to run idle check", ex);
      }
    }

    public void cancel() {
      canceled = true;
    }

    private void checkIdle() {
      closeLongIdleClients();

      schedule(this, idleTimeoutMillis);
    }
  }
}
