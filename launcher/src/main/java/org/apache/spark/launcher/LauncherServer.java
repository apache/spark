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

package org.apache.spark.launcher;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.spark.launcher.LauncherProtocol.*;

/**
 * A server that listens locally for connections from client launched by the library. Each client
 * has a secret that it needs to send to the server to identify itself and establish the session.
 *
 * I/O is currently blocking (one thread per client). Clients have a limited time to connect back
 * to the server, otherwise the server will ignore the connection.
 *
 * === Architecture Overview ===
 *
 * The launcher server is used when Spark apps are launched as separate processes than the calling
 * app. It looks more or less like the following:
 *
 *         -----------------------                       -----------------------
 *         |      User App       |     spark-submit      |      Spark App      |
 *         |                     |  -------------------> |                     |
 *         |         ------------|                       |-------------        |
 *         |         |           |        hello          |            |        |
 *         |         | L. Server |<----------------------| L. Backend |        |
 *         |         |           |                       |            |        |
 *         |         -------------                       -----------------------
 *         |               |     |                              ^
 *         |               v     |                              |
 *         |        -------------|                              |
 *         |        |            |      <per-app channel>       |
 *         |        | App Handle |<------------------------------
 *         |        |            |
 *         -----------------------
 *
 * The server is started on demand and remains active while there are active or outstanding clients,
 * to avoid opening too many ports when multiple clients are launched. Each client is given a unique
 * secret, and have a limited amount of time to connect back
 * ({@link SparkLauncher#CHILD_CONNECTION_TIMEOUT}), at which point the server will throw away
 * that client's state. A client is only allowed to connect back to the server once.
 *
 * The launcher server listens on the localhost only, so it doesn't need access controls (aside from
 * the per-app secret) nor encryption. It thus requires that the launched app has a local process
 * that communicates with the server. In cluster mode, this means that the client that launches the
 * application must remain alive for the duration of the application (or until the app handle is
 * disconnected).
 */
class LauncherServer implements Closeable {

  private static final Logger LOG = Logger.getLogger(LauncherServer.class.getName());
  private static final String THREAD_NAME_FMT = "LauncherServer-%d";
  private static final long DEFAULT_CONNECT_TIMEOUT = 10000L;

  /** For creating secrets used for communication with child processes. */
  private static final SecureRandom RND = new SecureRandom();

  private static volatile LauncherServer serverInstance;

  static synchronized LauncherServer getOrCreateServer() throws IOException {
    LauncherServer server;
    do {
      server = serverInstance != null ? serverInstance : new LauncherServer();
    } while (!server.running);

    server.ref();
    serverInstance = server;
    return server;
  }

  // For testing.
  static synchronized LauncherServer getServer() {
    return serverInstance;
  }

  private final AtomicLong refCount;
  private final AtomicLong threadIds;
  private final ConcurrentMap<String, AbstractAppHandle> secretToPendingApps;
  private final List<ServerConnection> clients;
  private final ServerSocket server;
  private final Thread serverThread;
  private final ThreadFactory factory;
  private final Timer timeoutTimer;

  private volatile boolean running;

  private LauncherServer() throws IOException {
    this.refCount = new AtomicLong(0);

    ServerSocket server = new ServerSocket();
    try {
      server.setReuseAddress(true);
      server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));

      this.clients = new ArrayList<>();
      this.threadIds = new AtomicLong();
      this.factory = new NamedThreadFactory(THREAD_NAME_FMT);
      this.secretToPendingApps = new ConcurrentHashMap<>();
      this.timeoutTimer = new Timer("LauncherServer-TimeoutTimer", true);
      this.server = server;
      this.running = true;

      this.serverThread = factory.newThread(this::acceptConnections);
      serverThread.start();
    } catch (IOException ioe) {
      close();
      throw ioe;
    } catch (Exception e) {
      close();
      throw new IOException(e);
    }
  }

  /**
   * Registers a handle with the server, and returns the secret the child app needs to connect
   * back.
   */
  synchronized String registerHandle(AbstractAppHandle handle) {
    String secret = createSecret();
    secretToPendingApps.put(secret, handle);
    return secret;
  }

  @Override
  public void close() throws IOException {
    synchronized (this) {
      if (!running) {
        return;
      }
      running = false;
    }

    synchronized(LauncherServer.class) {
      serverInstance = null;
    }

    timeoutTimer.cancel();
    server.close();
    synchronized (clients) {
      List<ServerConnection> copy = new ArrayList<>(clients);
      clients.clear();
      for (ServerConnection client : copy) {
        client.close();
      }
    }

    if (serverThread != null) {
      try {
        serverThread.join();
      } catch (InterruptedException ie) {
        // no-op
      }
    }
  }

  void ref() {
    refCount.incrementAndGet();
  }

  void unref() {
    synchronized(LauncherServer.class) {
      if (refCount.decrementAndGet() == 0) {
        try {
          close();
        } catch (IOException ioe) {
          // no-op.
        }
      }
    }
  }

  int getPort() {
    return server.getLocalPort();
  }

  /**
   * Removes the client handle from the pending list (in case it's still there), and unrefs
   * the server.
   */
  void unregister(AbstractAppHandle handle) {
    for (Map.Entry<String, AbstractAppHandle> e : secretToPendingApps.entrySet()) {
      if (e.getValue().equals(handle)) {
        String secret = e.getKey();
        secretToPendingApps.remove(secret);
        break;
      }
    }

    unref();
  }

  private void acceptConnections() {
    try {
      while (running) {
        final Socket client = server.accept();
        TimerTask timeout = new TimerTask() {
          @Override
          public void run() {
            LOG.warning("Timed out waiting for hello message from client.");
            try {
              client.close();
            } catch (IOException ioe) {
              // no-op.
            }
          }
        };
        ServerConnection clientConnection = new ServerConnection(client, timeout);
        Thread clientThread = factory.newThread(clientConnection);
        clientConnection.setConnectionThread(clientThread);
        synchronized (clients) {
          clients.add(clientConnection);
        }

        long timeoutMs = getConnectionTimeout();
        // 0 is used for testing to avoid issues with clock resolution / thread scheduling,
        // and force an immediate timeout.
        if (timeoutMs > 0) {
          timeoutTimer.schedule(timeout, timeoutMs);
        } else {
          timeout.run();
        }

        clientThread.start();
      }
    } catch (IOException ioe) {
      if (running) {
        LOG.log(Level.SEVERE, "Error in accept loop.", ioe);
      }
    }
  }

  private long getConnectionTimeout() {
    String value = SparkLauncher.launcherConfig.get(SparkLauncher.CHILD_CONNECTION_TIMEOUT);
    return (value != null) ? Long.parseLong(value) : DEFAULT_CONNECT_TIMEOUT;
  }

  private String createSecret() {
    while (true) {
      byte[] secret = new byte[128];
      RND.nextBytes(secret);

      StringBuilder sb = new StringBuilder();
      for (byte b : secret) {
        int ival = b >= 0 ? b : Byte.MAX_VALUE - b;
        if (ival < 0x10) {
          sb.append("0");
        }
        sb.append(Integer.toHexString(ival));
      }

      String secretStr = sb.toString();
      if (!secretToPendingApps.containsKey(secretStr)) {
        return secretStr;
      }
    }
  }

  class ServerConnection extends LauncherConnection {

    private TimerTask timeout;
    private volatile Thread connectionThread;
    private volatile AbstractAppHandle handle;

    ServerConnection(Socket socket, TimerTask timeout) throws IOException {
      super(socket);
      this.timeout = timeout;
    }

    void setConnectionThread(Thread t) {
      this.connectionThread = t;
    }

    @Override
    protected void handle(Message msg) throws IOException {
      try {
        if (msg instanceof Hello) {
          timeout.cancel();
          timeout = null;
          Hello hello = (Hello) msg;
          AbstractAppHandle handle = secretToPendingApps.remove(hello.secret);
          if (handle != null) {
            handle.setConnection(this);
            handle.setState(SparkAppHandle.State.CONNECTED);
            this.handle = handle;
          } else {
            throw new IllegalArgumentException("Received Hello for unknown client.");
          }
        } else {
          String msgClassName = msg != null ? msg.getClass().getName() : "no message";
          if (handle == null) {
            throw new IllegalArgumentException("Expected hello, got: " + msgClassName);
          }
          if (msg instanceof SetAppId) {
            SetAppId set = (SetAppId) msg;
            handle.setAppId(set.appId);
          } else if (msg instanceof SetState) {
            handle.setState(((SetState)msg).state);
          } else {
            throw new IllegalArgumentException("Invalid message: " + msgClassName);
          }
        }
      } catch (Exception e) {
        LOG.log(Level.INFO, "Error handling message from client.", e);
        if (timeout != null) {
          timeout.cancel();
        }
        close();
        if (handle != null) {
          handle.dispose();
        }
      } finally {
        timeoutTimer.purge();
      }
    }

    @Override
    public void close() throws IOException {
      if (!isOpen()) {
        return;
      }

      synchronized (clients) {
        clients.remove(this);
      }

      super.close();
    }

    /**
     * Wait for the remote side to close the connection so that any pending data is processed.
     * This ensures any changes reported by the child application take effect.
     *
     * This method allows a short period for the above to happen (same amount of time as the
     * connection timeout, which is configurable). This should be fine for well-behaved
     * applications, where they close the connection around the same time the app handle detects the
     * app has finished.
     *
     * In case the connection is not closed within the grace period, this method forcefully closes
     * it and any subsequent data that may arrive will be ignored.
     */
    public void waitForClose() throws IOException {
      Thread connThread = this.connectionThread;
      if (Thread.currentThread() != connThread) {
        try {
          connThread.join(getConnectionTimeout());
        } catch (InterruptedException ie) {
          // Ignore.
        }

        if (connThread.isAlive()) {
          LOG.log(Level.WARNING, "Timed out waiting for child connection to close.");
          close();
        }
      }
    }

  }

}
