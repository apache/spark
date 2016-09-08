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

package org.apache.spark.network.yarn;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.api.*;
import org.apache.spark.network.util.LevelDBProvider;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.TransportContext;
import org.apache.spark.network.sasl.SaslServerBootstrap;
import org.apache.spark.network.sasl.ShuffleSecretManager;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.shuffle.ExternalShuffleBlockHandler;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.yarn.util.HadoopConfigProvider;

/**
 * An external shuffle service used by Spark on Yarn.
 *
 * This is intended to be a long-running auxiliary service that runs in the NodeManager process.
 * A Spark application may connect to this service by setting `spark.shuffle.service.enabled`.
 * The application also automatically derives the service port through `spark.shuffle.service.port`
 * specified in the Yarn configuration. This is so that both the clients and the server agree on
 * the same port to communicate on.
 *
 * The service also optionally supports authentication. This ensures that executors from one
 * application cannot read the shuffle files written by those from another. This feature can be
 * enabled by setting `spark.authenticate` in the Yarn configuration before starting the NM.
 * Note that the Spark application must also set `spark.authenticate` manually and, unlike in
 * the case of the service port, will not inherit this setting from the Yarn configuration. This
 * is because an application running on the same Yarn cluster may choose to not use the external
 * shuffle service, in which case its setting of `spark.authenticate` should be independent of
 * the service's.
 */
public class YarnShuffleService extends AuxiliaryService {
  private static final Logger logger = LoggerFactory.getLogger(YarnShuffleService.class);

  // Port on which the shuffle server listens for fetch requests
  private static final String SPARK_SHUFFLE_SERVICE_PORT_KEY = "spark.shuffle.service.port";
  private static final int DEFAULT_SPARK_SHUFFLE_SERVICE_PORT = 7337;

  // Whether the shuffle server should authenticate fetch requests
  private static final String SPARK_AUTHENTICATE_KEY = "spark.authenticate";
  private static final boolean DEFAULT_SPARK_AUTHENTICATE = false;

  private static final String RECOVERY_FILE_NAME = "registeredExecutors.ldb";
  private static final String SECRETS_RECOVERY_FILE_NAME = "sparkShuffleRecovery.ldb";

  // Whether failure during service initialization should stop the NM.
  @VisibleForTesting
  static final String STOP_ON_FAILURE_KEY = "spark.yarn.shuffle.stopOnFailure";
  private static final boolean DEFAULT_STOP_ON_FAILURE = false;

  // just for testing when you want to find an open port
  @VisibleForTesting
  static int boundPort = -1;
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final String APP_CREDS_KEY_PREFIX = "AppCreds";
  private static final LevelDBProvider.StoreVersion CURRENT_VERSION = new LevelDBProvider
      .StoreVersion(1, 0);

  // just for integration tests that want to look at this file -- in general not sensible as
  // a static
  @VisibleForTesting
  static YarnShuffleService instance;

  // An entity that manages the shuffle secret per application
  // This is used only if authentication is enabled
  private ShuffleSecretManager secretManager;

  // The actual server that serves shuffle files
  private TransportServer shuffleServer = null;

  private Configuration _conf = null;

  // The recovery path used to shuffle service recovery
  @VisibleForTesting
  Path _recoveryPath = null;

  // Handles registering executors and opening shuffle blocks
  @VisibleForTesting
  ExternalShuffleBlockHandler blockHandler;

  // Where to store & reload executor info for recovering state after an NM restart
  @VisibleForTesting
  File registeredExecutorFile;

  // Where to store & reload application secrets for recovering state after an NM restart
  @VisibleForTesting
  File secretsFile;

  private DB db;

  public YarnShuffleService() {
    super("spark_shuffle");
    logger.info("Initializing YARN shuffle service for Spark");
    instance = this;
  }

  /**
   * Return whether authentication is enabled as specified by the configuration.
   * If so, fetch requests will fail unless the appropriate authentication secret
   * for the application is provided.
   */
  private boolean isAuthenticationEnabled() {
    return secretManager != null;
  }

  /**
   * Start the shuffle server with the given configuration.
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    _conf = conf;

    boolean stopOnFailure = conf.getBoolean(STOP_ON_FAILURE_KEY, DEFAULT_STOP_ON_FAILURE);

    try {
      // In case this NM was killed while there were running spark applications, we need to restore
      // lost state for the existing executors. We look for an existing file in the NM's local dirs.
      // If we don't find one, then we choose a file to use to save the state next time.  Even if
      // an application was stopped while the NM was down, we expect yarn to call stopApplication()
      // when it comes back
      registeredExecutorFile =
        new File(getRecoveryPath().toUri().getPath(), RECOVERY_FILE_NAME);

      TransportConf transportConf = new TransportConf("shuffle", new HadoopConfigProvider(conf));
      blockHandler = new ExternalShuffleBlockHandler(transportConf, registeredExecutorFile);

      // If authentication is enabled, set up the shuffle server to use a
      // special RPC handler that filters out unauthenticated fetch requests
      List<TransportServerBootstrap> bootstraps = Lists.newArrayList();
      boolean authEnabled = conf.getBoolean(SPARK_AUTHENTICATE_KEY, DEFAULT_SPARK_AUTHENTICATE);
      if (authEnabled) {
        createSecretManager();
        bootstraps.add(new SaslServerBootstrap(transportConf, secretManager));
      }

      int port = conf.getInt(
        SPARK_SHUFFLE_SERVICE_PORT_KEY, DEFAULT_SPARK_SHUFFLE_SERVICE_PORT);
      TransportContext transportContext = new TransportContext(transportConf, blockHandler);
      shuffleServer = transportContext.createServer(port, bootstraps);
      // the port should normally be fixed, but for tests its useful to find an open port
      port = shuffleServer.getPort();
      boundPort = port;
      String authEnabledString = authEnabled ? "enabled" : "not enabled";
      logger.info("Started YARN shuffle service for Spark on port {}. " +
        "Authentication is {}.  Registered executor file is {}", port, authEnabledString,
        registeredExecutorFile);
    } catch (Exception e) {
      if (stopOnFailure) {
        throw e;
      } else {
        noteFailure(e);
      }
    }
  }

  private void createSecretManager() throws IOException {
    secretManager = new ShuffleSecretManager();
    secretsFile = new File(getRecoveryPath().toUri().getPath(), SECRETS_RECOVERY_FILE_NAME);
 
    // Make sure this is protected in case its not in the NM recovery dir
    FileSystem fs = FileSystem.getLocal(_conf);
    fs.mkdirs(new Path(secretsFile.getPath()), new FsPermission((short)0700));

    db = LevelDBProvider.initLevelDB(secretsFile, CURRENT_VERSION, mapper);
    logger.info("Recovery location is: " + secretsFile.getPath());
    if (db != null) {
      logger.info("Going to reload spark shuffle data");
      DBIterator itr = db.iterator();
      itr.seek(APP_CREDS_KEY_PREFIX.getBytes(StandardCharsets.UTF_8));
      while (itr.hasNext()) {
        Map.Entry<byte[], byte[]> e = itr.next();
        String key = new String(e.getKey(), StandardCharsets.UTF_8);
        if (!key.startsWith(APP_CREDS_KEY_PREFIX)) {
          break;
        }
        String id = parseDbAppKey(key);
        ByteBuffer secret = mapper.readValue(e.getValue(), ByteBuffer.class);
        logger.info("Reloading tokens for app: " + id);
        secretManager.registerApp(id, secret);
      }
    }
  }

  private static String parseDbAppKey(String s) throws IOException {
    if (!s.startsWith(APP_CREDS_KEY_PREFIX)) {
      throw new IllegalArgumentException("expected a string starting with " + APP_CREDS_KEY_PREFIX);
    }
    String json = s.substring(APP_CREDS_KEY_PREFIX.length() + 1);
    AppId parsed = mapper.readValue(json, AppId.class);
    return parsed.appId;
  }

  private static byte[] dbAppKey(AppId appExecId) throws IOException {
    // we stick a common prefix on all the keys so we can find them in the DB
    String appExecJson = mapper.writeValueAsString(appExecId);
    String key = (APP_CREDS_KEY_PREFIX + ";" + appExecJson);
    return key.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public void initializeApplication(ApplicationInitializationContext context) {
    String appId = context.getApplicationId().toString();
    try {
      ByteBuffer shuffleSecret = context.getApplicationDataForService();
      logger.info("Initializing application {}", appId);
      if (isAuthenticationEnabled()) {
        AppId fullId = new AppId(appId);
        if (db != null) {
          byte[] key = dbAppKey(fullId);
          byte[] value = mapper.writeValueAsString(shuffleSecret).getBytes(StandardCharsets.UTF_8);
          db.put(key, value);
        }
        secretManager.registerApp(appId, shuffleSecret);
      }
    } catch (Exception e) {
      logger.error("Exception when initializing application {}", appId, e);
    }
  }

  @Override
  public void stopApplication(ApplicationTerminationContext context) {
    String appId = context.getApplicationId().toString();
    try {
      logger.info("Stopping application {}", appId);
      if (isAuthenticationEnabled()) {
        AppId fullId = new AppId(appId);
        if (db != null) {
          try {
            db.delete(dbAppKey(fullId));
          } catch (IOException e) {
            logger.error("Error deleting {} from executor state db", appId, e);
          }
        }
        secretManager.unregisterApp(appId);
      }
      blockHandler.applicationRemoved(appId, false /* clean up local dirs */);
    } catch (Exception e) {
      logger.error("Exception when stopping application {}", appId, e);
    }
  }

  @Override
  public void initializeContainer(ContainerInitializationContext context) {
    ContainerId containerId = context.getContainerId();
    logger.info("Initializing container {}", containerId);
  }

  @Override
  public void stopContainer(ContainerTerminationContext context) {
    ContainerId containerId = context.getContainerId();
    logger.info("Stopping container {}", containerId);
  }

  /**
   * Close the shuffle server to clean up any associated state.
   */
  @Override
  protected void serviceStop() {
    try {
      if (shuffleServer != null) {
        shuffleServer.close();
      }
      if (blockHandler != null) {
        blockHandler.close();
      }
      if (db != null) {
        db.close();
      } 
    } catch (Exception e) {
      logger.error("Exception when stopping service", e);
    }
  }

  // Not currently used
  @Override
  public ByteBuffer getMetaData() {
    return ByteBuffer.allocate(0);
  }

  /**
   * Set the recovery path for shuffle service recovery when NM is restarted. The method will be
   * overrode and called when Hadoop version is 2.5+ and NM recovery is enabled, otherwise we
   * have to manually call this to set our own recovery path.
   */
  public void setRecoveryPath(Path recoveryPath) {
    _recoveryPath = recoveryPath;
  }

  /**
   * Get the recovery path, this will override the default one to get our own maintained
   * recovery path.
   */
  protected Path getRecoveryPath() {
    String[] localDirs = _conf.getTrimmedStrings("yarn.nodemanager.local-dirs");
    for (String dir : localDirs) {
      File f = new File(new Path(dir).toUri().getPath(), RECOVERY_FILE_NAME);
      if (f.exists()) {
        if (_recoveryPath == null) {
          // If NM recovery is not enabled, we should specify the recovery path using NM local
          // dirs, which is compatible with the old code.
          _recoveryPath = new Path(dir);
        } else {
          // If NM recovery is enabled and the recovery file exists in old NM local dirs, which
          // means old version of Spark already generated the recovery file, we should copy the
          // old file in to a new recovery path for the compatibility.
          if (!f.renameTo(new File(_recoveryPath.toUri().getPath(), RECOVERY_FILE_NAME))) {
            // Fail to move recovery file to new path
            logger.error("Failed to move recovery file {} to the path {}",
              RECOVERY_FILE_NAME, _recoveryPath.toString());
          }
        }
        break;
      }
    }

    if (_recoveryPath == null) {
      _recoveryPath = new Path(localDirs[0]);
    }

    return _recoveryPath;
  }

  /**
   * Simply encodes an application ID.
   */
  public static class AppId {
    public final String appId;

    @JsonCreator
    public AppId(@JsonProperty("appId") String appId) {
      this.appId = appId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      AppId appExecId = (AppId) o;
      return Objects.equal(appId, appExecId.appId);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(appId);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("appId", appId)
          .toString();
    }
  }

}
