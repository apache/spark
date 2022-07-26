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
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.api.*;
import org.apache.spark.network.shuffle.MergedShuffleFileManager;
import org.apache.spark.network.shuffle.NoOpMergedShuffleFileManager;
import org.apache.spark.network.util.LevelDBProvider;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.TransportContext;
import org.apache.spark.network.crypto.AuthServerBootstrap;
import org.apache.spark.network.sasl.ShuffleSecretManager;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.shuffle.ExternalBlockHandler;
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
 *
 * The shuffle service will produce metrics via the YARN NodeManager's {@code metrics2} system
 * under a namespace specified by the {@value SPARK_SHUFFLE_SERVICE_METRICS_NAMESPACE_KEY} config.
 *
 * By default, all configurations for the shuffle service will be taken directly from the
 * Hadoop {@link Configuration} passed by the YARN NodeManager. It is also possible to configure
 * the shuffle service by placing a resource named
 * {@value SHUFFLE_SERVICE_CONF_OVERLAY_RESOURCE_NAME} into the classpath, which should be an
 * XML file in the standard Hadoop Configuration resource format. Note that when the shuffle
 * service is loaded in the default manner, without configuring
 * {@code yarn.nodemanager.aux-services.<service>.classpath}, this file must be on the classpath
 * of the NodeManager itself. When using the {@code classpath} configuration, it can be present
 * either on the NodeManager's classpath, or specified in the classpath configuration.
 * This {@code classpath} configuration is only supported on YARN versions >= 2.9.0.
 */
public class YarnShuffleService extends AuxiliaryService {
  private static final Logger defaultLogger = LoggerFactory.getLogger(YarnShuffleService.class);
  private Logger logger = defaultLogger;

  // Port on which the shuffle server listens for fetch requests
  private static final String SPARK_SHUFFLE_SERVICE_PORT_KEY = "spark.shuffle.service.port";
  private static final int DEFAULT_SPARK_SHUFFLE_SERVICE_PORT = 7337;

  /**
   * The namespace to use for the metrics record which will contain all metrics produced by the
   * shuffle service.
   */
  static final String SPARK_SHUFFLE_SERVICE_METRICS_NAMESPACE_KEY =
      "spark.yarn.shuffle.service.metrics.namespace";
  private static final String DEFAULT_SPARK_SHUFFLE_SERVICE_METRICS_NAME = "sparkShuffleService";

  /**
   * The namespace to use for the logs produced by the shuffle service
   */
  static final String SPARK_SHUFFLE_SERVICE_LOGS_NAMESPACE_KEY =
      "spark.yarn.shuffle.service.logs.namespace";

  // Whether the shuffle server should authenticate fetch requests
  private static final String SPARK_AUTHENTICATE_KEY = "spark.authenticate";
  private static final boolean DEFAULT_SPARK_AUTHENTICATE = false;

  private static final String RECOVERY_FILE_NAME = "registeredExecutors.ldb";
  private static final String SECRETS_RECOVERY_FILE_NAME = "sparkShuffleRecovery.ldb";
  @VisibleForTesting
  static final String SPARK_SHUFFLE_MERGE_RECOVERY_FILE_NAME = "sparkShuffleMergeRecovery.ldb";

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

  /**
   * The name of the resource to search for on the classpath to find a shuffle service-specific
   * configuration overlay. If found, this will be parsed as a standard Hadoop
   * {@link Configuration config} file and will override the configs passed from the NodeManager.
   */
  static final String SHUFFLE_SERVICE_CONF_OVERLAY_RESOURCE_NAME = "spark-shuffle-site.xml";

  // just for integration tests that want to look at this file -- in general not sensible as
  // a static
  @VisibleForTesting
  static YarnShuffleService instance;

  // An entity that manages the shuffle secret per application
  // This is used only if authentication is enabled
  @VisibleForTesting
  ShuffleSecretManager secretManager;

  // The actual server that serves shuffle files
  private TransportServer shuffleServer = null;

  private TransportContext transportContext = null;

  @VisibleForTesting
  Configuration _conf = null;

  // The recovery path used to shuffle service recovery
  @VisibleForTesting
  Path _recoveryPath = null;

  // Handles registering executors and opening shuffle blocks
  @VisibleForTesting
  ExternalBlockHandler blockHandler;

  // Handles merged shuffle registration, push blocks and finalization
  @VisibleForTesting
  MergedShuffleFileManager shuffleMergeManager;

  // Where to store & reload executor info for recovering state after an NM restart
  @VisibleForTesting
  File registeredExecutorFile;

  // Where to store & reload application secrets for recovering state after an NM restart
  @VisibleForTesting
  File secretsFile;

  // Where to store & reload merge manager info for recovering state after an NM restart
  @VisibleForTesting
  File mergeManagerFile;

  private DB db;

  public YarnShuffleService() {
    // The name of the auxiliary service configured within the NodeManager
    // (`yarn.nodemanager.aux-services`) is treated as the source-of-truth, so this one can be
    // arbitrary. The NodeManager will log a warning if the configured name doesn't match this name,
    // to inform operators of a potential misconfiguration, but this name is otherwise not used.
    // It is hard-coded instead of using the value of the `spark.shuffle.service.name` configuration
    // because at this point in instantiation there is no Configuration object; it is not passed
    // until `serviceInit` is called, at which point it's too late to adjust the name.
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
  protected void serviceInit(Configuration externalConf) throws Exception {
    _conf = new Configuration(externalConf);
    URL confOverlayUrl = Thread.currentThread().getContextClassLoader()
        .getResource(SHUFFLE_SERVICE_CONF_OVERLAY_RESOURCE_NAME);
    if (confOverlayUrl != null) {
      logger.info("Initializing Spark YARN shuffle service with configuration overlay from {}",
          confOverlayUrl);
      _conf.addResource(confOverlayUrl);
    }

    String logsNamespace = _conf.get(SPARK_SHUFFLE_SERVICE_LOGS_NAMESPACE_KEY, "");
    if (!logsNamespace.isEmpty()) {
      String className = YarnShuffleService.class.getName();
      logger = LoggerFactory.getLogger(className + "." + logsNamespace);
    }

    super.serviceInit(_conf);

    boolean stopOnFailure = _conf.getBoolean(STOP_ON_FAILURE_KEY, DEFAULT_STOP_ON_FAILURE);

    try {
      // In case this NM was killed while there were running spark applications, we need to restore
      // lost state for the existing executors. We look for an existing file in the NM's local dirs.
      // If we don't find one, then we choose a file to use to save the state next time.  Even if
      // an application was stopped while the NM was down, we expect yarn to call stopApplication()
      // when it comes back
      if (_recoveryPath != null) {
        registeredExecutorFile = initRecoveryDb(RECOVERY_FILE_NAME);
        mergeManagerFile = initRecoveryDb(SPARK_SHUFFLE_MERGE_RECOVERY_FILE_NAME);
      }

      TransportConf transportConf = new TransportConf("shuffle", new HadoopConfigProvider(_conf));
      // Create new MergedShuffleFileManager if shuffleMergeManager is null.
      // This is because in the unit test, a customized MergedShuffleFileManager will
      // be created through setShuffleFileManager method.
      if (shuffleMergeManager == null) {
        shuffleMergeManager = newMergedShuffleFileManagerInstance(transportConf, mergeManagerFile);
      }
      blockHandler = new ExternalBlockHandler(
        transportConf, registeredExecutorFile, shuffleMergeManager);

      // If authentication is enabled, set up the shuffle server to use a
      // special RPC handler that filters out unauthenticated fetch requests
      List<TransportServerBootstrap> bootstraps = Lists.newArrayList();
      boolean authEnabled = _conf.getBoolean(SPARK_AUTHENTICATE_KEY, DEFAULT_SPARK_AUTHENTICATE);
      if (authEnabled) {
        secretManager = new ShuffleSecretManager();
        if (_recoveryPath != null) {
          loadSecretsFromDb();
        }
        bootstraps.add(new AuthServerBootstrap(transportConf, secretManager));
      }

      int port = _conf.getInt(
        SPARK_SHUFFLE_SERVICE_PORT_KEY, DEFAULT_SPARK_SHUFFLE_SERVICE_PORT);
      transportContext = new TransportContext(transportConf, blockHandler, true);
      shuffleServer = transportContext.createServer(port, bootstraps);
      // the port should normally be fixed, but for tests its useful to find an open port
      port = shuffleServer.getPort();
      boundPort = port;
      String authEnabledString = authEnabled ? "enabled" : "not enabled";

      // register metrics on the block handler into the Node Manager's metrics system.
      blockHandler.getAllMetrics().getMetrics().put("numRegisteredConnections",
          shuffleServer.getRegisteredConnections());
      blockHandler.getAllMetrics().getMetrics().putAll(shuffleServer.getAllMetrics().getMetrics());
      String metricsNamespace = _conf.get(SPARK_SHUFFLE_SERVICE_METRICS_NAMESPACE_KEY,
          DEFAULT_SPARK_SHUFFLE_SERVICE_METRICS_NAME);
      YarnShuffleServiceMetrics serviceMetrics =
          new YarnShuffleServiceMetrics(metricsNamespace, blockHandler.getAllMetrics());

      MetricsSystemImpl metricsSystem = (MetricsSystemImpl) DefaultMetricsSystem.instance();
      metricsSystem.register(
          metricsNamespace, "Metrics on the Spark Shuffle Service", serviceMetrics);
      logger.info("Registered metrics with Hadoop's DefaultMetricsSystem using namespace '{}'",
          metricsNamespace);

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

  /**
   * Set the customized MergedShuffleFileManager for unit testing only
   * @param mergeManager
   */
  @VisibleForTesting
  void setShuffleMergeManager(MergedShuffleFileManager mergeManager) {
    this.shuffleMergeManager = mergeManager;
  }

  @VisibleForTesting
  static MergedShuffleFileManager newMergedShuffleFileManagerInstance(
      TransportConf conf, File mergeManagerFile) {
    String mergeManagerImplClassName = conf.mergedShuffleFileManagerImpl();
    try {
      Class<?> mergeManagerImplClazz = Class.forName(
        mergeManagerImplClassName, true, Thread.currentThread().getContextClassLoader());
      Class<? extends MergedShuffleFileManager> mergeManagerSubClazz =
        mergeManagerImplClazz.asSubclass(MergedShuffleFileManager.class);
      // The assumption is that all the custom implementations just like the RemoteBlockPushResolver
      // will also need the transport configuration.
      return mergeManagerSubClazz.getConstructor(TransportConf.class, File.class)
        .newInstance(conf, mergeManagerFile);
    } catch (Exception e) {
      defaultLogger.error("Unable to create an instance of {}", mergeManagerImplClassName);
      return new NoOpMergedShuffleFileManager(conf, mergeManagerFile);
    }
  }

  private void loadSecretsFromDb() throws IOException {
    secretsFile = initRecoveryDb(SECRETS_RECOVERY_FILE_NAME);

    // Make sure this is protected in case its not in the NM recovery dir
    FileSystem fs = FileSystem.getLocal(_conf);
    fs.mkdirs(new Path(secretsFile.getPath()), new FsPermission((short) 0700));

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
      if (transportContext != null) {
        transportContext.close();
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
   * Set the recovery path for shuffle service recovery when NM is restarted. This will be call
   * by NM if NM recovery is enabled.
   */
  @Override
  public void setRecoveryPath(Path recoveryPath) {
    _recoveryPath = recoveryPath;
  }

  /**
   * Get the path specific to this auxiliary service to use for recovery.
   */
  protected Path getRecoveryPath(String fileName) {
    return _recoveryPath;
  }

  /**
   * Figure out the recovery path and handle moving the DB if YARN NM recovery gets enabled
   * and DB exists in the local dir of NM by old version of shuffle service.
   */
  protected File initRecoveryDb(String dbName) {
    Preconditions.checkNotNull(_recoveryPath,
      "recovery path should not be null if NM recovery is enabled");

    File recoveryFile = new File(_recoveryPath.toUri().getPath(), dbName);
    if (recoveryFile.exists()) {
      return recoveryFile;
    }

    // db doesn't exist in recovery path go check local dirs for it
    String[] localDirs = _conf.getTrimmedStrings("yarn.nodemanager.local-dirs");
    for (String dir : localDirs) {
      File f = new File(new Path(dir).toUri().getPath(), dbName);
      if (f.exists()) {
        // If the recovery path is set then either NM recovery is enabled or another recovery
        // DB has been initialized. If NM recovery is enabled and had set the recovery path
        // make sure to move all DBs to the recovery path from the old NM local dirs.
        // If another DB was initialized first just make sure all the DBs are in the same
        // location.
        Path newLoc = new Path(_recoveryPath, dbName);
        Path copyFrom = new Path(f.toURI());
        if (!newLoc.equals(copyFrom)) {
          logger.info("Moving " + copyFrom + " to: " + newLoc);
          try {
            // The move here needs to handle moving non-empty directories across NFS mounts
            FileSystem fs = FileSystem.getLocal(_conf);
            fs.rename(copyFrom, newLoc);
          } catch (Exception e) {
            // Fail to move recovery file to new path, just continue on with new DB location
            logger.error("Failed to move recovery file {} to the path {}",
              dbName, _recoveryPath.toString(), e);
          }
        }
        return new File(newLoc.toUri().getPath());
      }
    }

    return new File(_recoveryPath.toUri().getPath(), dbName);
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
      return Objects.equals(appId, appExecId.appId);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(appId);
    }

    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
          .append("appId", appId)
          .toString();
    }
  }

}
