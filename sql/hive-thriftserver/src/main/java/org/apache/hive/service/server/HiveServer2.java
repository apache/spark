/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.service.server;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.common.LogUtils.LogInitializationException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManagerImpl;
import org.apache.hadoop.hive.ql.exec.tez.TezSessionPoolManager;
import org.apache.hadoop.hive.ql.util.ZooKeeperHiveHelper;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.hive.common.util.HiveVersionInfo;
import org.apache.hive.service.CompositeService;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.thrift.ThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIService;
import org.apache.hive.service.cli.thrift.ThriftHttpCLIService;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.data.ACL;

/**
 * HiveServer2.
 *
 */
public class HiveServer2 extends CompositeService {
  private static final Log LOG = LogFactory.getLog(HiveServer2.class);
  private static CountDownLatch deleteSignal;

  private CLIService cliService;
  private ThriftCLIService thriftCLIService;
  private PersistentEphemeralNode znode;
  private String znodePath;
  private CuratorFramework zooKeeperClient;
  private boolean registeredWithZooKeeper = false;

  public HiveServer2() {
    super(HiveServer2.class.getSimpleName());
    HiveConf.setLoadHiveServer2Config(true);
  }

  @Override
  public synchronized void init(HiveConf hiveConf) {
    cliService = new CLIService(this);
    addService(cliService);
    if (isHTTPTransportMode(hiveConf)) {
      thriftCLIService = new ThriftHttpCLIService(cliService);
    } else {
      thriftCLIService = new ThriftBinaryCLIService(cliService);
    }
    addService(thriftCLIService);
    super.init(hiveConf);

    // Add a shutdown hook for catching SIGTERM & SIGINT
    final HiveServer2 hiveServer2 = this;
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        hiveServer2.stop();
      }
    });
  }

  public static boolean isHTTPTransportMode(HiveConf hiveConf) {
    String transportMode = System.getenv("HIVE_SERVER2_TRANSPORT_MODE");
    if (transportMode == null) {
      transportMode = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE);
    }
    if (transportMode != null && (transportMode.equalsIgnoreCase("http"))) {
      return true;
    }
    return false;
  }

  /**
   * ACLProvider for providing appropriate ACLs to CuratorFrameworkFactory
   */
  private final ACLProvider zooKeeperAclProvider = new ACLProvider() {
    List<ACL> nodeAcls = new ArrayList<ACL>();

    @Override
    public List<ACL> getDefaultAcl() {
      if (UserGroupInformation.isSecurityEnabled()) {
        // Read all to the world
        nodeAcls.addAll(Ids.READ_ACL_UNSAFE);
        // Create/Delete/Write/Admin to the authenticated user
        nodeAcls.add(new ACL(Perms.ALL, Ids.AUTH_IDS));
      } else {
        // ACLs for znodes on a non-kerberized cluster
        // Create/Read/Delete/Write/Admin to the world
        nodeAcls.addAll(Ids.OPEN_ACL_UNSAFE);
      }
      return nodeAcls;
    }

    @Override
    public List<ACL> getAclForPath(String path) {
      return getDefaultAcl();
    }
  };

  /**
   * Adds a server instance to ZooKeeper as a znode.
   *
   * @param hiveConf
   * @throws Exception
   */
  private void addServerInstanceToZooKeeper(HiveConf hiveConf) throws Exception {
    String zooKeeperEnsemble = ZooKeeperHiveHelper.getQuorumServers(hiveConf);
    String rootNamespace = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_ZOOKEEPER_NAMESPACE);
    String instanceURI = getServerInstanceURI(hiveConf);
    byte[] znodeDataUTF8 = instanceURI.getBytes(Charset.forName("UTF-8"));
    setUpZooKeeperAuth(hiveConf);
    int sessionTimeout =
        (int) hiveConf.getTimeVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_SESSION_TIMEOUT,
            TimeUnit.MILLISECONDS);
    int baseSleepTime =
        (int) hiveConf.getTimeVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_CONNECTION_BASESLEEPTIME,
            TimeUnit.MILLISECONDS);
    int maxRetries = hiveConf.getIntVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_CONNECTION_MAX_RETRIES);
    // Create a CuratorFramework instance to be used as the ZooKeeper client
    // Use the zooKeeperAclProvider to create appropriate ACLs
    zooKeeperClient =
        CuratorFrameworkFactory.builder().connectString(zooKeeperEnsemble)
            .sessionTimeoutMs(sessionTimeout).aclProvider(zooKeeperAclProvider)
            .retryPolicy(new ExponentialBackoffRetry(baseSleepTime, maxRetries)).build();
    zooKeeperClient.start();
    // Create the parent znodes recursively; ignore if the parent already exists.
    try {
      zooKeeperClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
          .forPath(ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + rootNamespace);
      LOG.info("Created the root name space: " + rootNamespace + " on ZooKeeper for HiveServer2");
    } catch (KeeperException e) {
      if (e.code() != KeeperException.Code.NODEEXISTS) {
        LOG.fatal("Unable to create HiveServer2 namespace: " + rootNamespace + " on ZooKeeper", e);
        throw e;
      }
    }
    // Create a znode under the rootNamespace parent for this instance of the server
    // Znode name: serverUri=host:port;version=versionInfo;sequence=sequenceNumber
    try {
      String pathPrefix =
          ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + rootNamespace
              + ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + "serverUri=" + instanceURI + ";"
              + "version=" + HiveVersionInfo.getVersion() + ";" + "sequence=";
      znode =
          new PersistentEphemeralNode(zooKeeperClient,
              PersistentEphemeralNode.Mode.EPHEMERAL_SEQUENTIAL, pathPrefix, znodeDataUTF8);
      znode.start();
      // We'll wait for 120s for node creation
      long znodeCreationTimeout = 120;
      if (!znode.waitForInitialCreate(znodeCreationTimeout, TimeUnit.SECONDS)) {
        throw new Exception("Max znode creation wait time: " + znodeCreationTimeout + "s exhausted");
      }
      setRegisteredWithZooKeeper(true);
      znodePath = znode.getActualPath();
      // Set a watch on the znode
      if (zooKeeperClient.checkExists().usingWatcher(new DeRegisterWatcher()).forPath(znodePath) == null) {
        // No node exists, throw exception
        throw new Exception("Unable to create znode for this HiveServer2 instance on ZooKeeper.");
      }
      LOG.info("Created a znode on ZooKeeper for HiveServer2 uri: " + instanceURI);
    } catch (Exception e) {
      LOG.fatal("Unable to create a znode for this server instance", e);
      if (znode != null) {
        znode.close();
      }
      throw (e);
    }
  }

  /**
   * For a kerberized cluster, we dynamically set up the client's JAAS conf.
   *
   * @param hiveConf
   * @return
   * @throws Exception
   */
  private void setUpZooKeeperAuth(HiveConf hiveConf) throws Exception {
    if (UserGroupInformation.isSecurityEnabled()) {
      String principal = hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL);
      if (principal.isEmpty()) {
        throw new IOException("HiveServer2 Kerberos principal is empty");
      }
      String keyTabFile = hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB);
      if (keyTabFile.isEmpty()) {
        throw new IOException("HiveServer2 Kerberos keytab is empty");
      }
      // Install the JAAS Configuration for the runtime
      Utils.setZookeeperClientKerberosJaasConfig(principal, keyTabFile);
    }
  }

  /**
   * The watcher class which sets the de-register flag when the znode corresponding to this server
   * instance is deleted. Additionally, it shuts down the server if there are no more active client
   * sessions at the time of receiving a 'NodeDeleted' notification from ZooKeeper.
   */
  private class DeRegisterWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
      if (event.getType().equals(Watcher.Event.EventType.NodeDeleted)) {
        if (znode != null) {
          try {
            znode.close();
            LOG.warn("This HiveServer2 instance is now de-registered from ZooKeeper. "
                + "The server will be shut down after the last client sesssion completes.");
          } catch (IOException e) {
            LOG.error("Failed to close the persistent ephemeral znode", e);
          } finally {
            HiveServer2.this.setRegisteredWithZooKeeper(false);
            // If there are no more active client sessions, stop the server
            if (cliService.getSessionManager().getOpenSessionCount() == 0) {
              LOG.warn("This instance of HiveServer2 has been removed from the list of server "
                  + "instances available for dynamic service discovery. "
                  + "The last client session has ended - will shutdown now.");
              HiveServer2.this.stop();
            }
          }
        }
      }
    }
  }

  private void removeServerInstanceFromZooKeeper() throws Exception {
    setRegisteredWithZooKeeper(false);
    if (znode != null) {
      znode.close();
    }
    zooKeeperClient.close();
    LOG.info("Server instance removed from ZooKeeper.");
  }

  public boolean isRegisteredWithZooKeeper() {
    return registeredWithZooKeeper;
  }

  private void setRegisteredWithZooKeeper(boolean registeredWithZooKeeper) {
    this.registeredWithZooKeeper = registeredWithZooKeeper;
  }

  private String getServerInstanceURI(HiveConf hiveConf) throws Exception {
    if ((thriftCLIService == null) || (thriftCLIService.getServerIPAddress() == null)) {
      throw new Exception("Unable to get the server address; it hasn't been initialized yet.");
    }
    return thriftCLIService.getServerIPAddress().getHostName() + ":"
        + thriftCLIService.getPortNumber();
  }

  @Override
  public synchronized void start() {
    super.start();
  }

  @Override
  public synchronized void stop() {
    LOG.info("Shutting down HiveServer2");
    HiveConf hiveConf = this.getHiveConf();
    super.stop();
    // Remove this server instance from ZooKeeper if dynamic service discovery is set
    if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_SUPPORT_DYNAMIC_SERVICE_DISCOVERY)) {
      try {
        removeServerInstanceFromZooKeeper();
      } catch (Exception e) {
        LOG.error("Error removing znode for this HiveServer2 instance from ZooKeeper.", e);
      }
    }
    // There should already be an instance of the session pool manager.
    // If not, ignoring is fine while stopping HiveServer2.
    if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_TEZ_INITIALIZE_DEFAULT_SESSIONS)) {
      try {
        TezSessionPoolManager.getInstance().stop();
      } catch (Exception e) {
        LOG.error("Tez session pool manager stop had an error during stop of HiveServer2. "
            + "Shutting down HiveServer2 anyway.", e);
      }
    }

    if (hiveConf.getVar(ConfVars.HIVE_EXECUTION_ENGINE).equals("spark")) {
      try {
        SparkSessionManagerImpl.getInstance().shutdown();
      } catch(Exception ex) {
        LOG.error("Spark session pool manager failed to stop during HiveServer2 shutdown.", ex);
      }
    }
  }

  private static void startHiveServer2() throws Throwable {
    long attempts = 0, maxAttempts = 1;
    while (true) {
      LOG.info("Starting HiveServer2");
      HiveConf hiveConf = new HiveConf();
      maxAttempts = hiveConf.getLongVar(HiveConf.ConfVars.HIVE_SERVER2_MAX_START_ATTEMPTS);
      HiveServer2 server = null;
      try {
        server = new HiveServer2();
        server.init(hiveConf);
        server.start();
        ShimLoader.getHadoopShims().startPauseMonitor(hiveConf);
        // If we're supporting dynamic service discovery, we'll add the service uri for this
        // HiveServer2 instance to Zookeeper as a znode.
        if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_SUPPORT_DYNAMIC_SERVICE_DISCOVERY)) {
          server.addServerInstanceToZooKeeper(hiveConf);
        }
        if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_TEZ_INITIALIZE_DEFAULT_SESSIONS)) {
          TezSessionPoolManager sessionPool = TezSessionPoolManager.getInstance();
          sessionPool.setupPool(hiveConf);
          sessionPool.startPool();
        }

        if (hiveConf.getVar(ConfVars.HIVE_EXECUTION_ENGINE).equals("spark")) {
          SparkSessionManagerImpl.getInstance().setup(hiveConf);
        }
        break;
      } catch (Throwable throwable) {
        if (server != null) {
          try {
            server.stop();
          } catch (Throwable t) {
            LOG.info("Exception caught when calling stop of HiveServer2 before retrying start", t);
          } finally {
            server = null;
          }
        }
        if (++attempts >= maxAttempts) {
          throw new Error("Max start attempts " + maxAttempts + " exhausted", throwable);
        } else {
          LOG.warn("Error starting HiveServer2 on attempt " + attempts
              + ", will retry in 60 seconds", throwable);
          try {
            Thread.sleep(60L * 1000L);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }
    }
  }

  /**
   * Remove all znodes corresponding to the given version number from ZooKeeper
   *
   * @param versionNumber
   * @throws Exception
   */
  static void deleteServerInstancesFromZooKeeper(String versionNumber) throws Exception {
    HiveConf hiveConf = new HiveConf();
    String zooKeeperEnsemble = ZooKeeperHiveHelper.getQuorumServers(hiveConf);
    String rootNamespace = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_ZOOKEEPER_NAMESPACE);
    int baseSleepTime = (int) hiveConf.getTimeVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_CONNECTION_BASESLEEPTIME, TimeUnit.MILLISECONDS);
    int maxRetries = hiveConf.getIntVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_CONNECTION_MAX_RETRIES);
    CuratorFramework zooKeeperClient =
        CuratorFrameworkFactory.builder().connectString(zooKeeperEnsemble)
            .retryPolicy(new ExponentialBackoffRetry(baseSleepTime, maxRetries)).build();
    zooKeeperClient.start();
    List<String> znodePaths =
        zooKeeperClient.getChildren().forPath(
            ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + rootNamespace);
    List<String> znodePathsUpdated;
    // Now for each path that is for the given versionNumber, delete the znode from ZooKeeper
    for (int i = 0; i < znodePaths.size(); i++) {
      String znodePath = znodePaths.get(i);
      deleteSignal = new CountDownLatch(1);
      if (znodePath.contains("version=" + versionNumber + ";")) {
        String fullZnodePath =
            ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + rootNamespace
                + ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + znodePath;
        LOG.warn("Will attempt to remove the znode: " + fullZnodePath + " from ZooKeeper");
        System.out.println("Will attempt to remove the znode: " + fullZnodePath + " from ZooKeeper");
        zooKeeperClient.delete().guaranteed().inBackground(new DeleteCallBack())
            .forPath(fullZnodePath);
        // Wait for the delete to complete
        deleteSignal.await();
        // Get the updated path list
        znodePathsUpdated =
            zooKeeperClient.getChildren().forPath(
                ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + rootNamespace);
        // Gives a list of any new paths that may have been created to maintain the persistent ephemeral node
        znodePathsUpdated.removeAll(znodePaths);
        // Add the new paths to the znodes list. We'll try for their removal as well.
        znodePaths.addAll(znodePathsUpdated);
      }
    }
    zooKeeperClient.close();
  }

  private static class DeleteCallBack implements BackgroundCallback {
    @Override
    public void processResult(CuratorFramework zooKeeperClient, CuratorEvent event)
        throws Exception {
      if (event.getType() == CuratorEventType.DELETE) {
        deleteSignal.countDown();
      }
    }
  }

  public static void main(String[] args) {
    HiveConf.setLoadHiveServer2Config(true);
    try {
      ServerOptionsProcessor oproc = new ServerOptionsProcessor("hiveserver2");
      ServerOptionsProcessorResponse oprocResponse = oproc.parse(args);

      // NOTE: It is critical to do this here so that log4j is reinitialized
      // before any of the other core hive classes are loaded
      String initLog4jMessage = LogUtils.initHiveLog4j();
      LOG.debug(initLog4jMessage);
      HiveStringUtils.startupShutdownMessage(HiveServer2.class, args, LOG);

      // Log debug message from "oproc" after log4j initialize properly
      LOG.debug(oproc.getDebugMessage().toString());

      // Call the executor which will execute the appropriate command based on the parsed options
      oprocResponse.getServerOptionsExecutor().execute();
    } catch (LogInitializationException e) {
      LOG.error("Error initializing log: " + e.getMessage(), e);
      System.exit(-1);
    }
  }

  /**
   * ServerOptionsProcessor.
   * Process arguments given to HiveServer2 (-hiveconf property=value)
   * Set properties in System properties
   * Create an appropriate response object,
   * which has executor to execute the appropriate command based on the parsed options.
   */
  static class ServerOptionsProcessor {
    private final Options options = new Options();
    private org.apache.commons.cli.CommandLine commandLine;
    private final String serverName;
    private final StringBuilder debugMessage = new StringBuilder();

    @SuppressWarnings("static-access")
    ServerOptionsProcessor(String serverName) {
      this.serverName = serverName;
      // -hiveconf x=y
      options.addOption(OptionBuilder
          .withValueSeparator()
          .hasArgs(2)
          .withArgName("property=value")
          .withLongOpt("hiveconf")
          .withDescription("Use value for given property")
          .create());
      // -deregister <versionNumber>
      options.addOption(OptionBuilder
          .hasArgs(1)
          .withArgName("versionNumber")
          .withLongOpt("deregister")
          .withDescription("Deregister all instances of given version from dynamic service discovery")
          .create());
      options.addOption(new Option("H", "help", false, "Print help information"));
    }

    ServerOptionsProcessorResponse parse(String[] argv) {
      try {
        commandLine = new GnuParser().parse(options, argv);
        // Process --hiveconf
        // Get hiveconf param values and set the System property values
        Properties confProps = commandLine.getOptionProperties("hiveconf");
        for (String propKey : confProps.stringPropertyNames()) {
          // save logging message for log4j output latter after log4j initialize properly
          debugMessage.append("Setting " + propKey + "=" + confProps.getProperty(propKey) + ";\n");
          System.setProperty(propKey, confProps.getProperty(propKey));
        }

        // Process --help
        if (commandLine.hasOption('H')) {
          return new ServerOptionsProcessorResponse(new HelpOptionExecutor(serverName, options));
        }

        // Process --deregister
        if (commandLine.hasOption("deregister")) {
          return new ServerOptionsProcessorResponse(new DeregisterOptionExecutor(
              commandLine.getOptionValue("deregister")));
        }
      } catch (ParseException e) {
        // Error out & exit - we were not able to parse the args successfully
        System.err.println("Error starting HiveServer2 with given arguments: ");
        System.err.println(e.getMessage());
        System.exit(-1);
      }
      // Default executor, when no option is specified
      return new ServerOptionsProcessorResponse(new StartOptionExecutor());
    }

    StringBuilder getDebugMessage() {
      return debugMessage;
    }
  }

  /**
   * The response sent back from {@link ServerOptionsProcessor#parse(String[])}
   */
  static class ServerOptionsProcessorResponse {
    private final ServerOptionsExecutor serverOptionsExecutor;

    ServerOptionsProcessorResponse(ServerOptionsExecutor serverOptionsExecutor) {
      this.serverOptionsExecutor = serverOptionsExecutor;
    }

    ServerOptionsExecutor getServerOptionsExecutor() {
      return serverOptionsExecutor;
    }
  }

  /**
   * The executor interface for running the appropriate HiveServer2 command based on parsed options
   */
  static interface ServerOptionsExecutor {
    public void execute();
  }

  /**
   * HelpOptionExecutor: executes the --help option by printing out the usage
   */
  static class HelpOptionExecutor implements ServerOptionsExecutor {
    private final Options options;
    private final String serverName;

    HelpOptionExecutor(String serverName, Options options) {
      this.options = options;
      this.serverName = serverName;
    }

    @Override
    public void execute() {
      new HelpFormatter().printHelp(serverName, options);
      System.exit(0);
    }
  }

  /**
   * StartOptionExecutor: starts HiveServer2.
   * This is the default executor, when no option is specified.
   */
  static class StartOptionExecutor implements ServerOptionsExecutor {
    @Override
    public void execute() {
      try {
        startHiveServer2();
      } catch (Throwable t) {
        LOG.fatal("Error starting HiveServer2", t);
        System.exit(-1);
      }
    }
  }

  /**
   * DeregisterOptionExecutor: executes the --deregister option by deregistering all HiveServer2
   * instances from ZooKeeper of a specific version.
   */
  static class DeregisterOptionExecutor implements ServerOptionsExecutor {
    private final String versionNumber;

    DeregisterOptionExecutor(String versionNumber) {
      this.versionNumber = versionNumber;
    }

    @Override
    public void execute() {
      try {
        deleteServerInstancesFromZooKeeper(versionNumber);
      } catch (Exception e) {
        LOG.fatal("Error deregistering HiveServer2 instances for version: " + versionNumber
            + " from ZooKeeper", e);
        System.out.println("Error deregistering HiveServer2 instances for version: " + versionNumber
            + " from ZooKeeper." + e);
        System.exit(-1);
      }
      System.exit(0);
    }
  }
}
