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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.security.SecureRandom;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.ObjectName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.UnregisteredDatanodeException;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager.AccessMode;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.VolumeInfo;
import org.apache.hadoop.hdfs.server.datanode.SecureDataNodeStarter.SecureResources;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FileChecksumServlets;
import org.apache.hadoop.hdfs.server.namenode.JspHelper;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.StreamFile;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockMetaDataInfo;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DisallowedDatanodeException;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.KeyUpdateCommand;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.PluginDispatcher;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ServicePlugin;
import org.apache.hadoop.util.SingleArgumentRunnable;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.util.VersionInfo;
import org.mortbay.util.ajax.JSON;

/**********************************************************
 * DataNode is a class (and program) that stores a set of
 * blocks for a DFS deployment.  A single deployment can
 * have one or many DataNodes.  Each DataNode communicates
 * regularly with a single NameNode.  It also communicates
 * with client code and other DataNodes from time to time.
 *
 * DataNodes store a series of named blocks.  The DataNode
 * allows client code to read these blocks, or to write new
 * block data.  The DataNode may also, in response to instructions
 * from its NameNode, delete blocks or copy blocks to/from other
 * DataNodes.
 *
 * The DataNode maintains just one critical table:
 *   block-> stream of bytes (of BLOCK_SIZE or less)
 *
 * This info is stored on a local disk.  The DataNode
 * reports the table's contents to the NameNode upon startup
 * and every so often afterwards.
 *
 * DataNodes spend their lives in an endless loop of asking
 * the NameNode for something to do.  A NameNode cannot connect
 * to a DataNode directly; a NameNode simply returns values from
 * functions invoked by a DataNode.
 *
 * DataNodes maintain an open server socket so that client code 
 * or other DataNodes can read/write data.  The host/port for
 * this server is reported to the NameNode, which then sends that
 * information to clients or other DataNodes that might be interested.
 *
 **********************************************************/
public class DataNode extends Configured 
    implements InterDatanodeProtocol, ClientDatanodeProtocol, FSConstants, 
    Runnable, DataNodeMXBean {
  public static final Log LOG = LogFactory.getLog(DataNode.class);
  
  static{
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
  }

  public static final String DN_CLIENTTRACE_FORMAT =
        "src: %s" +      // src IP
        ", dest: %s" +   // dst IP
        ", bytes: %s" +  // byte count
        ", op: %s" +     // operation
        ", cliID: %s" +  // DFSClient id
        ", offset: %s" + // offset
        ", srvID: %s" +  // DatanodeRegistration
        ", blockid: %s" + // block id
        ", duration: %s"; // duration time

  static final Log ClientTraceLog =
    LogFactory.getLog(DataNode.class.getName() + ".clienttrace");

  /**
   * Use {@link NetUtils#createSocketAddr(String)} instead.
   */
  @Deprecated
  public static InetSocketAddress createSocketAddr(String target
                                                   ) throws IOException {
    return NetUtils.createSocketAddr(target);
  }
  
  public DatanodeProtocol namenode = null;
  public FSDatasetInterface data = null;
  public DatanodeRegistration dnRegistration = null;

  volatile boolean shouldRun = true;
  private LinkedList<Block> receivedBlockList = new LinkedList<Block>();
  /** list of blocks being recovered */
  private final Map<Block, Block> ongoingRecovery = new HashMap<Block, Block>();
  private LinkedList<String> delHints = new LinkedList<String>();
  public final static String EMPTY_DEL_HINT = "";
  AtomicInteger xmitsInProgress = new AtomicInteger();
  Daemon dataXceiverServer = null;
  ThreadGroup threadGroup = null;
  long blockReportInterval;
  //disallow the sending of BR before instructed to do so
  long lastBlockReport = 0;
  boolean resetBlockReportTime = true;
  long initialBlockReportDelay = BLOCKREPORT_INITIAL_DELAY * 1000L;
  long lastHeartbeat = 0;
  long heartBeatInterval;
  private DataStorage storage = null;
  private HttpServer infoServer = null;
  DataNodeMetrics myMetrics;
  private static InetSocketAddress nameNodeAddr;
  private InetSocketAddress selfAddr;
  private static DataNode datanodeObject = null;
  private Thread dataNodeThread = null;
  String machineName;
  private static String dnThreadName;
  int socketTimeout;
  int socketWriteTimeout = 0;  
  boolean transferToAllowed = true;
  private boolean dropCacheBehindWrites = false;
  private boolean syncBehindWrites = false;
  private boolean dropCacheBehindReads = false;
  private long readaheadLength = 0;

  int writePacketSize = 0;
  private boolean connectToDnViaHostname;
  private boolean relaxedVersionCheck;
  
  /**
   * Testing hook that allows tests to delay the sending of blockReceived
   * RPCs to the namenode. This can help find bugs in append.
   */
  int artificialBlockReceivedDelay = 0;

  boolean isBlockTokenEnabled;
  BlockTokenSecretManager blockTokenSecretManager;
  boolean isBlockTokenInitialized = false;
  final String userWithLocalPathAccess;

  public DataBlockScanner blockScanner = null;
  public Daemon blockScannerThread = null;
  
  /** Activated plug-ins. */
  private PluginDispatcher<DatanodePlugin> pluginDispatcher;
  
  private static final Random R = new Random();
  
  public static final String DATA_DIR_KEY = "dfs.data.dir";
  public final static String DATA_DIR_PERMISSION_KEY = 
    "dfs.datanode.data.dir.perm";
  private static final String DEFAULT_DATA_DIR_PERMISSION = "700";
  
  // Thresholds for when we start to log when a block report is
  // taking a long time to generate. Under heavy disk load and
  // memory pressure, it's normal for block reports to take
  // several minutes, since they cause many disk seeks.
  private static final long LATE_BLOCK_REPORT_WARN_THRESHOLD =
      10 * 60 * 1000; // 10m
  private static final long LATE_BLOCK_REPORT_INFO_THRESHOLD =
      3 * 60 * 1000; // 3m

  // For InterDataNodeProtocol
  public Server ipcServer;

  private SecureResources secureResources = null;
  
  /**
   * Current system time.
   * @return current time in msec.
   */
  static long now() {
    return System.currentTimeMillis();
  }
  
  /**
   * Create the DataNode given a configuration and an array of dataDirs.
   * 'dataDirs' is where the blocks are stored.
   */
  DataNode(final Configuration conf, 
           final AbstractList<File> dataDirs) throws IOException {
    this(conf, dataDirs, null);
  }
  
  /**
   * Start a Datanode with specified server sockets for secure environments
   * where they are run with privileged ports and injected from a higher
   * level of capability
   */
  DataNode(final Configuration conf,
           final AbstractList<File> dataDirs, SecureResources resources) throws IOException {
    super(conf);
    UserGroupInformation.setConfiguration(conf);
    SecurityUtil.login(conf, DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY, 
        DFSConfigKeys.DFS_DATANODE_USER_NAME_KEY);

    datanodeObject = this;
    userWithLocalPathAccess = conf.get(
        DFSConfigKeys.DFS_BLOCK_LOCAL_PATH_ACCESS_USER_KEY);

    try {
      startDataNode(conf, dataDirs, resources);
    } catch (IOException ie) {
      shutdown();
      throw ie;
    }   
  }
    
  
  /**
   * This method starts the data node with the specified conf.
   * 
   * @param conf - the configuration
   *  if conf's CONFIG_PROPERTY_SIMULATED property is set
   *  then a simulated storage based data node is created.
   * 
   * @param dataDirs - only for a non-simulated storage data node
   * @throws IOException
   * @throws MalformedObjectNameException 
   * @throws MBeanRegistrationException 
   * @throws InstanceAlreadyExistsException 
   */
  void startDataNode(Configuration conf, 
                     AbstractList<File> dataDirs, SecureResources resources
                     ) throws IOException {
    if(UserGroupInformation.isSecurityEnabled() && resources == null &&
       conf.getBoolean("dfs.datanode.require.secure.ports", true))
      throw new RuntimeException(
        "Cannot start secure cluster without privileged resources. " +
        "In a secure cluster, the DataNode must be started from within " +
        "jsvc. If using Cloudera packages, please install the " +
        "hadoop-0.20-sbin package.\n\n" +
        "For development purposes ONLY you may override this check by setting" +
        " dfs.datanode.require.secure.ports to false. *** THIS WILL OPEN A " +
        "SECURITY HOLE AND MUST NOT BE USED FOR A REAL CLUSTER ***.");
    
    this.secureResources = resources;
    // use configured nameserver & interface to get local hostname
    if (conf.get("slave.host.name") != null) {
      machineName = conf.get("slave.host.name");   
    }
    if (machineName == null) {
      machineName = DNS.getDefaultHost(
                                     conf.get("dfs.datanode.dns.interface","default"),
                                     conf.get("dfs.datanode.dns.nameserver","default"));
    }
    InetSocketAddress nameNodeAddr = NameNode.getServiceAddress(conf, true);
    
    this.socketTimeout =  conf.getInt("dfs.socket.timeout",
                                      HdfsConstants.READ_TIMEOUT);
    this.socketWriteTimeout = conf.getInt("dfs.datanode.socket.write.timeout",
                                          HdfsConstants.WRITE_TIMEOUT);
    /* Based on results on different platforms, we might need set the default 
     * to false on some of them. */
    this.transferToAllowed = conf.getBoolean("dfs.datanode.transferTo.allowed", 
                                             true);
    this.writePacketSize = conf.getInt("dfs.write.packet.size", 64*1024);
    this.readaheadLength = conf.getLong(
        DFSConfigKeys.DFS_DATANODE_READAHEAD_BYTES_KEY,
        DFSConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
    this.dropCacheBehindWrites = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_DROP_CACHE_BEHIND_WRITES_KEY,
        DFSConfigKeys.DFS_DATANODE_DROP_CACHE_BEHIND_WRITES_DEFAULT);
    this.syncBehindWrites = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_SYNC_BEHIND_WRITES_KEY,
        DFSConfigKeys.DFS_DATANODE_SYNC_BEHIND_WRITES_DEFAULT);
    this.dropCacheBehindReads = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_DROP_CACHE_BEHIND_READS_KEY,
        DFSConfigKeys.DFS_DATANODE_DROP_CACHE_BEHIND_READS_DEFAULT);

    this.relaxedVersionCheck = conf.getBoolean(
        CommonConfigurationKeys.HADOOP_RELAXED_VERSION_CHECK_KEY,
        CommonConfigurationKeys.HADOOP_RELAXED_VERSION_CHECK_DEFAULT);

    InetSocketAddress socAddr = DataNode.getStreamingAddr(conf);
    int tmpPort = socAddr.getPort();
    storage = new DataStorage();
    // construct registration
    this.dnRegistration = new DatanodeRegistration(machineName + ":" + tmpPort);

    // connect to name node
    this.namenode = (DatanodeProtocol) 
      RPC.waitForProxy(DatanodeProtocol.class,
                       DatanodeProtocol.versionID,
                       nameNodeAddr, 
                       conf);
    // get version and id info from the name-node
    NamespaceInfo nsInfo = handshake();
    StartupOption startOpt = getStartupOption(conf);
    assert startOpt != null : "Startup option must be set.";
    
    boolean simulatedFSDataset = 
        conf.getBoolean("dfs.datanode.simulateddatastorage", false);
    if (simulatedFSDataset) {
        setNewStorageID(dnRegistration);
        dnRegistration.storageInfo.layoutVersion = FSConstants.LAYOUT_VERSION;
        dnRegistration.storageInfo.namespaceID = nsInfo.namespaceID;
        // it would have been better to pass storage as a parameter to
        // constructor below - need to augment ReflectionUtils used below.
        conf.set("StorageId", dnRegistration.getStorageID());
        try {
          //Equivalent of following (can't do because Simulated is in test dir)
          //  this.data = new SimulatedFSDataset(conf);
          this.data = (FSDatasetInterface) ReflectionUtils.newInstance(
              Class.forName("org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset"), conf);
        } catch (ClassNotFoundException e) {
          throw new IOException(StringUtils.stringifyException(e));
        }
    } else { // real storage
      // read storage info, lock data dirs and transition fs state if necessary
      storage.recoverTransitionRead(nsInfo, dataDirs, startOpt);
      // adjust
      this.dnRegistration.setStorageInfo(storage);
      // initialize data node internal structure
      this.data = new FSDataset(storage, conf);
    }

    // Allow configuration to delay block reports to find bugs
    artificialBlockReceivedDelay = conf.getInt(
      "dfs.datanode.artificialBlockReceivedDelay", 0);

    // register datanode MXBean
    this.registerMXBean(conf); // register the MXBean for DataNode

    // find free port or use privileged port provide
    ServerSocket ss;
    if(secureResources == null) {
      ss = (socketWriteTimeout > 0) ? 
        ServerSocketChannel.open().socket() : new ServerSocket();
      Server.bind(ss, socAddr, 0);
    } else {
      ss = resources.getStreamingSocket();
    }
    ss.setReceiveBufferSize(DEFAULT_DATA_SOCKET_SIZE); 
    // adjust machine name with the actual port
    tmpPort = ss.getLocalPort();
    selfAddr = new InetSocketAddress(ss.getInetAddress().getHostAddress(),
                                     tmpPort);
    this.dnRegistration.setName(machineName + ":" + tmpPort);
    LOG.info("Opened streaming server at " + tmpPort);
      
    this.threadGroup = new ThreadGroup("dataXceiverServer");
    this.dataXceiverServer = new Daemon(threadGroup, 
        new DataXceiverServer(ss, conf, this));
    this.threadGroup.setDaemon(true); // auto destroy when empty

    this.blockReportInterval =
      conf.getLong("dfs.blockreport.intervalMsec", BLOCKREPORT_INTERVAL);
    this.initialBlockReportDelay = conf.getLong("dfs.blockreport.initialDelay",
                                            BLOCKREPORT_INITIAL_DELAY)* 1000L; 
    if (this.initialBlockReportDelay >= blockReportInterval) {
      this.initialBlockReportDelay = 0;
      LOG.info("dfs.blockreport.initialDelay is greater than " +
        "dfs.blockreport.intervalMsec." + " Setting initial delay to 0 msec:");
    }
    this.heartBeatInterval = conf.getLong("dfs.heartbeat.interval", HEARTBEAT_INTERVAL) * 1000L;
    DataNode.nameNodeAddr = nameNodeAddr;

    //initialize periodic block scanner
    String reason = null;
    if (conf.getInt("dfs.datanode.scan.period.hours", 0) < 0) {
      reason = "verification is turned off by configuration";
    } else if ( !(data instanceof FSDataset) ) {
      reason = "verifcation is supported only with FSDataset";
    } 
    if ( reason == null ) {
      blockScanner = new DataBlockScanner(this, (FSDataset)data, conf);
    } else {
      LOG.info("Periodic Block Verification is disabled because " +
               reason + ".");
    }

    this.connectToDnViaHostname = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME,
        DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME_DEFAULT);
    LOG.debug("Connect to datanode via hostname is " + connectToDnViaHostname);

    //create a servlet to serve full-file content
    InetSocketAddress infoSocAddr = DataNode.getInfoAddr(conf);
    String infoHost = infoSocAddr.getHostName();
    int tmpInfoPort = infoSocAddr.getPort();
    this.infoServer = (secureResources == null) 
       ? new HttpServer("datanode", infoHost, tmpInfoPort, tmpInfoPort == 0, 
           conf, SecurityUtil.getAdminAcls(conf, DFSConfigKeys.DFS_ADMIN))
       : new HttpServer("datanode", infoHost, tmpInfoPort, tmpInfoPort == 0,
           conf, SecurityUtil.getAdminAcls(conf, DFSConfigKeys.DFS_ADMIN),
           secureResources.getListener());
    if (conf.getBoolean("dfs.https.enable", false)) {
      boolean needClientAuth = conf.getBoolean("dfs.https.need.client.auth", false);
      InetSocketAddress secInfoSocAddr = NetUtils.createSocketAddr(conf.get(
          "dfs.datanode.https.address", infoHost + ":" + 0));
      Configuration sslConf = new Configuration(false);
      sslConf.addResource(conf.get("dfs.https.server.keystore.resource",
          "ssl-server.xml"));
      this.infoServer.addSslListener(secInfoSocAddr, sslConf, needClientAuth);
    }
    this.infoServer.addInternalServlet(null, "/streamFile/*", StreamFile.class);
    this.infoServer.addInternalServlet(null, "/getFileChecksum/*",
        FileChecksumServlets.GetServlet.class);
    this.infoServer.setAttribute("datanode.blockScanner", blockScanner);
    this.infoServer.setAttribute(JspHelper.CURRENT_CONF, conf);
    this.infoServer.addServlet(null, "/blockScannerReport", 
                               DataBlockScanner.Servlet.class);
    this.infoServer.start();
    // adjust info port
    this.dnRegistration.setInfoPort(this.infoServer.getPort());
    myMetrics = new DataNodeMetrics(conf, dnRegistration.getName());

    // BlockTokenSecretManager is created here, but it shouldn't be
    // used until it is initialized in register().
    this.blockTokenSecretManager = new BlockTokenSecretManager(false,
        0, 0);
    //init ipc server
    InetSocketAddress ipcAddr = NetUtils.createSocketAddr(
        conf.get("dfs.datanode.ipc.address"));
    ipcServer = RPC.getServer(this, ipcAddr.getHostName(), ipcAddr.getPort(), 
        conf.getInt("dfs.datanode.handler.count", 3), false, conf,
        blockTokenSecretManager);

    // set service-level authorization security policy
    if (conf.getBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
      ipcServer.refreshServiceAcl(conf, new HDFSPolicyProvider());
    }

    dnRegistration.setIpcPort(ipcServer.getListenerAddress().getPort());

    LOG.info("dnRegistration = " + dnRegistration);
    
    pluginDispatcher = PluginDispatcher.createFromConfiguration(
        conf, DFSConfigKeys.DFS_DATANODE_PLUGINS_KEY, DatanodePlugin.class);
    pluginDispatcher.dispatchStart(this);
  }

  private ObjectName mxBean = null;
  /**
   * Register the DataNode MXBean using the name
   *        "hadoop:service=DataNode,name=DataNodeInfo"
   */
  void registerMXBean(Configuration conf) {
    // We wrap to bypass standard mbean naming convention.
    // This wraping can be removed in java 6 as it is more flexible in 
    // package naming for mbeans and their impl.
    mxBean = MBeanUtil.registerMBean("DataNode", "DataNodeInfo", this);
  }
  
  public void unRegisterMXBean() {
    if (mxBean != null)
      MBeanUtil.unregisterMBean(mxBean);
  }

  /**
   * Determine the http server's effective addr
   */
  public static InetSocketAddress getInfoAddr(Configuration conf) {
    String infoAddr = NetUtils.getServerAddress(conf, 
        "dfs.datanode.info.bindAddress", 
        "dfs.datanode.info.port",
        "dfs.datanode.http.address");
    
    return NetUtils.createSocketAddr(infoAddr); 
  }

  /**
   * Creates either NIO or regular depending on socketWriteTimeout.
   */
  protected Socket newSocket() throws IOException {
    return (socketWriteTimeout > 0) ? 
           SocketChannel.open().socket() : new Socket();                                   
  }
  
  /**
   * @return true if this datanode is permitted to connect to
   *    the given namenode version
   */
  boolean isPermittedVersion(NamespaceInfo nsInfo) {
    boolean versionMatch =
      nsInfo.getVersion().equals(VersionInfo.getVersion());
    boolean revisionMatch =
      nsInfo.getRevision().equals(VersionInfo.getRevision());

    if (revisionMatch && !versionMatch) {
      throw new AssertionError("Invalid build. The revisions match" +
          " but the NN version is " + nsInfo.getVersion() +
          " and the DN version is " + VersionInfo.getVersion());
    }
    if (relaxedVersionCheck) {
      if (versionMatch && !revisionMatch) {
        LOG.info("Permitting datanode revision " + VersionInfo.getRevision() +
            " to connect to namenode revision " + nsInfo.getRevision() +
            " because " + CommonConfigurationKeys.HADOOP_RELAXED_VERSION_CHECK_KEY +
            " is enabled");
      }
      return versionMatch;
    } else {
      return revisionMatch;
    }
  }

  private NamespaceInfo handshake() throws IOException {
    NamespaceInfo nsInfo = new NamespaceInfo();
    while (shouldRun) {
      try {
        nsInfo = namenode.versionRequest();
        break;
      } catch(SocketTimeoutException e) {  // namenode is busy
        LOG.info("Problem connecting to server: " + getNameNodeAddr());
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {}
      }
    }
    if (!isPermittedVersion(nsInfo)) {
      String errorMsg = "Incompatible versions: namenode version " +
        nsInfo.getVersion() + " revision " + nsInfo.getRevision() +
        " datanode version " + VersionInfo.getVersion() + " revision " +
        VersionInfo.getRevision() + " and " +
        CommonConfigurationKeys.HADOOP_RELAXED_VERSION_CHECK_KEY +
        " is " + (relaxedVersionCheck ? "enabled" : "not enabled");
      LOG.fatal(errorMsg);
      notifyNamenode(DatanodeProtocol.NOTIFY, errorMsg);
      throw new IOException(errorMsg);
    }
    assert FSConstants.LAYOUT_VERSION == nsInfo.getLayoutVersion() :
      "Data-node and name-node layout versions must be the same."
      + "Expected: "+ FSConstants.LAYOUT_VERSION + " actual "+ nsInfo.getLayoutVersion();
    return nsInfo;
  }

  /** Return the DataNode object
   * 
   */
  public static DataNode getDataNode() {
    return datanodeObject;
  } 

  public static InterDatanodeProtocol createInterDataNodeProtocolProxy(
      DatanodeInfo info, final Configuration conf, final int socketTimeout,
      boolean connectToDnViaHostname) throws IOException {
    final String dnName = info.getNameWithIpcPort(connectToDnViaHostname);
    final InetSocketAddress addr = NetUtils.createSocketAddr(dnName);
    if (InterDatanodeProtocol.LOG.isDebugEnabled()) {
      InterDatanodeProtocol.LOG.info("InterDatanodeProtocol addr=" + addr);
    }

    UserGroupInformation loginUgi = UserGroupInformation.getLoginUser();
    try {
      return loginUgi
          .doAs(new PrivilegedExceptionAction<InterDatanodeProtocol>() {
            public InterDatanodeProtocol run() throws IOException {
              return (InterDatanodeProtocol) RPC.getProxy(
                  InterDatanodeProtocol.class, InterDatanodeProtocol.versionID,
                  addr, UserGroupInformation.getCurrentUser(), conf,
                  NetUtils.getDefaultSocketFactory(conf), socketTimeout);
            }
          });
    } catch (InterruptedException ie) {
      throw new IOException(ie.getMessage());
    }
  }

  public InetSocketAddress getNameNodeAddr() {
    return nameNodeAddr;
  }
  
  public InetSocketAddress getSelfAddr() {
    return selfAddr;
  }
    
  DataNodeMetrics getMetrics() {
    return myMetrics;
  }
  
  /**
   * Return the namenode's identifier
   */
  public String getNamenode() {
    //return namenode.toString();
    return "<namenode>";
  }

  public static void setNewStorageID(DatanodeRegistration dnReg) {
    /* Return 
     * "DS-randInt-ipaddr-currentTimeMillis"
     * It is considered extermely rare for all these numbers to match
     * on a different machine accidentally for the following 
     * a) SecureRandom(INT_MAX) is pretty much random (1 in 2 billion), and
     * b) Good chance ip address would be different, and
     * c) Even on the same machine, Datanode is designed to use different ports.
     * d) Good chance that these are started at different times.
     * For a confict to occur all the 4 above have to match!.
     * The format of this string can be changed anytime in future without
     * affecting its functionality.
     */
    String ip = "unknownIP";
    try {
      ip = DNS.getDefaultIP("default");
    } catch (UnknownHostException ignored) {
      LOG.warn("Could not find ip address of \"default\" inteface.");
    }
    
    int rand = 0;
    try {
      rand = SecureRandom.getInstance("SHA1PRNG").nextInt(Integer.MAX_VALUE);
    } catch (NoSuchAlgorithmException e) {
      LOG.warn("Could not use SecureRandom");
      rand = R.nextInt(Integer.MAX_VALUE);
    }
    dnReg.storageID = "DS-" + rand + "-"+ ip + "-" + dnReg.getPort() + "-" + 
                      System.currentTimeMillis();
  }
  /**
   * Register datanode
   * <p>
   * The datanode needs to register with the namenode on startup in order
   * 1) to report which storage it is serving now and 
   * 2) to receive a registrationID 
   * issued by the namenode to recognize registered datanodes.
   * 
   * @see FSNamesystem#registerDatanode(DatanodeRegistration)
   * @throws IOException
   */
  private void register() throws IOException {
    if (dnRegistration.getStorageID().equals("")) {
      setNewStorageID(dnRegistration);
    }
    while(shouldRun) {
      try {
        // reset name to machineName. Mainly for web interface.
        dnRegistration.name = machineName + ":" + dnRegistration.getPort();
        dnRegistration = namenode.register(dnRegistration);
        break;
      } catch(SocketTimeoutException e) {  // namenode is busy
        LOG.info("Problem connecting to server: " + getNameNodeAddr());
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {}
      }
    }

    assert ("".equals(storage.getStorageID()) 
            && !"".equals(dnRegistration.getStorageID()))
            || storage.getStorageID().equals(dnRegistration.getStorageID()) :
            "New storageID can be assigned only if data-node is not formatted";
    if (storage.getStorageID().equals("")) {
      storage.setStorageID(dnRegistration.getStorageID());
      storage.writeAll();
      LOG.info("New storage id " + dnRegistration.getStorageID()
          + " is assigned to data-node " + dnRegistration.getName());
    }
    if(! storage.getStorageID().equals(dnRegistration.getStorageID())) {
      throw new IOException("Inconsistent storage IDs. Name-node returned "
          + dnRegistration.getStorageID() 
          + ". Expecting " + storage.getStorageID());
    }
    
    if (!isBlockTokenInitialized) {
      /* first time registering with NN */
      ExportedBlockKeys keys = dnRegistration.exportedKeys;
      this.isBlockTokenEnabled = keys.isBlockTokenEnabled();
      if (isBlockTokenEnabled) {
        long blockKeyUpdateInterval = keys.getKeyUpdateInterval();
        long blockTokenLifetime = keys.getTokenLifetime();
        LOG.info("Block token params received from NN: keyUpdateInterval="
            + blockKeyUpdateInterval / (60 * 1000) + " min(s), tokenLifetime="
            + blockTokenLifetime / (60 * 1000) + " min(s)");
        blockTokenSecretManager.setTokenLifetime(blockTokenLifetime);
      }
      isBlockTokenInitialized = true;
    }

    if (isBlockTokenEnabled) {
      blockTokenSecretManager.setKeys(dnRegistration.exportedKeys);
      dnRegistration.exportedKeys = ExportedBlockKeys.DUMMY_KEYS;
    }


    Block[] bbwReport = data.getBlocksBeingWrittenReport();
    long[] blocksBeingWritten =
      BlockListAsLongs.convertToArrayLongs(bbwReport);
    namenode.blocksBeingWrittenReport(dnRegistration, blocksBeingWritten);

    // random short delay - helps scatter the BR from all DNs
    // - but we can start generating the block report immediately
    data.requestAsyncBlockReport();
    scheduleBlockReport(initialBlockReportDelay);
  }

  /**
   * Shut down this instance of the datanode.
   * Returns only after shutdown is complete.
   * This method can only be called by the offerService thread.
   * Otherwise, deadlock might occur.
   */
  public void shutdown() {
    this.unRegisterMXBean();

    if (pluginDispatcher != null) {
      pluginDispatcher.dispatchStop();
    }

    if (infoServer != null) {
      try {
        infoServer.stop();
      } catch (Exception e) {
        LOG.warn("Exception shutting down DataNode", e);
      }
    }
    if (ipcServer != null) {
      ipcServer.stop();
    }
    this.shouldRun = false;
    if (dataXceiverServer != null) {
      ((DataXceiverServer) this.dataXceiverServer.getRunnable()).kill();
      this.dataXceiverServer.interrupt();

      // wait for all data receiver threads to exit
      if (this.threadGroup != null) {
        while (true) {
          this.threadGroup.interrupt();
          LOG.info("Waiting for threadgroup to exit, active threads is " +
                   this.threadGroup.activeCount());
          if (this.threadGroup.activeCount() == 0) {
            break;
          }
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {}
        }
      }
      // wait for dataXceiveServer to terminate
      try {
        this.dataXceiverServer.join();
      } catch (InterruptedException ie) {
      }
    }
    
    RPC.stopProxy(namenode); // stop the RPC threads
    
    if(upgradeManager != null)
      upgradeManager.shutdownUpgrade();
    if (blockScannerThread != null) { 
      blockScannerThread.interrupt();
      try {
        blockScannerThread.join(3600000L); // wait for at most 1 hour
      } catch (InterruptedException ie) {
      }
    }
    if (storage != null) {
      try {
        this.storage.unlockAll();
      } catch (IOException ie) {
      }
    }
    if (dataNodeThread != null) {
      dataNodeThread.interrupt();
      try {
        dataNodeThread.join();
      } catch (InterruptedException ie) {
      }
    }
    if (data != null) {
      data.shutdown();
    }
    if (myMetrics != null) {
      myMetrics.shutdown();
    }
  }
  
  
  /** Check if there is no space in disk 
   *  @param e that caused this checkDiskError call
   **/
  protected void checkDiskError(Exception e ) throws IOException {
    
    LOG.warn("checkDiskError: exception: ", e);
    
    if (e.getMessage() != null &&
        e.getMessage().startsWith("No space left on device")) {
      throw new DiskOutOfSpaceException("No space left on device");
    } else {
      checkDiskError();
    }
  }
  
  /**
   *  Check if there is a disk failure and if so, handle the error
   *
   **/
  protected void checkDiskError( ) {
    try {
      data.checkDataDir();
    } catch (DiskErrorException de) {
      handleDiskError(de.getMessage());
    }
  }
  
  private void handleDiskError(String errMsgr) {
    final boolean hasEnoughResources = data.hasEnoughResources();
    LOG.warn("DataNode.handleDiskError: Keep Running: " + hasEnoughResources);
          
    // If we have enough active valid volumes then we do not want to 
    // shutdown the DN completely.
    int dpError = hasEnoughResources ? DatanodeProtocol.DISK_ERROR  
                                     : DatanodeProtocol.FATAL_DISK_ERROR; 
    myMetrics.volumeFailures.inc(1);
    notifyNamenode(dpError, errMsgr);
    
    if (hasEnoughResources) {
      scheduleBlockReport(0);
      return; // do not shutdown
    }
    
    LOG.warn("DataNode is shutting down.\n" + errMsgr);
    shouldRun = false; 
  }
    
  /** Number of concurrent xceivers per node. */
  public int getXceiverCount() {
    return threadGroup == null ? 0 : threadGroup.activeCount();
  }
    
  /**
   * Main loop for the DataNode.  Runs until shutdown,
   * forever calling remote NameNode functions.
   */
  public void offerService() throws Exception {
     
    LOG.info("using BLOCKREPORT_INTERVAL of " + blockReportInterval + "msec" + 
       " Initial delay: " + initialBlockReportDelay + "msec");

    //
    // Now loop for a long time....
    //

    while (shouldRun) {
      try {
        long startTime = now();

        //
        // Every so often, send heartbeat or block-report
        //
        
        if (startTime - lastHeartbeat > heartBeatInterval) {
          //
          // All heartbeat messages include following info:
          // -- Datanode name
          // -- data transfer port
          // -- Total capacity
          // -- Bytes remaining
          //
          lastHeartbeat = startTime;
          DatanodeCommand[] cmds = namenode.sendHeartbeat(dnRegistration,
                                                       data.getCapacity(),
                                                       data.getDfsUsed(),
                                                       data.getRemaining(),
                                                       xmitsInProgress.get(),
                                                       getXceiverCount(),
                                                       data.getNumFailedVolumes());
          myMetrics.heartbeats.inc(now() - startTime);
          //LOG.info("Just sent heartbeat, with name " + localName);
          if (!processCommand(cmds))
            continue;
        }
            
        // check if there are newly received blocks
        Block [] blockArray=null;
        String [] delHintArray=null;
        synchronized(receivedBlockList) {
          synchronized(delHints) {
            int numBlocks = receivedBlockList.size();
            if (numBlocks > 0) {
              if(numBlocks!=delHints.size()) {
                LOG.warn("Panic: receiveBlockList and delHints are not of the same length" );
              }
              //
              // Send newly-received blockids to namenode
              //
              blockArray = receivedBlockList.toArray(new Block[numBlocks]);
              delHintArray = delHints.toArray(new String[numBlocks]);
            }
          }
        }
        if (blockArray != null) {
          if(delHintArray == null || delHintArray.length != blockArray.length ) {
            LOG.warn("Panic: block array & delHintArray are not the same" );
          }
          namenode.blockReceived(dnRegistration, blockArray, delHintArray);
          synchronized (receivedBlockList) {
            synchronized (delHints) {
              for(int i=0; i<blockArray.length; i++) {
                receivedBlockList.remove(blockArray[i]);
                delHints.remove(delHintArray[i]);
              }
            }
          }
        }

        // send block report
        if (startTime - lastBlockReport > blockReportInterval) {
          if (data.isAsyncBlockReportReady()) {
            //
            // Send latest blockinfo report if timer has expired.
            // Get back a list of local block(s) that are obsolete
            // and can be safely GC'ed.
            //
            long brStartTime = now();
            Block[] bReport = data.retrieveAsyncBlockReport();
            DatanodeCommand cmd = namenode.blockReport(dnRegistration,
                    BlockListAsLongs.convertToArrayLongs(bReport));
            long brTime = now() - brStartTime;
            myMetrics.blockReports.inc(brTime);
            LOG.info("BlockReport of " + bReport.length +
                " blocks got processed in " + brTime + " msecs");
            //
            // If we have sent the first block report, then wait a random
            // time before we start the periodic block reports.
            //
            if (resetBlockReportTime) {
              lastBlockReport = startTime - R.nextInt((int)(blockReportInterval));
              resetBlockReportTime = false;
            } else {
              /* say the last block report was at 8:20:14. The current report 
               * should have started around 9:20:14 (default 1 hour interval). 
               * If current time is :
               *   1) normal like 9:20:18, next report should be at 10:20:14
               *   2) unexpected like 11:35:43, next report should be at
               *      12:20:14
               */
              lastBlockReport += (now() - lastBlockReport) / 
                                 blockReportInterval * blockReportInterval;
            }
            processCommand(cmd);
          } else {
            data.requestAsyncBlockReport();
            if (lastBlockReport > 0) { // this isn't the first report
              long waitingFor =
                  startTime - lastBlockReport - blockReportInterval;
              String msg = "Block report is due, and been waiting for it for " +
                  (waitingFor/1000) + " seconds...";
              if (waitingFor > LATE_BLOCK_REPORT_WARN_THRESHOLD) {
                LOG.warn(msg);
              } else if (waitingFor > LATE_BLOCK_REPORT_INFO_THRESHOLD) {
                LOG.info(msg);
              } else if (LOG.isDebugEnabled()) {
                LOG.debug(msg);
              }
            }
          }
        }

        // start block scanner
        if (blockScanner != null && blockScannerThread == null &&
            upgradeManager.isUpgradeCompleted()) {
          LOG.info("Starting Periodic block scanner.");
          blockScannerThread = new Daemon(blockScanner);
          blockScannerThread.start();
        }
            
        //
        // There is no work to do;  sleep until hearbeat timer elapses, 
        // or work arrives, and then iterate again.
        //
        long waitTime = heartBeatInterval - (System.currentTimeMillis() - lastHeartbeat);
        synchronized(receivedBlockList) {
          if (waitTime > 0 && receivedBlockList.size() == 0) {
            try {
              receivedBlockList.wait(waitTime);
            } catch (InterruptedException ie) {
            }
            delayBeforeBlockReceived();
          }
        } // synchronized
      } catch(RemoteException re) {
        String reClass = re.getClassName();
        if (UnregisteredDatanodeException.class.getName().equals(reClass) ||
            DisallowedDatanodeException.class.getName().equals(reClass) ||
            IncorrectVersionException.class.getName().equals(reClass)) {
          LOG.warn("DataNode is shutting down: " + 
                   StringUtils.stringifyException(re));
          shutdown();
          return;
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {}
        LOG.warn(StringUtils.stringifyException(re));
      } catch (IOException e) {
        LOG.warn(StringUtils.stringifyException(e));
      }
    } // while (shouldRun)
  } // offerService

  /**
   * When a block has been received, we can delay some period of time before
   * reporting it to the DN, for the purpose of testing. This simulates
   * the actual latency of blockReceived on a real network (where the client
   * may be closer to the NN than the DNs).
   */
  private void delayBeforeBlockReceived() {
    if (artificialBlockReceivedDelay > 0 && !receivedBlockList.isEmpty()) {
      try {
        long sleepFor = (long)R.nextInt(artificialBlockReceivedDelay);
        LOG.debug("DataNode " + dnRegistration + " sleeping for " +
                  "artificial delay: " + sleepFor + " ms");
        Thread.sleep(sleepFor);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Process an array of datanode commands
   * 
   * @param cmds an array of datanode commands
   * @return true if further processing may be required or false otherwise. 
   */
  private boolean processCommand(DatanodeCommand[] cmds) {
    if (cmds != null) {
      for (DatanodeCommand cmd : cmds) {
        try {
          if (processCommand(cmd) == false) {
            return false;
          }
        } catch (IOException ioe) {
          LOG.warn("Error processing datanode Command", ioe);
        }
      }
    }
    return true;
  }
  
    /**
     * 
     * @param cmd
     * @return true if further processing may be required or false otherwise. 
     * @throws IOException
     */
  private boolean processCommand(DatanodeCommand cmd) throws IOException {
    if (cmd == null)
      return true;
    final BlockCommand bcmd = cmd instanceof BlockCommand? (BlockCommand)cmd: null;

    switch(cmd.getAction()) {
    case DatanodeProtocol.DNA_TRANSFER:
      // Send a copy of a block to another datanode
      transferBlocks(bcmd.getBlocks(), bcmd.getTargets());
      myMetrics.blocksReplicated.inc(bcmd.getBlocks().length);
      break;
    case DatanodeProtocol.DNA_INVALIDATE:
      //
      // Some local block(s) are obsolete and can be 
      // safely garbage-collected.
      //
      Block toDelete[] = bcmd.getBlocks();
      try {
        if (blockScanner != null) {
          blockScanner.deleteBlocks(toDelete);
        }
        data.invalidate(toDelete);
      } catch(IOException e) {
        checkDiskError();
        throw e;
      }
      myMetrics.blocksRemoved.inc(toDelete.length);
      break;
    case DatanodeProtocol.DNA_SHUTDOWN:
      // shut down the data node
      this.shutdown();
      return false;
    case DatanodeProtocol.DNA_REGISTER:
      // namenode requested a registration - at start or if NN lost contact
      LOG.info("DatanodeCommand action: DNA_REGISTER");
      if (shouldRun) {
        register();
        pluginDispatcher.dispatchCall(
          new SingleArgumentRunnable<DatanodePlugin>() {
            public void run(DatanodePlugin p) { p.reregistrationComplete(); }
          });
      }
      break;
    case DatanodeProtocol.DNA_FINALIZE:
      storage.finalizeUpgrade();
      break;
    case UpgradeCommand.UC_ACTION_START_UPGRADE:
      // start distributed upgrade here
      processDistributedUpgradeCommand((UpgradeCommand)cmd);
      break;
    case DatanodeProtocol.DNA_RECOVERBLOCK:
      recoverBlocks(bcmd.getBlocks(), bcmd.getTargets());
      break;
    case DatanodeProtocol.DNA_ACCESSKEYUPDATE:
      LOG.info("DatanodeCommand action: DNA_ACCESSKEYUPDATE");
      if (isBlockTokenEnabled) {
        blockTokenSecretManager.setKeys(((KeyUpdateCommand) cmd).getExportedKeys());
      }
      break;
    default:
      LOG.warn("Unknown DatanodeCommand action: " + cmd.getAction());
    }
    return true;
  }

  // Distributed upgrade manager
  UpgradeManagerDatanode upgradeManager = new UpgradeManagerDatanode(this);

  private void processDistributedUpgradeCommand(UpgradeCommand comm
                                               ) throws IOException {
    assert upgradeManager != null : "DataNode.upgradeManager is null.";
    upgradeManager.processUpgradeCommand(comm);
  }


  /**
   * Start distributed upgrade if it should be initiated by the data-node.
   */
  private void startDistributedUpgradeIfNeeded() throws IOException {
    UpgradeManagerDatanode um = DataNode.getDataNode().upgradeManager;
    assert um != null : "DataNode.upgradeManager is null.";
    if(!um.getUpgradeState())
      return;
    um.setUpgradeState(false, um.getUpgradeVersion());
    um.startUpgrade();
    return;
  }

  private void notifyNamenode(int dpCode, String msg) {
    try {
      namenode.errorReport(dnRegistration, dpCode, msg);
    } catch (SocketTimeoutException e) {
      LOG.warn("Problem connecting to " + getNameNodeAddr());
    } catch (IOException ignored) {
    }
  }

  private void transferBlock( Block block, 
                              DatanodeInfo xferTargets[] 
                              ) throws IOException {
    if (!data.isValidBlock(block)) {
      // block does not exist or is under-construction
      String errStr = "Can't send invalid block " + block;
      LOG.info(errStr);
      notifyNamenode(DatanodeProtocol.INVALID_BLOCK, errStr);
      return;
    }

    // Check if NN recorded length matches on-disk length 
    long onDiskLength = data.getLength(block);
    if (block.getNumBytes() > onDiskLength) {
      // Shorter on-disk len indicates corruption so report NN the corrupt block
      namenode.reportBadBlocks(new LocatedBlock[]{
          new LocatedBlock(block, new DatanodeInfo[] {
              new DatanodeInfo(dnRegistration)})});
      LOG.info("Can't replicate block " + block
          + " because on-disk length " + onDiskLength 
          + " is shorter than NameNode recorded length " + block.getNumBytes());
      return;
    }
    
    int numTargets = xferTargets.length;
    if (numTargets > 0) {
      if (LOG.isInfoEnabled()) {
        StringBuilder xfersBuilder = new StringBuilder();
        for (int i = 0; i < numTargets; i++) {
          xfersBuilder.append(xferTargets[i].getName());
          xfersBuilder.append(" ");
        }
        LOG.info(dnRegistration + " Starting thread to transfer block " + 
                 block + " to " + xfersBuilder);                       
      }

      new Daemon(new DataTransfer(xferTargets, block, this)).start();
    }
  }

  private void transferBlocks( Block blocks[], 
                               DatanodeInfo xferTargets[][] 
                               ) {
    for (int i = 0; i < blocks.length; i++) {
      try {
        transferBlock(blocks[i], xferTargets[i]);
      } catch (IOException ie) {
        LOG.warn("Failed to transfer block " + blocks[i], ie);
      }
    }
  }

  /*
   * Informing the name node could take a long long time! Should we wait
   * till namenode is informed before responding with success to the
   * client? For now we don't.
   */
  protected void notifyNamenodeReceivedBlock(Block block, String delHint) {
    if(block==null || delHint==null) {
      throw new IllegalArgumentException(block==null?"Block is null":"delHint is null");
    }
    synchronized (receivedBlockList) {
      synchronized (delHints) {
        receivedBlockList.add(block);
        delHints.add(delHint);
        receivedBlockList.notifyAll();
      }
    }
  }

  


  /* ********************************************************************
  Protocol when a client reads data from Datanode (Cur Ver: 9):
  
  Client's Request :
  =================
   
     Processed in DataXceiver:
     +----------------------------------------------+
     | Common Header   | 1 byte OP == OP_READ_BLOCK |
     +----------------------------------------------+
     
     Processed in readBlock() :
     +-------------------------------------------------------------------------+
     | 8 byte Block ID | 8 byte genstamp | 8 byte start offset | 8 byte length |
     +-------------------------------------------------------------------------+
     |   vInt length   |  <DFSClient id> |
     +-----------------------------------+
     
     Client sends optional response only at the end of receiving data.
       
  DataNode Response :
  ===================
   
    In readBlock() :
    If there is an error while initializing BlockSender :
       +---------------------------+
       | 2 byte OP_STATUS_ERROR    | and connection will be closed.
       +---------------------------+
    Otherwise
       +---------------------------+
       | 2 byte OP_STATUS_SUCCESS  |
       +---------------------------+
       
    Actual data, sent by BlockSender.sendBlock() :
    
      ChecksumHeader :
      +--------------------------------------------------+
      | 1 byte CHECKSUM_TYPE | 4 byte BYTES_PER_CHECKSUM |
      +--------------------------------------------------+
      Followed by actual data in the form of PACKETS: 
      +------------------------------------+
      | Sequence of data PACKETs ....      |
      +------------------------------------+
    
    A "PACKET" is defined further below.
    
    The client reads data until it receives a packet with 
    "LastPacketInBlock" set to true or with a zero length. It then replies
    to DataNode with one of the status codes:
    - CHECKSUM_OK:    All the chunk checksums have been verified
    - SUCCESS:        Data received; checksums not verified
    - ERROR_CHECKSUM: (Currently not used) Detected invalid checksums

      +---------------+
      | 2 byte Status |
      +---------------+
    
    The DataNode always expects the 2 byte status code. It will close the
    client connection if it is absent.
    
    PACKET : Contains a packet header, checksum and data. Amount of data
    ======== carried is set by BUFFER_SIZE.
    
      +-----------------------------------------------------+
      | 4 byte packet length (excluding packet header)      |
      +-----------------------------------------------------+
      | 8 byte offset in the block | 8 byte sequence number |
      +-----------------------------------------------------+
      | 1 byte isLastPacketInBlock                          |
      +-----------------------------------------------------+
      | 4 byte Length of actual data                        |
      +-----------------------------------------------------+
      | x byte checksum data. x is defined below            |
      +-----------------------------------------------------+
      | actual data ......                                  |
      +-----------------------------------------------------+
      
      x = (length of data + BYTE_PER_CHECKSUM - 1)/BYTES_PER_CHECKSUM *
          CHECKSUM_SIZE
          
      CHECKSUM_SIZE depends on CHECKSUM_TYPE (usually, 4 for CRC32)
      
      The above packet format is used while writing data to DFS also.
      Not all the fields might be used while reading.
    
   ************************************************************************ */
  
  /** Header size for a packet */
  public static final int PKT_HEADER_LEN = ( 4 + /* Packet payload length */
                                      8 + /* offset in block */
                                      8 + /* seqno */
                                      1   /* isLastPacketInBlock */);
  


  /**
   * Used for transferring a block of data.  This class
   * sends a piece of data to another DataNode.
   */
  class DataTransfer implements Runnable {
    DatanodeInfo targets[];
    Block b;
    DataNode datanode;

    /**
     * Connect to the first item in the target list.  Pass along the 
     * entire target list, the block, and the data.
     */
    public DataTransfer(DatanodeInfo targets[], Block b, DataNode datanode) throws IOException {
      this.targets = targets;
      this.b = b;
      this.datanode = datanode;
    }

    /**
     * Do the deed, write the bytes
     */
    public void run() {
      xmitsInProgress.getAndIncrement();
      Socket sock = null;
      DataOutputStream out = null;
      BlockSender blockSender = null;
      
      try {
        final String dnName = targets[0].getName(connectToDnViaHostname);
        InetSocketAddress curTarget = NetUtils.createSocketAddr(dnName);
        sock = newSocket();
        LOG.debug("Connecting to " + dnName);
        NetUtils.connect(sock, curTarget, socketTimeout);
        sock.setSoTimeout(targets.length * socketTimeout);

        long writeTimeout = socketWriteTimeout + 
                            HdfsConstants.WRITE_TIMEOUT_EXTENSION * (targets.length-1);
        OutputStream baseStream = NetUtils.getOutputStream(sock, writeTimeout);
        out = new DataOutputStream(new BufferedOutputStream(baseStream, 
                                                            SMALL_BUFFER_SIZE));

        blockSender = new BlockSender(b, 0, b.getNumBytes(), false, false, false, 
            datanode);
        DatanodeInfo srcNode = new DatanodeInfo(dnRegistration);

        //
        // Header info
        //
        out.writeShort(DataTransferProtocol.DATA_TRANSFER_VERSION);
        out.writeByte(DataTransferProtocol.OP_WRITE_BLOCK);
        out.writeLong(b.getBlockId());
        out.writeLong(b.getGenerationStamp());
        out.writeInt(0);           // no pipelining
        out.writeBoolean(false);   // not part of recovery
        Text.writeString(out, ""); // client
        out.writeBoolean(true); // sending src node information
        srcNode.write(out); // Write src node DatanodeInfo
        // write targets
        out.writeInt(targets.length - 1);
        for (int i = 1; i < targets.length; i++) {
          targets[i].write(out);
        }
        Token<BlockTokenIdentifier> accessToken = BlockTokenSecretManager.DUMMY_TOKEN;
        if (isBlockTokenEnabled) {
          accessToken = blockTokenSecretManager.generateToken(null, b,
              EnumSet.of(BlockTokenSecretManager.AccessMode.WRITE));
        }
        accessToken.write(out);
        // send data & checksum
        blockSender.sendBlock(out, baseStream, null);

        // no response necessary
        LOG.info(dnRegistration + ":Transmitted block " + b + " to " + curTarget);

      } catch (IOException ie) {
        LOG.warn(dnRegistration + ":Failed to transfer " + b + " to " + targets[0].getName()
            + " got " + StringUtils.stringifyException(ie));
        // check if there are any disk problem
        datanode.checkDiskError();
        
      } finally {
        xmitsInProgress.getAndDecrement();
        IOUtils.closeStream(blockSender);
        IOUtils.closeStream(out);
        IOUtils.closeSocket(sock);
      }
    }
  }

  /**
   * No matter what kind of exception we get, keep retrying to offerService().
   * That's the loop that connects to the NameNode and provides basic DataNode
   * functionality.
   *
   * Only stop when "shouldRun" is turned off (which can only happen at shutdown).
   */
  public void run() {
    LOG.info(dnRegistration + "In DataNode.run, data = " + data);

    // start dataXceiveServer
    dataXceiverServer.start();
    ipcServer.start();
        
    while (shouldRun) {
      try {
        startDistributedUpgradeIfNeeded();
        offerService();
      } catch (Exception ex) {
        LOG.error("Exception: " + StringUtils.stringifyException(ex));
        if (shouldRun) {
          try {
            Thread.sleep(5000);
          } catch (InterruptedException ie) {
          }
        }
      }
    }
        
    LOG.info(dnRegistration + ":Finishing DataNode in: "+data);
    shutdown();
  }
    
  /** Start a single datanode daemon and wait for it to finish.
   *  If this thread is specifically interrupted, it will stop waiting.
   */
  public static void runDatanodeDaemon(DataNode dn) throws IOException {
    if (dn != null) {
      //register datanode
      dn.register();
      dn.pluginDispatcher.dispatchCall(
        new SingleArgumentRunnable<DatanodePlugin>() {
          public void run(DatanodePlugin p) { p.initialRegistrationComplete(); }
        });
      dn.dataNodeThread = new Thread(dn, dnThreadName);
      dn.dataNodeThread.setDaemon(true); // needed for JUnit testing
      dn.dataNodeThread.start();
    }
  }
  
  static boolean isDatanodeUp(DataNode dn) {
    return dn.isDatanodeUp();
  }

  public boolean isDatanodeUp() {
    return dataNodeThread != null && dataNodeThread.isAlive();
  }

  /** Instantiate a single datanode object. This must be run by invoking
   *  {@link DataNode#runDatanodeDaemon(DataNode)} subsequently. 
   */
  public static DataNode instantiateDataNode(String args[],
                                      Configuration conf) throws IOException {
    return instantiateDataNode(args, conf, null);
  }
  
  /** Instantiate a single datanode object. This must be run by invoking
   *  {@link DataNode#runDatanodeDaemon(DataNode)} subsequently. 
   * @param resources Secure resources needed to run under Kerberos
   */
  public static DataNode instantiateDataNode(String args[],
                                      Configuration conf, 
                                      SecureResources resources) throws IOException {
    if (conf == null)
      conf = new Configuration();
    if (!parseArguments(args, conf)) {
      printUsage();
      return null;
    }
    if (conf.get("dfs.network.script") != null) {
      LOG.error("This configuration for rack identification is not supported" +
          " anymore. RackID resolution is handled by the NameNode.");
      System.exit(-1);
    }
    String[] dataDirs = conf.getTrimmedStrings(DATA_DIR_KEY);
    dnThreadName = "DataNode: [" +
                        StringUtils.arrayToString(dataDirs) + "]";
    return makeInstance(dataDirs, conf, resources);
  }

  /** Instantiate & Start a single datanode daemon and wait for it to finish.
   *  If this thread is specifically interrupted, it will stop waiting.
   */
  public static DataNode createDataNode(String args[],
                                 Configuration conf) throws IOException {
    return createDataNode(args, conf, null);
  }
  
  
  /** Instantiate & Start a single datanode daemon and wait for it to finish.
   *  If this thread is specifically interrupted, it will stop waiting.
   *  LimitedPrivate for creating secure datanodes
   */
  public static DataNode createDataNode(String args[],
            Configuration conf, SecureResources resources) throws IOException {
    DataNode dn = instantiateDataNode(args, conf, resources);
    runDatanodeDaemon(dn);
    return dn;
  }

  void join() {
    if (dataNodeThread != null) {
      try {
        dataNodeThread.join();
      } catch (InterruptedException e) {}
    }
  }

  /**
   * Make an instance of DataNode after ensuring that at least one of the
   * given data directories (and their parent directories, if necessary)
   * can be created.
   * @param dataDirs List of directories, where the new DataNode instance should
   * keep its files.
   * @param conf Configuration instance to use.
   * @param resources Secure resources needed to run under Kerberos
   * @return DataNode instance for given list of data dirs and conf, or null if
   * no directory from this directory list can be created.
   * @throws IOException
   */
  public static DataNode makeInstance(String[] dataDirs, Configuration conf, 
      SecureResources resources) throws IOException {
    LocalFileSystem localFS = FileSystem.getLocal(conf);
    ArrayList<File> dirs = new ArrayList<File>();
    FsPermission dataDirPermission = 
      new FsPermission(conf.get(DATA_DIR_PERMISSION_KEY, 
                                DEFAULT_DATA_DIR_PERMISSION));
    for (String dir : dataDirs) {
      try {
        DiskChecker.checkDir(localFS, new Path(dir), dataDirPermission);
        dirs.add(new File(dir));
      } catch (IOException ioe) {
        LOG.warn("Invalid "+ DATA_DIR_KEY +" directory " + dir +  ": "
            + ioe.getMessage());
      }
    }
    if (dirs.size() > 0) {
      return new DataNode(conf, dirs, resources);
    }
    LOG.error("All directories in " + DATA_DIR_KEY + " are invalid.");
    return null;
  }

  @Override
  public String toString() {
    return "DataNode{" +
      "data=" + data +
      ", localName='" + dnRegistration.getName() + "'" +
      ", storageID='" + dnRegistration.getStorageID() + "'" +
      ", xmitsInProgress=" + xmitsInProgress.get() +
      "}";
  }
  
  private static void printUsage() {
    System.err.println("Usage: java DataNode");
    System.err.println("           [-rollback]");
  }

  /**
   * Parse and verify command line arguments and set configuration parameters.
   *
   * @return false if passed argements are incorrect
   */
  private static boolean parseArguments(String args[], 
                                        Configuration conf) {
    int argsLen = (args == null) ? 0 : args.length;
    StartupOption startOpt = StartupOption.REGULAR;
    for(int i=0; i < argsLen; i++) {
      String cmd = args[i];
      if ("-r".equalsIgnoreCase(cmd) || "--rack".equalsIgnoreCase(cmd)) {
        LOG.error("-r, --rack arguments are not supported anymore. RackID " +
            "resolution is handled by the NameNode.");
        System.exit(-1);
      } else if ("-rollback".equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.ROLLBACK;
      } else if ("-regular".equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.REGULAR;
      } else
        return false;
    }
    setStartupOption(conf, startOpt);
    return true;
  }

  private static void setStartupOption(Configuration conf, StartupOption opt) {
    conf.set("dfs.datanode.startup", opt.toString());
  }

  static StartupOption getStartupOption(Configuration conf) {
    return StartupOption.valueOf(conf.get("dfs.datanode.startup",
                                          StartupOption.REGULAR.toString()));
  }

  /**
   * This methods  arranges for the data node to send the block report at the next heartbeat.
   */
  public void scheduleBlockReport(long delay) {
    if (delay > 0) { // send BR after random delay
      lastBlockReport = System.currentTimeMillis()
                            - ( blockReportInterval - R.nextInt((int)(delay)));
    } else { // send at next heartbeat
      lastBlockReport = lastHeartbeat - blockReportInterval;
    }
    resetBlockReportTime = true; // reset future BRs for randomness
  }
  
  
  /**
   * This method is used for testing. 
   * Examples are adding and deleting blocks directly.
   * The most common usage will be when the data node's storage is similated.
   * 
   * @return the fsdataset that stores the blocks
   */
  public FSDatasetInterface getFSDataset() {
    return data;
  }

  public static void secureMain(String [] args, SecureResources resources) {
    try {
      StringUtils.startupShutdownMessage(DataNode.class, args, LOG);
      DataNode datanode = createDataNode(args, null, resources);
      if (datanode != null)
        datanode.join();
    } catch (Throwable e) {
      LOG.error(StringUtils.stringifyException(e));
      System.exit(-1);
    } finally {
      // We need to add System.exit here because either shutdown was called or
      // some disk related conditions like volumes tolerated or volumes required
      // condition was not met. Also, In secure mode, control will go to Jsvc an
      // the process hangs without System.exit.
      LOG.info("Exiting Datanode");
      System.exit(0);
    }
  }
  
  public static void main(String args[]) {
    secureMain(args, null);
  }

  // InterDataNodeProtocol implementation
  /** {@inheritDoc} */
  public BlockMetaDataInfo getBlockMetaDataInfo(Block block
      ) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("block=" + block);
    }
    
    Block stored = data.getStoredBlock(block.getBlockId());

    if (stored == null) {
      return null;
    }
    BlockMetaDataInfo info = new BlockMetaDataInfo(stored,
                                 blockScanner.getLastScanTime(stored));
    if (LOG.isDebugEnabled()) {
      LOG.debug("getBlockMetaDataInfo successful block=" + stored +
                " length " + stored.getNumBytes() +
                " genstamp " + stored.getGenerationStamp());
    }

    // paranoia! verify that the contents of the stored block
    // matches the block file on disk.
    data.validateBlockMetadata(stored);
    return info;
  }
  
  @Override
  public BlockRecoveryInfo startBlockRecovery(Block block) throws IOException {
    return data.startBlockRecovery(block.getBlockId());
  }

  public Daemon recoverBlocks(final Block[] blocks, final DatanodeInfo[][] targets) {
    Daemon d = new Daemon(threadGroup, new Runnable() {
      /** Recover a list of blocks. It is run by the primary datanode. */
      public void run() {
        for(int i = 0; i < blocks.length; i++) {
          try {
            logRecoverBlock("NameNode", blocks[i], targets[i]);
            recoverBlock(blocks[i], false, targets[i], true);
          } catch (IOException e) {
            LOG.warn("recoverBlocks FAILED, blocks[" + i + "]=" + blocks[i], e);
          }
        }
      }
    });
    d.start();
    return d;
  }

  /** {@inheritDoc} */
  public void updateBlock(Block oldblock, Block newblock, boolean finalize) throws IOException {
    LOG.info("oldblock=" + oldblock + "(length=" + oldblock.getNumBytes()
        + "), newblock=" + newblock + "(length=" + newblock.getNumBytes()
        + "), datanode=" + dnRegistration.getName());
    data.updateBlock(oldblock, newblock);
    if (finalize) {
      data.finalizeBlockIfNeeded(newblock);
      myMetrics.blocksWritten.inc(); 
      notifyNamenodeReceivedBlock(newblock, EMPTY_DEL_HINT);
      LOG.info("Received block " + newblock +
                " of size " + newblock.getNumBytes() +
                " as part of lease recovery.");
    }
  }

  /** {@inheritDoc} */
  public long getProtocolVersion(String protocol, long clientVersion
      ) throws IOException {
    if (protocol.equals(InterDatanodeProtocol.class.getName())) {
      return InterDatanodeProtocol.versionID; 
    } else if (protocol.equals(ClientDatanodeProtocol.class.getName())) {
      return ClientDatanodeProtocol.versionID; 
    }
    throw new IOException("Unknown protocol to " + getClass().getSimpleName()
        + ": " + protocol);
  }
  
  /** Ensure the authentication method is kerberos */
  private void checkKerberosAuthMethod(String msg) throws IOException {
    // User invoking the call must be same as the datanode user
    if (!UserGroupInformation.isSecurityEnabled()) {
      return;
    }
    if (UserGroupInformation.getCurrentUser().getAuthenticationMethod() != 
        AuthenticationMethod.KERBEROS) {
      throw new AccessControlException("Error in "+msg+". Only "
          + "kerberos based authentication is allowed.");
    }
  }
  
  private void checkBlockLocalPathAccess() throws IOException {
    checkKerberosAuthMethod("getBlockLocalPathInfo()");
    String currentUser = UserGroupInformation.getCurrentUser().getShortUserName();
    if (!currentUser.equals(this.userWithLocalPathAccess)) {
      throw new AccessControlException(
          "Can't continue with getBlockLocalPathInfo() "
              + "authorization. The user " + currentUser
              + " is not allowed to call getBlockLocalPathInfo");
    }
  }

  @Override
  public BlockLocalPathInfo getBlockLocalPathInfo(Block block,
      Token<BlockTokenIdentifier> token) throws IOException {
    checkBlockLocalPathAccess();
    checkBlockToken(block, token, BlockTokenSecretManager.AccessMode.READ);
    BlockLocalPathInfo info = data.getBlockLocalPathInfo(block);
    if (LOG.isDebugEnabled()) {
      if (info != null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("getBlockLocalPathInfo successful block=" + block
              + " blockfile " + info.getBlockPath() + " metafile "
              + info.getMetaPath());
        }
      } else {
        if (LOG.isTraceEnabled()) {
          LOG.trace("getBlockLocalPathInfo for block=" + block
              + " returning null");
        }
      }
    }
    //myMetrics.incrBlocksGetLocalPathInfo();
    return info;
  }
  
  private void checkBlockToken(Block block, Token<BlockTokenIdentifier> token,
      AccessMode accessMode) throws IOException {
    if (isBlockTokenEnabled && UserGroupInformation.isSecurityEnabled()) {
      BlockTokenIdentifier id = new BlockTokenIdentifier();
      ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
      DataInputStream in = new DataInputStream(buf);
      id.readFields(in);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Got: " + id.toString());
      }
      blockTokenSecretManager.checkAccess(id, null, block, accessMode);
    }
  }

  /** Check block access token for the given access mode */
  private void checkBlockToken(Block block,
      BlockTokenSecretManager.AccessMode accessMode) throws IOException {
    if (isBlockTokenEnabled && UserGroupInformation.isSecurityEnabled()) {
      Set<TokenIdentifier> tokenIds = UserGroupInformation.getCurrentUser()
          .getTokenIdentifiers();
      if (tokenIds.size() != 1) {
        throw new IOException("Can't continue with "
            + "authorization since " + tokenIds.size()
            + " BlockTokenIdentifier " + "is found.");
      }
      for (TokenIdentifier tokenId : tokenIds) {
        BlockTokenIdentifier id = (BlockTokenIdentifier) tokenId;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Got: " + id.toString());
        }
        blockTokenSecretManager.checkAccess(id, null, block, accessMode);
      }
    }
  }

  /** A convenient class used in lease recovery */
  private static class BlockRecord { 
    final DatanodeID id;
    final InterDatanodeProtocol datanode;
    final BlockRecoveryInfo info;
    
    BlockRecord(DatanodeID id, InterDatanodeProtocol datanode,
        BlockRecoveryInfo info) {
      this.id = id;
      this.datanode = datanode;
      this.info = info;
    }

    /** {@inheritDoc} */
    public String toString() {
      return "BlockRecord(info=" + info + " node=" + id + ")";
    }
  }

  /** Recover a block
   * @param keepLength if true, will only recover replicas that have the same length
   * as the block passed in. Otherwise, will calculate the minimum length of the
   * replicas and truncate the rest to that length.
   **/
  private LocatedBlock recoverBlock(Block block, boolean keepLength,
      DatanodeInfo[] targets, boolean closeFile) throws IOException {

    // If the block is already being recovered, then skip recovering it.
    // This can happen if the namenode and client start recovering the same
    // file at the same time.
    synchronized (ongoingRecovery) {
      if (ongoingRecovery.get(block.getWithWildcardGS()) != null) {
        String msg = "Block " + block + " is already being recovered, " +
                     " ignoring this request to recover it.";
        LOG.info(msg);
        throw new IOException(msg);
      }
      ongoingRecovery.put(block, block);
    }
    try {
      int errorCount = 0;

      // Number of "replicasBeingWritten" in 0.21 parlance - these are replicas
      // on DNs that are still alive from when the write was happening
      int rbwCount = 0;
      // Number of "replicasWaitingRecovery" in 0.21 parlance - these replicas
      // have survived a DN restart, and thus might be truncated (eg if the
      // DN died because of a machine power failure, and when the ext3 journal
      // replayed, it truncated the file
      int rwrCount = 0;
      
      List<BlockRecord> blockRecords = new ArrayList<BlockRecord>();
      for (DatanodeInfo id : targets) {
        try {
          InterDatanodeProtocol datanode;
          if (dnRegistration.getHost().equals(id.getHost()) &&
              dnRegistration.getIpcPort() == id.getIpcPort()) {
            datanode = this;
          } else {
            datanode = DataNode.createInterDataNodeProtocolProxy(id, getConf(),
                socketTimeout, connectToDnViaHostname);
          }
          BlockRecoveryInfo info = datanode.startBlockRecovery(block);
          if (info == null) {
            LOG.info("No block metadata found for block " + block + " on datanode "
                + id);
            continue;
          }
          if (info.getBlock().getGenerationStamp() < block.getGenerationStamp()) {
            LOG.info("Only old generation stamp " + info.getBlock().getGenerationStamp()
                + " found on datanode " + id + " (needed block=" +
                block + ")");
            continue;
          }
          blockRecords.add(new BlockRecord(id, datanode, info));

          if (info.wasRecoveredOnStartup()) {
            rwrCount++;
          } else {
            rbwCount++;
          }
        } catch (IOException e) {
          ++errorCount;
          InterDatanodeProtocol.LOG.warn(
              "Failed to getBlockMetaDataInfo for block (=" + block 
              + ") from datanode (=" + id + ")", e);
        }
      }

      // If we *only* have replicas from post-DN-restart, then we should
      // include them in determining length. Otherwise they might cause us
      // to truncate too short.
      boolean shouldRecoverRwrs = (rbwCount == 0);
      
      List<BlockRecord> syncList = new ArrayList<BlockRecord>();
      long minlength = Long.MAX_VALUE;
      
      for (BlockRecord record : blockRecords) {
        BlockRecoveryInfo info = record.info;
        assert (info != null && info.getBlock().getGenerationStamp() >= block.getGenerationStamp());
        if (!shouldRecoverRwrs && info.wasRecoveredOnStartup()) {
          LOG.info("Not recovering replica " + record + " since it was recovered on "
              + "startup and we have better replicas");
          continue;
        }
        if (keepLength) {
          if (info.getBlock().getNumBytes() == block.getNumBytes()) {
            syncList.add(record);
          }
        } else {          
          syncList.add(record);
          if (info.getBlock().getNumBytes() < minlength) {
            minlength = info.getBlock().getNumBytes();
          }
        }
      }

      if (syncList.isEmpty() && errorCount > 0) {
        throw new IOException("All datanodes failed: block=" + block
            + ", datanodeids=" + Arrays.asList(targets));
      }
      if (!keepLength) {
        block.setNumBytes(minlength);
      }
      return syncBlock(block, syncList, targets, closeFile);
    } finally {
      synchronized (ongoingRecovery) {
        ongoingRecovery.remove(block);
      }
    }
  }

  /** Block synchronization */
  private LocatedBlock syncBlock(Block block, List<BlockRecord> syncList,
      DatanodeInfo[] targets, boolean closeFile) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("block=" + block + ", (length=" + block.getNumBytes()
          + "), syncList=" + syncList + ", closeFile=" + closeFile);
    }

    //syncList.isEmpty() that all datanodes do not have the block
    //so the block can be deleted.
    if (syncList.isEmpty()) {
      namenode.commitBlockSynchronization(block, 0, 0, closeFile, true,
          DatanodeID.EMPTY_ARRAY);
      //always return a new access token even if everything else stays the same
      LocatedBlock b = new LocatedBlock(block, targets);
      if (isBlockTokenEnabled) {
        b.setBlockToken(blockTokenSecretManager.generateToken(null, b.getBlock(), 
            EnumSet.of(BlockTokenSecretManager.AccessMode.WRITE)));
      }
      return b;
    }

    List<DatanodeID> successList = new ArrayList<DatanodeID>();

    long generationstamp = namenode.nextGenerationStamp(block, closeFile);
    Block newblock = new Block(block.getBlockId(), block.getNumBytes(), generationstamp);

    for(BlockRecord r : syncList) {
      try {
        r.datanode.updateBlock(r.info.getBlock(), newblock, closeFile);
        successList.add(r.id);
      } catch (IOException e) {
        InterDatanodeProtocol.LOG.warn("Failed to updateBlock (newblock="
            + newblock + ", datanode=" + r.id + ")", e);
      }
    }

    if (!successList.isEmpty()) {
      DatanodeID[] nlist = successList.toArray(new DatanodeID[successList.size()]);

      namenode.commitBlockSynchronization(block,
          newblock.getGenerationStamp(), newblock.getNumBytes(), closeFile, false,
          nlist);
      DatanodeInfo[] info = new DatanodeInfo[nlist.length];
      for (int i = 0; i < nlist.length; i++) {
        info[i] = new DatanodeInfo(nlist[i]);
      }
      LocatedBlock b = new LocatedBlock(newblock, info); // success
      // should have used client ID to generate access token, but since 
      // owner ID is not checked, we simply pass null for now.
      if (isBlockTokenEnabled) {
        b.setBlockToken(blockTokenSecretManager.generateToken(null, b.getBlock(), 
            EnumSet.of(BlockTokenSecretManager.AccessMode.WRITE)));
      }
      return b;
    }

    //failed
    StringBuilder b = new StringBuilder();
    for(BlockRecord r : syncList) {
      b.append("\n  " + r.id);
    }
    throw new IOException("Cannot recover " + block + ", none of these "
        + syncList.size() + " datanodes success {" + b + "\n}");
  }
  
  // ClientDataNodeProtocol implementation
  /** {@inheritDoc} */
  public LocatedBlock recoverBlock(Block block, boolean keepLength, DatanodeInfo[] targets
      ) throws IOException {
    logRecoverBlock("Client", block, targets);
    checkBlockToken(block, BlockTokenSecretManager.AccessMode.WRITE);
    return recoverBlock(block, keepLength, targets, false);
  }

  /** {@inheritDoc} */
  public Block getBlockInfo(Block block) throws IOException {
    checkBlockToken(block, BlockTokenSecretManager.AccessMode.READ);
    Block stored = data.getStoredBlock(block.getBlockId());
    return stored;
  }

  private static void logRecoverBlock(String who,
      Block block, DatanodeID[] targets) {
    StringBuilder msg = new StringBuilder(targets[0].getName());
    for (int i = 1; i < targets.length; i++) {
      msg.append(", " + targets[i].getName());
    }
    LOG.info(who + " calls recoverBlock(block=" + block
        + ", targets=[" + msg + "])");
  }
  
  public static InetSocketAddress getStreamingAddr(Configuration conf) {
    String address = 
      NetUtils.getServerAddress(conf,
                                "dfs.datanode.bindAddress", 
                                "dfs.datanode.port",
                                "dfs.datanode.address");
    return NetUtils.createSocketAddr(address);
  }

  
  @Override // DataNodeMXBean
  public String getHostName() {
    return this.machineName;
  }
  
  @Override // DataNodeMXBean
  public String getVersion() {
    return VersionInfo.getVersion();
  }
  
  @Override // DataNodeMXBean
  public String getRpcPort(){
    InetSocketAddress ipcAddr = NetUtils.createSocketAddr(
        this.getConf().get("dfs.datanode.ipc.address"));
    return Integer.toString(ipcAddr.getPort());
  }

  @Override // DataNodeMXBean
  public String getHttpPort(){
    return this.getConf().get("dfs.datanode.info.port");
  }

  @Override // DataNodeMXBean
  public String getNamenodeAddress(){
    return nameNodeAddr.getHostName();
  }

  /**
   * Returned information is a JSON representation of a map with 
   * volume name as the key and value is a map of volume attribute 
   * keys to its values
   */
  @Override // DataNodeMXBean
  public String getVolumeInfo() {
    final Map<String, Object> info = new HashMap<String, Object>();
    Collection<VolumeInfo> volumes = ((FSDataset)this.data).getVolumeInfo();
    for (VolumeInfo v : volumes) {
      final Map<String, Object> innerInfo = new HashMap<String, Object>();
      innerInfo.put("usedSpace", v.usedSpace);
      innerInfo.put("freeSpace", v.freeSpace);
      innerInfo.put("reservedSpace", v.reservedSpace);
      info.put(v.directory, innerInfo);
    }
    return JSON.toString(info);
  }

  long getReadaheadLength() {
    return readaheadLength;
  }

  boolean shouldDropCacheBehindWrites() {
    return dropCacheBehindWrites;
  }

  boolean shouldDropCacheBehindReads() {
    return dropCacheBehindReads;
  }

  boolean shouldSyncBehindWrites() {
    return syncBehindWrites;
  }
}
