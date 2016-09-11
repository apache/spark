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

package org.apache.hive.service.cli.session;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.cli.HiveFileProcessor;
import org.apache.hadoop.hive.common.cli.IHiveFileProcessor;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.exec.FetchFormatter;
import org.apache.hadoop.hive.ql.exec.ListSinkOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.history.HiveHistory;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.processors.SetProcessor;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hive.common.util.HiveVersionInfo;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.FetchType;
import org.apache.hive.service.cli.GetInfoType;
import org.apache.hive.service.cli.GetInfoValue;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.operation.ExecuteStatementOperation;
import org.apache.hive.service.cli.operation.GetCatalogsOperation;
import org.apache.hive.service.cli.operation.GetColumnsOperation;
import org.apache.hive.service.cli.operation.GetFunctionsOperation;
import org.apache.hive.service.cli.operation.GetSchemasOperation;
import org.apache.hive.service.cli.operation.GetTableTypesOperation;
import org.apache.hive.service.cli.operation.GetTypeInfoOperation;
import org.apache.hive.service.cli.operation.MetadataOperation;
import org.apache.hive.service.cli.operation.Operation;
import org.apache.hive.service.cli.operation.OperationManager;
import org.apache.hive.service.cli.thrift.TProtocolVersion;
import org.apache.hive.service.server.ThreadWithGarbageCleanup;

/**
 * HiveSession
 *
 */
public class HiveSessionImpl implements HiveSession {
  private final SessionHandle sessionHandle;
  private String username;
  private final String password;
  private HiveConf hiveConf;
  private SessionState sessionState;
  private String ipAddress;
  private static final String FETCH_WORK_SERDE_CLASS =
      "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
  private static final Log LOG = LogFactory.getLog(HiveSessionImpl.class);
  private SessionManager sessionManager;
  private OperationManager operationManager;
  private final Set<OperationHandle> opHandleSet = new HashSet<OperationHandle>();
  private boolean isOperationLogEnabled;
  private File sessionLogDir;
  private volatile long lastAccessTime;
  private volatile long lastIdleTime;

  public HiveSessionImpl(TProtocolVersion protocol, String username, String password,
      HiveConf serverhiveConf, String ipAddress) {
    this.username = username;
    this.password = password;
    this.sessionHandle = new SessionHandle(protocol);
    this.hiveConf = new HiveConf(serverhiveConf);
    this.ipAddress = ipAddress;

    try {
      // In non-impersonation mode, map scheduler queue to current user
      // if fair scheduler is configured.
      if (! hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS) &&
        hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_MAP_FAIR_SCHEDULER_QUEUE)) {
        ShimLoader.getHadoopShims().refreshDefaultQueue(hiveConf, username);
      }
    } catch (IOException e) {
      LOG.warn("Error setting scheduler queue: " + e, e);
    }
    // Set an explicit session name to control the download directory name
    hiveConf.set(ConfVars.HIVESESSIONID.varname,
        sessionHandle.getHandleIdentifier().toString());
    // Use thrift transportable formatter
    hiveConf.set(ListSinkOperator.OUTPUT_FORMATTER,
        FetchFormatter.ThriftFormatter.class.getName());
    hiveConf.setInt(ListSinkOperator.OUTPUT_PROTOCOL, protocol.getValue());
  }

  @Override
  /**
   * Opens a new HiveServer2 session for the client connection.
   * Creates a new SessionState object that will be associated with this HiveServer2 session.
   * When the server executes multiple queries in the same session,
   * this SessionState object is reused across multiple queries.
   * Note that if doAs is true, this call goes through a proxy object,
   * which wraps the method logic in a UserGroupInformation#doAs.
   * That's why it is important to create SessionState here rather than in the constructor.
   */
  public void open(Map<String, String> sessionConfMap) throws HiveSQLException {
    sessionState = new SessionState(hiveConf, username);
    sessionState.setUserIpAddress(ipAddress);
    sessionState.setIsHiveServerQuery(true);
    SessionState.start(sessionState);
    try {
      sessionState.reloadAuxJars();
    } catch (IOException e) {
      String msg = "Failed to load reloadable jar file path: " + e;
      LOG.error(msg, e);
      throw new HiveSQLException(msg, e);
    }
    // Process global init file: .hiverc
    processGlobalInitFile();
    if (sessionConfMap != null) {
      configureSession(sessionConfMap);
    }
    lastAccessTime = System.currentTimeMillis();
    lastIdleTime = lastAccessTime;
  }

  /**
   * It is used for processing hiverc file from HiveServer2 side.
   */
  private class GlobalHivercFileProcessor extends HiveFileProcessor {
    @Override
    protected BufferedReader loadFile(String fileName) throws IOException {
      FileInputStream initStream = null;
      BufferedReader bufferedReader = null;
      initStream = new FileInputStream(fileName);
      bufferedReader = new BufferedReader(new InputStreamReader(initStream));
      return bufferedReader;
    }

    @Override
    protected int processCmd(String cmd) {
      int rc = 0;
      String cmd_trimed = cmd.trim();
      try {
        executeStatementInternal(cmd_trimed, null, false);
      } catch (HiveSQLException e) {
        rc = -1;
        LOG.warn("Failed to execute HQL command in global .hiverc file.", e);
      }
      return rc;
    }
  }

  private void processGlobalInitFile() {
    IHiveFileProcessor processor = new GlobalHivercFileProcessor();

    try {
      String hiverc = hiveConf.getVar(ConfVars.HIVE_SERVER2_GLOBAL_INIT_FILE_LOCATION);
      if (hiverc != null) {
        File hivercFile = new File(hiverc);
        if (hivercFile.isDirectory()) {
          hivercFile = new File(hivercFile, SessionManager.HIVERCFILE);
        }
        if (hivercFile.isFile()) {
          LOG.info("Running global init file: " + hivercFile);
          int rc = processor.processFile(hivercFile.getAbsolutePath());
          if (rc != 0) {
            LOG.error("Failed on initializing global .hiverc file");
          }
        } else {
          LOG.debug("Global init file " + hivercFile + " does not exist");
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed on initializing global .hiverc file", e);
    }
  }

  private void configureSession(Map<String, String> sessionConfMap) throws HiveSQLException {
    SessionState.setCurrentSessionState(sessionState);
    for (Map.Entry<String, String> entry : sessionConfMap.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith("set:")) {
        try {
          SetProcessor.setVariable(key.substring(4), entry.getValue());
        } catch (Exception e) {
          throw new HiveSQLException(e);
        }
      } else if (key.startsWith("use:")) {
        SessionState.get().setCurrentDatabase(entry.getValue());
      } else {
        hiveConf.verifyAndSet(key, entry.getValue());
      }
    }
  }

  @Override
  public void setOperationLogSessionDir(File operationLogRootDir) {
    sessionLogDir = new File(operationLogRootDir, sessionHandle.getHandleIdentifier().toString());
    isOperationLogEnabled = true;
    if (!sessionLogDir.exists()) {
      if (!sessionLogDir.mkdir()) {
        LOG.warn("Unable to create operation log session directory: " +
            sessionLogDir.getAbsolutePath());
        isOperationLogEnabled = false;
      }
    }
    if (isOperationLogEnabled) {
      LOG.info("Operation log session directory is created: " + sessionLogDir.getAbsolutePath());
    }
  }

  @Override
  public boolean isOperationLogEnabled() {
    return isOperationLogEnabled;
  }

  @Override
  public File getOperationLogSessionDir() {
    return sessionLogDir;
  }

  @Override
  public TProtocolVersion getProtocolVersion() {
    return sessionHandle.getProtocolVersion();
  }

  @Override
  public SessionManager getSessionManager() {
    return sessionManager;
  }

  @Override
  public void setSessionManager(SessionManager sessionManager) {
    this.sessionManager = sessionManager;
  }

  private OperationManager getOperationManager() {
    return operationManager;
  }

  @Override
  public void setOperationManager(OperationManager operationManager) {
    this.operationManager = operationManager;
  }

  protected synchronized void acquire(boolean userAccess) {
    // Need to make sure that the this HiveServer2's session's SessionState is
    // stored in the thread local for the handler thread.
    SessionState.setCurrentSessionState(sessionState);
    if (userAccess) {
      lastAccessTime = System.currentTimeMillis();
    }
  }

  /**
   * 1. We'll remove the ThreadLocal SessionState as this thread might now serve
   * other requests.
   * 2. We'll cache the ThreadLocal RawStore object for this background thread for an orderly cleanup
   * when this thread is garbage collected later.
   * @see org.apache.hive.service.server.ThreadWithGarbageCleanup#finalize()
   */
  protected synchronized void release(boolean userAccess) {
    SessionState.detachSession();
    if (ThreadWithGarbageCleanup.currentThread() instanceof ThreadWithGarbageCleanup) {
      ThreadWithGarbageCleanup currentThread =
          (ThreadWithGarbageCleanup) ThreadWithGarbageCleanup.currentThread();
      currentThread.cacheThreadLocalRawStore();
    }
    if (userAccess) {
      lastAccessTime = System.currentTimeMillis();
    }
    if (opHandleSet.isEmpty()) {
      lastIdleTime = System.currentTimeMillis();
    } else {
      lastIdleTime = 0;
    }
  }

  @Override
  public SessionHandle getSessionHandle() {
    return sessionHandle;
  }

  @Override
  public String getUsername() {
    return username;
  }

  @Override
  public String getPassword() {
    return password;
  }

  @Override
  public HiveConf getHiveConf() {
    hiveConf.setVar(HiveConf.ConfVars.HIVEFETCHOUTPUTSERDE, FETCH_WORK_SERDE_CLASS);
    return hiveConf;
  }

  @Override
  public IMetaStoreClient getMetaStoreClient() throws HiveSQLException {
    try {
      return Hive.get(getHiveConf()).getMSC();
    } catch (HiveException e) {
      throw new HiveSQLException("Failed to get metastore connection", e);
    } catch (MetaException e) {
      throw new HiveSQLException("Failed to get metastore connection", e);
    }
  }

  @Override
  public GetInfoValue getInfo(GetInfoType getInfoType)
      throws HiveSQLException {
    acquire(true);
    try {
      switch (getInfoType) {
      case CLI_SERVER_NAME:
        return new GetInfoValue("Hive");
      case CLI_DBMS_NAME:
        return new GetInfoValue("Apache Hive");
      case CLI_DBMS_VER:
        return new GetInfoValue(HiveVersionInfo.getVersion());
      case CLI_MAX_COLUMN_NAME_LEN:
        return new GetInfoValue(128);
      case CLI_MAX_SCHEMA_NAME_LEN:
        return new GetInfoValue(128);
      case CLI_MAX_TABLE_NAME_LEN:
        return new GetInfoValue(128);
      case CLI_TXN_CAPABLE:
      default:
        throw new HiveSQLException("Unrecognized GetInfoType value: " + getInfoType.toString());
      }
    } finally {
      release(true);
    }
  }

  @Override
  public OperationHandle executeStatement(String statement, Map<String, String> confOverlay)
      throws HiveSQLException {
    return executeStatementInternal(statement, confOverlay, false);
  }

  @Override
  public OperationHandle executeStatementAsync(String statement, Map<String, String> confOverlay)
      throws HiveSQLException {
    return executeStatementInternal(statement, confOverlay, true);
  }

  private OperationHandle executeStatementInternal(String statement, Map<String, String> confOverlay,
      boolean runAsync)
          throws HiveSQLException {
    acquire(true);

    OperationManager operationManager = getOperationManager();
    ExecuteStatementOperation operation = operationManager
        .newExecuteStatementOperation(getSession(), statement, confOverlay, runAsync);
    OperationHandle opHandle = operation.getHandle();
    try {
      operation.run();
      opHandleSet.add(opHandle);
      return opHandle;
    } catch (HiveSQLException e) {
      // Refering to SQLOperation.java,there is no chance that a HiveSQLException throws and the asyn
      // background operation submits to thread pool successfully at the same time. So, Cleanup
      // opHandle directly when got HiveSQLException
      operationManager.closeOperation(opHandle);
      throw e;
    } finally {
      release(true);
    }
  }

  @Override
  public OperationHandle getTypeInfo()
      throws HiveSQLException {
    acquire(true);

    OperationManager operationManager = getOperationManager();
    GetTypeInfoOperation operation = operationManager.newGetTypeInfoOperation(getSession());
    OperationHandle opHandle = operation.getHandle();
    try {
      operation.run();
      opHandleSet.add(opHandle);
      return opHandle;
    } catch (HiveSQLException e) {
      operationManager.closeOperation(opHandle);
      throw e;
    } finally {
      release(true);
    }
  }

  @Override
  public OperationHandle getCatalogs()
      throws HiveSQLException {
    acquire(true);

    OperationManager operationManager = getOperationManager();
    GetCatalogsOperation operation = operationManager.newGetCatalogsOperation(getSession());
    OperationHandle opHandle = operation.getHandle();
    try {
      operation.run();
      opHandleSet.add(opHandle);
      return opHandle;
    } catch (HiveSQLException e) {
      operationManager.closeOperation(opHandle);
      throw e;
    } finally {
      release(true);
    }
  }

  @Override
  public OperationHandle getSchemas(String catalogName, String schemaName)
      throws HiveSQLException {
    acquire(true);

    OperationManager operationManager = getOperationManager();
    GetSchemasOperation operation =
        operationManager.newGetSchemasOperation(getSession(), catalogName, schemaName);
    OperationHandle opHandle = operation.getHandle();
    try {
      operation.run();
      opHandleSet.add(opHandle);
      return opHandle;
    } catch (HiveSQLException e) {
      operationManager.closeOperation(opHandle);
      throw e;
    } finally {
      release(true);
    }
  }

  @Override
  public OperationHandle getTables(String catalogName, String schemaName, String tableName,
      List<String> tableTypes)
          throws HiveSQLException {
    acquire(true);

    OperationManager operationManager = getOperationManager();
    MetadataOperation operation =
        operationManager.newGetTablesOperation(getSession(), catalogName, schemaName, tableName, tableTypes);
    OperationHandle opHandle = operation.getHandle();
    try {
      operation.run();
      opHandleSet.add(opHandle);
      return opHandle;
    } catch (HiveSQLException e) {
      operationManager.closeOperation(opHandle);
      throw e;
    } finally {
      release(true);
    }
  }

  @Override
  public OperationHandle getTableTypes()
      throws HiveSQLException {
    acquire(true);

    OperationManager operationManager = getOperationManager();
    GetTableTypesOperation operation = operationManager.newGetTableTypesOperation(getSession());
    OperationHandle opHandle = operation.getHandle();
    try {
      operation.run();
      opHandleSet.add(opHandle);
      return opHandle;
    } catch (HiveSQLException e) {
      operationManager.closeOperation(opHandle);
      throw e;
    } finally {
      release(true);
    }
  }

  @Override
  public OperationHandle getColumns(String catalogName, String schemaName,
      String tableName, String columnName)  throws HiveSQLException {
    acquire(true);
    String addedJars = Utilities.getResourceFiles(hiveConf, SessionState.ResourceType.JAR);
    if (StringUtils.isNotBlank(addedJars)) {
       IMetaStoreClient metastoreClient = getSession().getMetaStoreClient();
       metastoreClient.setHiveAddedJars(addedJars);
    }
    OperationManager operationManager = getOperationManager();
    GetColumnsOperation operation = operationManager.newGetColumnsOperation(getSession(),
        catalogName, schemaName, tableName, columnName);
    OperationHandle opHandle = operation.getHandle();
    try {
      operation.run();
      opHandleSet.add(opHandle);
      return opHandle;
    } catch (HiveSQLException e) {
      operationManager.closeOperation(opHandle);
      throw e;
    } finally {
      release(true);
    }
  }

  @Override
  public OperationHandle getFunctions(String catalogName, String schemaName, String functionName)
      throws HiveSQLException {
    acquire(true);

    OperationManager operationManager = getOperationManager();
    GetFunctionsOperation operation = operationManager
        .newGetFunctionsOperation(getSession(), catalogName, schemaName, functionName);
    OperationHandle opHandle = operation.getHandle();
    try {
      operation.run();
      opHandleSet.add(opHandle);
      return opHandle;
    } catch (HiveSQLException e) {
      operationManager.closeOperation(opHandle);
      throw e;
    } finally {
      release(true);
    }
  }

  @Override
  public void close() throws HiveSQLException {
    try {
      acquire(true);
      // Iterate through the opHandles and close their operations
      for (OperationHandle opHandle : opHandleSet) {
        operationManager.closeOperation(opHandle);
      }
      opHandleSet.clear();
      // Cleanup session log directory.
      cleanupSessionLogDir();
      HiveHistory hiveHist = sessionState.getHiveHistory();
      if (null != hiveHist) {
        hiveHist.closeStream();
      }
      try {
        sessionState.close();
      } finally {
        sessionState = null;
      }
    } catch (IOException ioe) {
      throw new HiveSQLException("Failure to close", ioe);
    } finally {
      if (sessionState != null) {
        try {
          sessionState.close();
        } catch (Throwable t) {
          LOG.warn("Error closing session", t);
        }
        sessionState = null;
      }
      release(true);
    }
  }

  private void cleanupSessionLogDir() {
    if (isOperationLogEnabled) {
      try {
        FileUtils.forceDelete(sessionLogDir);
      } catch (Exception e) {
        LOG.error("Failed to cleanup session log dir: " + sessionHandle, e);
      }
    }
  }

  @Override
  public SessionState getSessionState() {
    return sessionState;
  }

  @Override
  public String getUserName() {
    return username;
  }

  @Override
  public void setUserName(String userName) {
    this.username = userName;
  }

  @Override
  public long getLastAccessTime() {
    return lastAccessTime;
  }

  @Override
  public void closeExpiredOperations() {
    OperationHandle[] handles = opHandleSet.toArray(new OperationHandle[opHandleSet.size()]);
    if (handles.length > 0) {
      List<Operation> operations = operationManager.removeExpiredOperations(handles);
      if (!operations.isEmpty()) {
        closeTimedOutOperations(operations);
      }
    }
  }

  @Override
  public long getNoOperationTime() {
    return lastIdleTime > 0 ? System.currentTimeMillis() - lastIdleTime : 0;
  }

  private void closeTimedOutOperations(List<Operation> operations) {
    acquire(false);
    try {
      for (Operation operation : operations) {
        opHandleSet.remove(operation.getHandle());
        try {
          operation.close();
        } catch (Exception e) {
          LOG.warn("Exception is thrown closing timed-out operation " + operation.getHandle(), e);
        }
      }
    } finally {
      release(false);
    }
  }

  @Override
  public void cancelOperation(OperationHandle opHandle) throws HiveSQLException {
    acquire(true);
    try {
      sessionManager.getOperationManager().cancelOperation(opHandle);
    } finally {
      release(true);
    }
  }

  @Override
  public void closeOperation(OperationHandle opHandle) throws HiveSQLException {
    acquire(true);
    try {
      operationManager.closeOperation(opHandle);
      opHandleSet.remove(opHandle);
    } finally {
      release(true);
    }
  }

  @Override
  public TableSchema getResultSetMetadata(OperationHandle opHandle) throws HiveSQLException {
    acquire(true);
    try {
      return sessionManager.getOperationManager().getOperationResultSetSchema(opHandle);
    } finally {
      release(true);
    }
  }

  @Override
  public RowSet fetchResults(OperationHandle opHandle, FetchOrientation orientation,
      long maxRows, FetchType fetchType) throws HiveSQLException {
    acquire(true);
    try {
      if (fetchType == FetchType.QUERY_OUTPUT) {
        return operationManager.getOperationNextRowSet(opHandle, orientation, maxRows);
      }
      return operationManager.getOperationLogRowSet(opHandle, orientation, maxRows);
    } finally {
      release(true);
    }
  }

  protected HiveSession getSession() {
    return this;
  }

  @Override
  public String getIpAddress() {
    return ipAddress;
  }

  @Override
  public void setIpAddress(String ipAddress) {
    this.ipAddress = ipAddress;
  }

  @Override
  public String getDelegationToken(HiveAuthFactory authFactory, String owner, String renewer)
      throws HiveSQLException {
    HiveAuthFactory.verifyProxyAccess(getUsername(), owner, getIpAddress(), getHiveConf());
    return authFactory.getDelegationToken(owner, renewer);
  }

  @Override
  public void cancelDelegationToken(HiveAuthFactory authFactory, String tokenStr)
      throws HiveSQLException {
    HiveAuthFactory.verifyProxyAccess(getUsername(), getUserFromToken(authFactory, tokenStr),
        getIpAddress(), getHiveConf());
    authFactory.cancelDelegationToken(tokenStr);
  }

  @Override
  public void renewDelegationToken(HiveAuthFactory authFactory, String tokenStr)
      throws HiveSQLException {
    HiveAuthFactory.verifyProxyAccess(getUsername(), getUserFromToken(authFactory, tokenStr),
        getIpAddress(), getHiveConf());
    authFactory.renewDelegationToken(tokenStr);
  }

  // extract the real user from the given token string
  private String getUserFromToken(HiveAuthFactory authFactory, String tokenStr) throws HiveSQLException {
    return authFactory.getUserFromToken(tokenStr);
  }
}
