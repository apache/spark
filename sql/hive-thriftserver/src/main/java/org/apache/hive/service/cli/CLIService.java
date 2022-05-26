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

package org.apache.hive.service.cli;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.service.CompositeService;
import org.apache.hive.service.ServiceException;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.operation.Operation;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.hive.service.cli.session.SessionManager;
import org.apache.hive.service.rpc.thrift.TOperationHandle;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.apache.hive.service.rpc.thrift.TRowSet;
import org.apache.hive.service.rpc.thrift.TTableSchema;
import org.apache.hive.service.server.HiveServer2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CLIService.
 *
 */
public class CLIService extends CompositeService implements ICLIService {

  public static final TProtocolVersion SERVER_VERSION;

  static {
    TProtocolVersion[] protocols = TProtocolVersion.values();
    SERVER_VERSION = protocols[protocols.length - 1];
  }

  private final Logger LOG = LoggerFactory.getLogger(CLIService.class.getName());

  private HiveConf hiveConf;
  private SessionManager sessionManager;
  private UserGroupInformation serviceUGI;
  private UserGroupInformation httpUGI;
  // The HiveServer2 instance running this service
  private final HiveServer2 hiveServer2;

  public CLIService(HiveServer2 hiveServer2) {
    super(CLIService.class.getSimpleName());
    this.hiveServer2 = hiveServer2;
  }

  @Override
  public synchronized void init(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
    sessionManager = new SessionManager(hiveServer2);
    addService(sessionManager);
    //  If the hadoop cluster is secure, do a kerberos login for the service from the keytab
    if (UserGroupInformation.isSecurityEnabled()) {
      try {
        HiveAuthFactory.loginFromKeytab(hiveConf);
        this.serviceUGI = Utils.getUGI();
      } catch (IOException e) {
        throw new ServiceException("Unable to login to kerberos with given principal/keytab", e);
      } catch (LoginException e) {
        throw new ServiceException("Unable to login to kerberos with given principal/keytab", e);
      }

      // Also try creating a UGI object for the SPNego principal
      String principal = hiveConf.getVar(ConfVars.HIVE_SERVER2_SPNEGO_PRINCIPAL);
      String keyTabFile = hiveConf.getVar(ConfVars.HIVE_SERVER2_SPNEGO_KEYTAB);
      if (principal.isEmpty() || keyTabFile.isEmpty()) {
        LOG.info("SPNego httpUGI not created, spNegoPrincipal: " + principal +
            ", ketabFile: " + keyTabFile);
      } else {
        try {
          this.httpUGI = HiveAuthFactory.loginFromSpnegoKeytabAndReturnUGI(hiveConf);
          LOG.info("SPNego httpUGI successfully created.");
        } catch (IOException e) {
          LOG.warn("SPNego httpUGI creation failed: ", e);
        }
      }
    }
    // creates connection to HMS and thus *must* occur after kerberos login above
    try {
      applyAuthorizationConfigPolicy(hiveConf);
    } catch (Exception e) {
      throw new RuntimeException("Error applying authorization policy on hive configuration: "
          + e.getMessage(), e);
    }
    setupBlockedUdfs();
    super.init(hiveConf);
  }

  private void applyAuthorizationConfigPolicy(HiveConf newHiveConf) throws HiveException,
      MetaException {
    // authorization setup using SessionState should be revisited eventually, as
    // authorization and authentication are not session specific settings
    SessionState ss = new SessionState(newHiveConf);
    ss.setIsHiveServerQuery(true);
    SessionState.start(ss);
    ss.applyAuthorizationPolicy();
  }

  private void setupBlockedUdfs() {
    FunctionRegistry.setupPermissionsForBuiltinUDFs(
        hiveConf.getVar(ConfVars.HIVE_SERVER2_BUILTIN_UDF_WHITELIST),
        hiveConf.getVar(ConfVars.HIVE_SERVER2_BUILTIN_UDF_BLACKLIST));
  }

  public UserGroupInformation getServiceUGI() {
    return this.serviceUGI;
  }

  public UserGroupInformation getHttpUGI() {
    return this.httpUGI;
  }

  @Override
  public synchronized void start() {
    super.start();
    // Initialize and test a connection to the metastore
    IMetaStoreClient metastoreClient = null;
    try {
      metastoreClient = new HiveMetaStoreClient(hiveConf);
      metastoreClient.getDatabases("default");
    } catch (Exception e) {
      throw new ServiceException("Unable to connect to MetaStore!", e);
    }
    finally {
      if (metastoreClient != null) {
        metastoreClient.close();
      }
    }
  }

  @Override
  public synchronized void stop() {
    super.stop();
  }

  /**
   * @deprecated  Use {@link #openSession(TProtocolVersion, String, String, String, Map)}
   */
  @Deprecated
  public SessionHandle openSession(TProtocolVersion protocol, String username, String password,
      Map<String, String> configuration) throws HiveSQLException {
    SessionHandle sessionHandle = sessionManager.openSession(protocol, username, password, null, configuration, false, null);
    LOG.debug(sessionHandle + ": openSession()");
    return sessionHandle;
  }

  /**
   * @deprecated  Use {@link #openSessionWithImpersonation(TProtocolVersion, String, String, String, Map, String)}
   */
  @Deprecated
  public SessionHandle openSessionWithImpersonation(TProtocolVersion protocol, String username,
      String password, Map<String, String> configuration, String delegationToken)
          throws HiveSQLException {
    SessionHandle sessionHandle = sessionManager.openSession(protocol, username, password, null, configuration,
        true, delegationToken);
    LOG.debug(sessionHandle + ": openSessionWithImpersonation()");
    return sessionHandle;
  }

  public SessionHandle openSession(TProtocolVersion protocol, String username, String password, String ipAddress,
      Map<String, String> configuration) throws HiveSQLException {
    SessionHandle sessionHandle = sessionManager.openSession(protocol, username, password, ipAddress, configuration, false, null);
    LOG.debug(sessionHandle + ": openSession()");
    return sessionHandle;
  }

  public SessionHandle openSessionWithImpersonation(TProtocolVersion protocol, String username,
      String password, String ipAddress, Map<String, String> configuration, String delegationToken)
          throws HiveSQLException {
    SessionHandle sessionHandle = sessionManager.openSession(protocol, username, password, ipAddress, configuration,
        true, delegationToken);
    LOG.debug(sessionHandle + ": openSession()");
    return sessionHandle;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#openSession(java.lang.String, java.lang.String, java.util.Map)
   */
  @Override
  public SessionHandle openSession(String username, String password, Map<String, String> configuration)
      throws HiveSQLException {
    SessionHandle sessionHandle = sessionManager.openSession(SERVER_VERSION, username, password, null, configuration, false, null);
    LOG.debug(sessionHandle + ": openSession()");
    return sessionHandle;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#openSession(java.lang.String, java.lang.String, java.util.Map)
   */
  @Override
  public SessionHandle openSessionWithImpersonation(String username, String password, Map<String, String> configuration,
      String delegationToken) throws HiveSQLException {
    SessionHandle sessionHandle = sessionManager.openSession(SERVER_VERSION, username, password, null, configuration,
        true, delegationToken);
    LOG.debug(sessionHandle + ": openSession()");
    return sessionHandle;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#closeSession(org.apache.hive.service.cli.SessionHandle)
   */
  @Override
  public void closeSession(SessionHandle sessionHandle)
      throws HiveSQLException {
    sessionManager.closeSession(sessionHandle);
    LOG.debug(sessionHandle + ": closeSession()");
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getInfo(org.apache.hive.service.cli.SessionHandle, java.util.List)
   */
  @Override
  public GetInfoValue getInfo(SessionHandle sessionHandle, GetInfoType getInfoType)
      throws HiveSQLException {
    GetInfoValue infoValue = sessionManager.getSession(sessionHandle)
        .getInfo(getInfoType);
    LOG.debug(sessionHandle + ": getInfo()");
    return infoValue;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#executeStatement(org.apache.hive.service.cli.SessionHandle,
   *  java.lang.String, java.util.Map)
   */
  @Override
  public OperationHandle executeStatement(SessionHandle sessionHandle, String statement,
      Map<String, String> confOverlay) throws HiveSQLException {
    HiveSession session = sessionManager.getSession(sessionHandle);
    // need to reset the monitor, as operation handle is not available down stream, Ideally the
    // monitor should be associated with the operation handle.
    session.getSessionState().updateProgressMonitor(null);
    OperationHandle opHandle = session.executeStatement(statement, confOverlay);
    LOG.debug(sessionHandle + ": executeStatement()");
    return opHandle;
  }

  /**
   * Execute statement on the server with a timeout. This is a blocking call.
   */
  @Override
  public OperationHandle executeStatement(SessionHandle sessionHandle, String statement,
        Map<String, String> confOverlay, long queryTimeout) throws HiveSQLException {
    HiveSession session = sessionManager.getSession(sessionHandle);
    // need to reset the monitor, as operation handle is not available down stream, Ideally the
    // monitor should be associated with the operation handle.
    session.getSessionState().updateProgressMonitor(null);
    OperationHandle opHandle = session.executeStatement(statement, confOverlay, queryTimeout);
    LOG.debug(sessionHandle + ": executeStatement()");
    return opHandle;
  }

  /**
   * Execute statement asynchronously on the server. This is a non-blocking call
   */
  @Override
  public OperationHandle executeStatementAsync(SessionHandle sessionHandle, String statement,
      Map<String, String> confOverlay) throws HiveSQLException {
    HiveSession session = sessionManager.getSession(sessionHandle);
    // need to reset the monitor, as operation handle is not available down stream, Ideally the
    // monitor should be associated with the operation handle.
    session.getSessionState().updateProgressMonitor(null);
    OperationHandle opHandle = session.executeStatementAsync(statement, confOverlay);
    LOG.debug(sessionHandle + ": executeStatementAsync()");
    return opHandle;
  }

  /**
   * Execute statement asynchronously on the server with a timeout. This is a non-blocking call
   */
  @Override
  public OperationHandle executeStatementAsync(SessionHandle sessionHandle, String statement,
      Map<String, String> confOverlay, long queryTimeout) throws HiveSQLException {
    HiveSession session = sessionManager.getSession(sessionHandle);
    // need to reset the monitor, as operation handle is not available down stream, Ideally the
    // monitor should be associated with the operation handle.
    session.getSessionState().updateProgressMonitor(null);
    OperationHandle opHandle = session.executeStatementAsync(statement, confOverlay, queryTimeout);
    LOG.debug(sessionHandle + ": executeStatementAsync()");
    return opHandle;
  }


  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getTypeInfo(org.apache.hive.service.cli.SessionHandle)
   */
  @Override
  public OperationHandle getTypeInfo(SessionHandle sessionHandle)
      throws HiveSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle)
        .getTypeInfo();
    LOG.debug(sessionHandle + ": getTypeInfo()");
    return opHandle;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getCatalogs(org.apache.hive.service.cli.SessionHandle)
   */
  @Override
  public OperationHandle getCatalogs(SessionHandle sessionHandle)
      throws HiveSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle)
        .getCatalogs();
    LOG.debug(sessionHandle + ": getCatalogs()");
    return opHandle;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getSchemas(org.apache.hive.service.cli.SessionHandle, java.lang.String, java.lang.String)
   */
  @Override
  public OperationHandle getSchemas(SessionHandle sessionHandle,
      String catalogName, String schemaName)
          throws HiveSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle)
        .getSchemas(catalogName, schemaName);
    LOG.debug(sessionHandle + ": getSchemas()");
    return opHandle;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getTables(org.apache.hive.service.cli.SessionHandle, java.lang.String, java.lang.String, java.lang.String, java.util.List)
   */
  @Override
  public OperationHandle getTables(SessionHandle sessionHandle,
      String catalogName, String schemaName, String tableName, List<String> tableTypes)
          throws HiveSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle)
        .getTables(catalogName, schemaName, tableName, tableTypes);
    LOG.debug(sessionHandle + ": getTables()");
    return opHandle;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getTableTypes(org.apache.hive.service.cli.SessionHandle)
   */
  @Override
  public OperationHandle getTableTypes(SessionHandle sessionHandle)
      throws HiveSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle)
        .getTableTypes();
    LOG.debug(sessionHandle + ": getTableTypes()");
    return opHandle;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getColumns(org.apache.hive.service.cli.SessionHandle)
   */
  @Override
  public OperationHandle getColumns(SessionHandle sessionHandle,
      String catalogName, String schemaName, String tableName, String columnName)
          throws HiveSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle)
        .getColumns(catalogName, schemaName, tableName, columnName);
    LOG.debug(sessionHandle + ": getColumns()");
    return opHandle;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getFunctions(org.apache.hive.service.cli.SessionHandle)
   */
  @Override
  public OperationHandle getFunctions(SessionHandle sessionHandle,
      String catalogName, String schemaName, String functionName)
          throws HiveSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle)
        .getFunctions(catalogName, schemaName, functionName);
    LOG.debug(sessionHandle + ": getFunctions()");
    return opHandle;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getPrimaryKeys(org.apache.hive.service.cli.SessionHandle)
   */
  @Override
  public OperationHandle getPrimaryKeys(SessionHandle sessionHandle,
      String catalog, String schema, String table) throws HiveSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle)
        .getPrimaryKeys(catalog, schema, table);
    LOG.debug(sessionHandle + ": getPrimaryKeys()");
    return opHandle;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getCrossReference(org.apache.hive.service.cli.SessionHandle)
   */
  @Override
  public OperationHandle getCrossReference(SessionHandle sessionHandle,
      String primaryCatalog, String primarySchema, String primaryTable, String foreignCatalog,
      String foreignSchema, String foreignTable) throws HiveSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle)
        .getCrossReference(primaryCatalog, primarySchema, primaryTable,
         foreignCatalog,
         foreignSchema, foreignTable);
    LOG.debug(sessionHandle + ": getCrossReference()");
    return opHandle;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getOperationStatus(org.apache.hive.service.cli.OperationHandle)
   */
  @Override
  public OperationStatus getOperationStatus(OperationHandle opHandle)
      throws HiveSQLException {
    Operation operation = sessionManager.getOperationManager().getOperation(opHandle);
    /**
     * If this is a background operation run asynchronously,
     * we block for a configured duration, before we return
     * (duration: HIVE_SERVER2_LONG_POLLING_TIMEOUT).
     * However, if the background operation is complete, we return immediately.
     */
    if (operation.shouldRunAsync()) {
      HiveConf conf = operation.getParentSession().getHiveConf();
      long timeout = HiveConf.getTimeVar(conf,
          HiveConf.ConfVars.HIVE_SERVER2_LONG_POLLING_TIMEOUT, TimeUnit.MILLISECONDS);
      try {
        operation.getBackgroundHandle().get(timeout, TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        // No Op, return to the caller since long polling timeout has expired
        LOG.trace(opHandle + ": Long polling timed out");
      } catch (CancellationException e) {
        // The background operation thread was cancelled
        LOG.trace(opHandle + ": The background operation was cancelled", e);
      } catch (ExecutionException e) {
        // The background operation thread was aborted
        LOG.warn(opHandle + ": The background operation was aborted", e);
      } catch (InterruptedException e) {
        // No op, this thread was interrupted
        // In this case, the call might return sooner than long polling timeout
      }
    }
    OperationStatus opStatus = operation.getStatus();
    LOG.debug(opHandle + ": getOperationStatus()");
    return opStatus;
  }

  public HiveConf getSessionConf(SessionHandle sessionHandle) throws HiveSQLException {
    return sessionManager.getSession(sessionHandle).getHiveConf();
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#cancelOperation(org.apache.hive.service.cli.OperationHandle)
   */
  @Override
  public void cancelOperation(OperationHandle opHandle)
      throws HiveSQLException {
    sessionManager.getOperationManager().getOperation(opHandle)
    .getParentSession().cancelOperation(opHandle);
    LOG.debug(opHandle + ": cancelOperation()");
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#closeOperation(org.apache.hive.service.cli.OperationHandle)
   */
  @Override
  public void closeOperation(OperationHandle opHandle)
      throws HiveSQLException {
    sessionManager.getOperationManager().getOperation(opHandle)
    .getParentSession().closeOperation(opHandle);
    LOG.debug(opHandle + ": closeOperation");
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getResultSetMetadata(org.apache.hive.service.cli.OperationHandle)
   */
  @Override
  public TTableSchema getResultSetMetadata(OperationHandle opHandle)
      throws HiveSQLException {
    TTableSchema tableSchema = sessionManager.getOperationManager()
        .getOperation(opHandle).getParentSession().getResultSetMetadata(opHandle);
    LOG.debug(opHandle + ": getResultSetMetadata()");
    return tableSchema;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#fetchResults(org.apache.hive.service.cli.OperationHandle)
   */
  @Override
  public TRowSet fetchResults(OperationHandle opHandle)
      throws HiveSQLException {
    return fetchResults(opHandle, Operation.DEFAULT_FETCH_ORIENTATION,
        Operation.DEFAULT_FETCH_MAX_ROWS, FetchType.QUERY_OUTPUT);
  }

  @Override
  public TRowSet fetchResults(OperationHandle opHandle, FetchOrientation orientation,
      long maxRows, FetchType fetchType) throws HiveSQLException {
    TRowSet rowSet = sessionManager.getOperationManager().getOperation(opHandle)
        .getParentSession().fetchResults(opHandle, orientation, maxRows, fetchType);
    LOG.debug(opHandle + ": fetchResults()");
    return rowSet;
  }

  // obtain delegation token for the give user from metastore
  public synchronized String getDelegationTokenFromMetaStore(String owner)
      throws HiveSQLException, UnsupportedOperationException, LoginException, IOException {
    if (!hiveConf.getBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL) ||
        !hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS)) {
      throw new UnsupportedOperationException(
          "delegation token is can only be obtained for a secure remote metastore");
    }

    try {
      Hive.closeCurrent();
      return Hive.get(hiveConf).getDelegationToken(owner, owner);
    } catch (HiveException e) {
      if (e.getCause() instanceof UnsupportedOperationException) {
        throw (UnsupportedOperationException)e.getCause();
      } else {
        throw new HiveSQLException("Error connect metastore to setup impersonation", e);
      }
    }
  }

  @Override
  public String getDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
      String owner, String renewer) throws HiveSQLException {
    String delegationToken = sessionManager.getSession(sessionHandle)
        .getDelegationToken(authFactory, owner, renewer);
    LOG.info(sessionHandle  + ": getDelegationToken()");
    return delegationToken;
  }

  @Override
  public void cancelDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
      String tokenStr) throws HiveSQLException {
    sessionManager.getSession(sessionHandle).cancelDelegationToken(authFactory, tokenStr);
    LOG.info(sessionHandle  + ": cancelDelegationToken()");
  }

  @Override
  public void renewDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
      String tokenStr) throws HiveSQLException {
    sessionManager.getSession(sessionHandle).renewDelegationToken(authFactory, tokenStr);
    LOG.info(sessionHandle  + ": renewDelegationToken()");
  }

  @Override
  public String getQueryId(TOperationHandle opHandle) throws HiveSQLException {
    Operation operation = sessionManager.getOperationManager().getOperation(
        new OperationHandle(opHandle));
    final String queryId = operation.getParentSession().getHiveConf().getVar(ConfVars.HIVEQUERYID);
    LOG.debug(opHandle + ": getQueryId() " + queryId);
    return queryId;
  }

  public SessionManager getSessionManager() {
    return sessionManager;
  }
}
