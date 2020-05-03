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

package org.apache.hive.service.cli.thrift;

import java.util.List;
import java.util.Map;

import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.*;
import org.apache.thrift.TException;

/**
 * ThriftCLIServiceClient.
 *
 */
public class ThriftCLIServiceClient extends CLIServiceClient {
  private final TCLIService.Iface cliService;

  public ThriftCLIServiceClient(TCLIService.Iface cliService) {
    this.cliService = cliService;
  }

  public void checkStatus(TStatus status) throws HiveSQLException {
    if (TStatusCode.ERROR_STATUS.equals(status.getStatusCode())) {
      throw new HiveSQLException(status);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#openSession(java.lang.String, java.lang.String, java.util.Map)
   */
  @Override
  public SessionHandle openSession(String username, String password,
      Map<String, String> configuration)
          throws HiveSQLException {
    try {
      TOpenSessionReq req = new TOpenSessionReq();
      req.setUsername(username);
      req.setPassword(password);
      req.setConfiguration(configuration);
      TOpenSessionResp resp = cliService.OpenSession(req);
      checkStatus(resp.getStatus());
      return new SessionHandle(resp.getSessionHandle(), resp.getServerProtocolVersion());
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#closeSession(org.apache.hive.service.cli.SessionHandle)
   */
  @Override
  public SessionHandle openSessionWithImpersonation(String username, String password,
      Map<String, String> configuration, String delegationToken) throws HiveSQLException {
    throw new HiveSQLException("open with impersonation operation is not supported in the client");
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#closeSession(org.apache.hive.service.cli.SessionHandle)
   */
  @Override
  public void closeSession(SessionHandle sessionHandle) throws HiveSQLException {
    try {
      TCloseSessionReq req = new TCloseSessionReq(sessionHandle.toTSessionHandle());
      TCloseSessionResp resp = cliService.CloseSession(req);
      checkStatus(resp.getStatus());
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getInfo(org.apache.hive.service.cli.SessionHandle, java.util.List)
   */
  @Override
  public GetInfoValue getInfo(SessionHandle sessionHandle, GetInfoType infoType)
      throws HiveSQLException {
    try {
      // FIXME extract the right info type
      TGetInfoReq req = new TGetInfoReq(sessionHandle.toTSessionHandle(), infoType.toTGetInfoType());
      TGetInfoResp resp = cliService.GetInfo(req);
      checkStatus(resp.getStatus());
      return new GetInfoValue(resp.getInfoValue());
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#executeStatement(org.apache.hive.service.cli.SessionHandle, java.lang.String, java.util.Map)
   */
  @Override
  public OperationHandle executeStatement(SessionHandle sessionHandle, String statement,
      Map<String, String> confOverlay)
          throws HiveSQLException {
    return executeStatementInternal(sessionHandle, statement, confOverlay, false);
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#executeStatementAsync(org.apache.hive.service.cli.SessionHandle, java.lang.String, java.util.Map)
   */
  @Override
  public OperationHandle executeStatementAsync(SessionHandle sessionHandle, String statement,
      Map<String, String> confOverlay)
          throws HiveSQLException {
    return executeStatementInternal(sessionHandle, statement, confOverlay, true);
  }

  private OperationHandle executeStatementInternal(SessionHandle sessionHandle, String statement,
      Map<String, String> confOverlay, boolean isAsync)
          throws HiveSQLException {
    try {
      TExecuteStatementReq req =
          new TExecuteStatementReq(sessionHandle.toTSessionHandle(), statement);
      req.setConfOverlay(confOverlay);
      req.setRunAsync(isAsync);
      TExecuteStatementResp resp = cliService.ExecuteStatement(req);
      checkStatus(resp.getStatus());
      TProtocolVersion protocol = sessionHandle.getProtocolVersion();
      return new OperationHandle(resp.getOperationHandle(), protocol);
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getTypeInfo(org.apache.hive.service.cli.SessionHandle)
   */
  @Override
  public OperationHandle getTypeInfo(SessionHandle sessionHandle) throws HiveSQLException {
    try {
      TGetTypeInfoReq req = new TGetTypeInfoReq(sessionHandle.toTSessionHandle());
      TGetTypeInfoResp resp = cliService.GetTypeInfo(req);
      checkStatus(resp.getStatus());
      TProtocolVersion protocol = sessionHandle.getProtocolVersion();
      return new OperationHandle(resp.getOperationHandle(), protocol);
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getCatalogs(org.apache.hive.service.cli.SessionHandle)
   */
  @Override
  public OperationHandle getCatalogs(SessionHandle sessionHandle) throws HiveSQLException {
    try {
      TGetCatalogsReq req = new TGetCatalogsReq(sessionHandle.toTSessionHandle());
      TGetCatalogsResp resp = cliService.GetCatalogs(req);
      checkStatus(resp.getStatus());
      TProtocolVersion protocol = sessionHandle.getProtocolVersion();
      return new OperationHandle(resp.getOperationHandle(), protocol);
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getSchemas(org.apache.hive.service.cli.SessionHandle, java.lang.String, java.lang.String)
   */
  @Override
  public OperationHandle getSchemas(SessionHandle sessionHandle, String catalogName,
      String schemaName)
          throws HiveSQLException {
    try {
      TGetSchemasReq req = new TGetSchemasReq(sessionHandle.toTSessionHandle());
      req.setCatalogName(catalogName);
      req.setSchemaName(schemaName);
      TGetSchemasResp resp = cliService.GetSchemas(req);
      checkStatus(resp.getStatus());
      TProtocolVersion protocol = sessionHandle.getProtocolVersion();
      return new OperationHandle(resp.getOperationHandle(), protocol);
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getTables(org.apache.hive.service.cli.SessionHandle, java.lang.String, java.lang.String, java.lang.String, java.util.List)
   */
  @Override
  public OperationHandle getTables(SessionHandle sessionHandle, String catalogName,
      String schemaName, String tableName, List<String> tableTypes)
          throws HiveSQLException {
    try {
      TGetTablesReq req = new TGetTablesReq(sessionHandle.toTSessionHandle());
      req.setTableName(tableName);
      req.setTableTypes(tableTypes);
      req.setSchemaName(schemaName);
      TGetTablesResp resp = cliService.GetTables(req);
      checkStatus(resp.getStatus());
      TProtocolVersion protocol = sessionHandle.getProtocolVersion();
      return new OperationHandle(resp.getOperationHandle(), protocol);
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getTableTypes(org.apache.hive.service.cli.SessionHandle)
   */
  @Override
  public OperationHandle getTableTypes(SessionHandle sessionHandle) throws HiveSQLException {
    try {
      TGetTableTypesReq req = new TGetTableTypesReq(sessionHandle.toTSessionHandle());
      TGetTableTypesResp resp = cliService.GetTableTypes(req);
      checkStatus(resp.getStatus());
      TProtocolVersion protocol = sessionHandle.getProtocolVersion();
      return new OperationHandle(resp.getOperationHandle(), protocol);
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getColumns(org.apache.hive.service.cli.SessionHandle)
   */
  @Override
  public OperationHandle getColumns(SessionHandle sessionHandle,
      String catalogName, String schemaName, String tableName, String columnName)
          throws HiveSQLException {
    try {
      TGetColumnsReq req = new TGetColumnsReq();
      req.setSessionHandle(sessionHandle.toTSessionHandle());
      req.setCatalogName(catalogName);
      req.setSchemaName(schemaName);
      req.setTableName(tableName);
      req.setColumnName(columnName);
      TGetColumnsResp resp = cliService.GetColumns(req);
      checkStatus(resp.getStatus());
      TProtocolVersion protocol = sessionHandle.getProtocolVersion();
      return new OperationHandle(resp.getOperationHandle(), protocol);
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getFunctions(org.apache.hive.service.cli.SessionHandle)
   */
  @Override
  public OperationHandle getFunctions(SessionHandle sessionHandle,
      String catalogName, String schemaName, String functionName) throws HiveSQLException {
    try {
      TGetFunctionsReq req = new TGetFunctionsReq(sessionHandle.toTSessionHandle(), functionName);
      req.setCatalogName(catalogName);
      req.setSchemaName(schemaName);
      TGetFunctionsResp resp = cliService.GetFunctions(req);
      checkStatus(resp.getStatus());
      TProtocolVersion protocol = sessionHandle.getProtocolVersion();
      return new OperationHandle(resp.getOperationHandle(), protocol);
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getOperationStatus(org.apache.hive.service.cli.OperationHandle)
   */
  @Override
  public OperationStatus getOperationStatus(OperationHandle opHandle) throws HiveSQLException {
    try {
      TGetOperationStatusReq req = new TGetOperationStatusReq(opHandle.toTOperationHandle());
      TGetOperationStatusResp resp = cliService.GetOperationStatus(req);
      // Checks the status of the RPC call, throws an exception in case of error
      checkStatus(resp.getStatus());
      OperationState opState = OperationState.getOperationState(resp.getOperationState());
      HiveSQLException opException = null;
      if (opState == OperationState.ERROR) {
        opException = new HiveSQLException(resp.getErrorMessage(), resp.getSqlState(), resp.getErrorCode());
      }
      return new OperationStatus(opState, opException);
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#cancelOperation(org.apache.hive.service.cli.OperationHandle)
   */
  @Override
  public void cancelOperation(OperationHandle opHandle) throws HiveSQLException {
    try {
      TCancelOperationReq req = new TCancelOperationReq(opHandle.toTOperationHandle());
      TCancelOperationResp resp = cliService.CancelOperation(req);
      checkStatus(resp.getStatus());
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#closeOperation(org.apache.hive.service.cli.OperationHandle)
   */
  @Override
  public void closeOperation(OperationHandle opHandle)
      throws HiveSQLException {
    try {
      TCloseOperationReq req  = new TCloseOperationReq(opHandle.toTOperationHandle());
      TCloseOperationResp resp = cliService.CloseOperation(req);
      checkStatus(resp.getStatus());
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getResultSetMetadata(org.apache.hive.service.cli.OperationHandle)
   */
  @Override
  public TableSchema getResultSetMetadata(OperationHandle opHandle)
      throws HiveSQLException {
    try {
      TGetResultSetMetadataReq req = new TGetResultSetMetadataReq(opHandle.toTOperationHandle());
      TGetResultSetMetadataResp resp = cliService.GetResultSetMetadata(req);
      checkStatus(resp.getStatus());
      return new TableSchema(resp.getSchema());
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  @Override
  public RowSet fetchResults(OperationHandle opHandle, FetchOrientation orientation, long maxRows,
      FetchType fetchType) throws HiveSQLException {
    try {
      TFetchResultsReq req = new TFetchResultsReq();
      req.setOperationHandle(opHandle.toTOperationHandle());
      req.setOrientation(orientation.toTFetchOrientation());
      req.setMaxRows(maxRows);
      req.setFetchType(fetchType.toTFetchType());
      TFetchResultsResp resp = cliService.FetchResults(req);
      checkStatus(resp.getStatus());
      return RowSetFactory.create(resp.getResults(), opHandle.getProtocolVersion());
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#fetchResults(org.apache.hive.service.cli.OperationHandle)
   */
  @Override
  public RowSet fetchResults(OperationHandle opHandle) throws HiveSQLException {
    // TODO: set the correct default fetch size
    return fetchResults(opHandle, FetchOrientation.FETCH_NEXT, 10000, FetchType.QUERY_OUTPUT);
  }

  @Override
  public String getDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
      String owner, String renewer) throws HiveSQLException {
    TGetDelegationTokenReq req = new TGetDelegationTokenReq(
        sessionHandle.toTSessionHandle(), owner, renewer);
    try {
      TGetDelegationTokenResp tokenResp = cliService.GetDelegationToken(req);
      checkStatus(tokenResp.getStatus());
      return tokenResp.getDelegationToken();
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  @Override
  public void cancelDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
      String tokenStr) throws HiveSQLException {
    TCancelDelegationTokenReq cancelReq = new TCancelDelegationTokenReq(
          sessionHandle.toTSessionHandle(), tokenStr);
    try {
      TCancelDelegationTokenResp cancelResp =
        cliService.CancelDelegationToken(cancelReq);
      checkStatus(cancelResp.getStatus());
      return;
    } catch (TException e) {
      throw new HiveSQLException(e);
    }
  }

  @Override
  public void renewDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
      String tokenStr) throws HiveSQLException {
    TRenewDelegationTokenReq cancelReq = new TRenewDelegationTokenReq(
        sessionHandle.toTSessionHandle(), tokenStr);
    try {
      TRenewDelegationTokenResp renewResp =
        cliService.RenewDelegationToken(cancelReq);
      checkStatus(renewResp.getStatus());
      return;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }
}
