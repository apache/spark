/*
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

package org.apache.spark.sql.thriftserver.cli.thrift;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.thriftserver.auth.HiveAuthFactory;
import org.apache.spark.sql.thriftserver.cli.*;
import org.apache.spark.sql.types.StructType;
import org.apache.thrift.TException;

import com.google.common.annotations.VisibleForTesting;

/**
 * ThriftCLIServiceClient.
 *
 */
public class ThriftCLIServiceClient extends CLIServiceClient {
    private final TCLIService.Iface cliService;

    public ThriftCLIServiceClient(TCLIService.Iface cliService, Configuration conf) {
        this.cliService = cliService;
    }

    @VisibleForTesting
    public ThriftCLIServiceClient(TCLIService.Iface cliService) {
        this(cliService, new Configuration());
    }

    public void checkStatus(TStatus status) throws SparkThriftServerSQLException {
        if (TStatusCode.ERROR_STATUS.equals(status.getStatusCode())) {
            throw new SparkThriftServerSQLException(status);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.spark.sql.thriftserver.cli.ICLIService#openSession(java.lang.String, java.lang.String, java.util.Map)
     */
    @Override
    public SessionHandle openSession(String username, String password,
                                     Map<String, String> configuration)
            throws SparkThriftServerSQLException {
        try {
            TOpenSessionReq req = new TOpenSessionReq();
            req.setUsername(username);
            req.setPassword(password);
            req.setConfiguration(configuration);
            TOpenSessionResp resp = cliService.OpenSession(req);
            checkStatus(resp.getStatus());
            return new SessionHandle(resp.getSessionHandle(), resp.getServerProtocolVersion());
        } catch (SparkThriftServerSQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SparkThriftServerSQLException(e);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.spark.sql.thriftserver.cli.ICLIService#closeSession(org.apache.spark.sql.thriftserver.cli.SessionHandle)
     */
    @Override
    public SessionHandle openSessionWithImpersonation(String username, String password,
                                                      Map<String, String> configuration, String delegationToken) throws SparkThriftServerSQLException {
        throw new SparkThriftServerSQLException("open with impersonation operation is not supported in the client");
    }

    /* (non-Javadoc)
     * @see org.apache.spark.sql.thriftserver.cli.ICLIService#closeSession(org.apache.spark.sql.thriftserver.cli.SessionHandle)
     */
    @Override
    public void closeSession(SessionHandle sessionHandle) throws SparkThriftServerSQLException {
        try {
            TCloseSessionReq req = new TCloseSessionReq(sessionHandle.toTSessionHandle());
            TCloseSessionResp resp = cliService.CloseSession(req);
            checkStatus(resp.getStatus());
        } catch (SparkThriftServerSQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SparkThriftServerSQLException(e);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.spark.sql.thriftserver.cli.ICLIService#getInfo(org.apache.spark.sql.thriftserver.cli.SessionHandle, java.util.List)
     */
    @Override
    public GetInfoValue getInfo(SessionHandle sessionHandle, GetInfoType infoType)
            throws SparkThriftServerSQLException {
        try {
            // FIXME extract the right info type
            TGetInfoReq req = new TGetInfoReq(sessionHandle.toTSessionHandle(), infoType.toTGetInfoType());
            TGetInfoResp resp = cliService.GetInfo(req);
            checkStatus(resp.getStatus());
            return new GetInfoValue(resp.getInfoValue());
        } catch (SparkThriftServerSQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SparkThriftServerSQLException(e);
        }
    }

    @Override
    public OperationHandle executeStatement(SessionHandle sessionHandle, String statement,
                                            Map<String, String> confOverlay) throws SparkThriftServerSQLException {
        return executeStatementInternal(sessionHandle, statement, confOverlay, false, 0);
    }

    @Override
    public OperationHandle executeStatement(SessionHandle sessionHandle, String statement,
                                            Map<String, String> confOverlay, long queryTimeout) throws SparkThriftServerSQLException {
        return executeStatementInternal(sessionHandle, statement, confOverlay, false, queryTimeout);
    }

    @Override
    public OperationHandle executeStatementAsync(SessionHandle sessionHandle, String statement,
                                                 Map<String, String> confOverlay) throws SparkThriftServerSQLException {
        return executeStatementInternal(sessionHandle, statement, confOverlay, true, 0);
    }

    @Override
    public OperationHandle executeStatementAsync(SessionHandle sessionHandle, String statement,
                                                 Map<String, String> confOverlay, long queryTimeout) throws SparkThriftServerSQLException {
        return executeStatementInternal(sessionHandle, statement, confOverlay, true, queryTimeout);
    }

    private OperationHandle executeStatementInternal(SessionHandle sessionHandle, String statement,
                                                     Map<String, String> confOverlay, boolean isAsync, long queryTimeout) throws SparkThriftServerSQLException {
        try {
            TExecuteStatementReq req =
                    new TExecuteStatementReq(sessionHandle.toTSessionHandle(), statement);
            req.setConfOverlay(confOverlay);
            req.setRunAsync(isAsync);
            req.setQueryTimeout(queryTimeout);
            TExecuteStatementResp resp = cliService.ExecuteStatement(req);
            checkStatus(resp.getStatus());
            TProtocolVersion protocol = sessionHandle.getProtocolVersion();
            return new OperationHandle(resp.getOperationHandle(), protocol);
        } catch (SparkThriftServerSQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SparkThriftServerSQLException(e);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.spark.sql.thriftserver.cli.ICLIService#getTypeInfo(org.apache.spark.sql.thriftserver.cli.SessionHandle)
     */
    @Override
    public OperationHandle getTypeInfo(SessionHandle sessionHandle) throws SparkThriftServerSQLException {
        try {
            TGetTypeInfoReq req = new TGetTypeInfoReq(sessionHandle.toTSessionHandle());
            TGetTypeInfoResp resp = cliService.GetTypeInfo(req);
            checkStatus(resp.getStatus());
            TProtocolVersion protocol = sessionHandle.getProtocolVersion();
            return new OperationHandle(resp.getOperationHandle(), protocol);
        } catch (SparkThriftServerSQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SparkThriftServerSQLException(e);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.spark.sql.thriftserver.cli.ICLIService#getCatalogs(org.apache.spark.sql.thriftserver.cli.SessionHandle)
     */
    @Override
    public OperationHandle getCatalogs(SessionHandle sessionHandle) throws SparkThriftServerSQLException {
        try {
            TGetCatalogsReq req = new TGetCatalogsReq(sessionHandle.toTSessionHandle());
            TGetCatalogsResp resp = cliService.GetCatalogs(req);
            checkStatus(resp.getStatus());
            TProtocolVersion protocol = sessionHandle.getProtocolVersion();
            return new OperationHandle(resp.getOperationHandle(), protocol);
        } catch (SparkThriftServerSQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SparkThriftServerSQLException(e);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.spark.sql.thriftserver.cli.ICLIService#getSchemas(org.apache.spark.sql.thriftserver.cli.SessionHandle, java.lang.String, java.lang.String)
     */
    @Override
    public OperationHandle getSchemas(SessionHandle sessionHandle, String catalogName,
                                      String schemaName)
            throws SparkThriftServerSQLException {
        try {
            TGetSchemasReq req = new TGetSchemasReq(sessionHandle.toTSessionHandle());
            req.setCatalogName(catalogName);
            req.setSchemaName(schemaName);
            TGetSchemasResp resp = cliService.GetSchemas(req);
            checkStatus(resp.getStatus());
            TProtocolVersion protocol = sessionHandle.getProtocolVersion();
            return new OperationHandle(resp.getOperationHandle(), protocol);
        } catch (SparkThriftServerSQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SparkThriftServerSQLException(e);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.spark.sql.thriftserver.cli.ICLIService#getTables(org.apache.spark.sql.thriftserver.cli.SessionHandle, java.lang.String, java.lang.String, java.lang.String, java.util.List)
     */
    @Override
    public OperationHandle getTables(SessionHandle sessionHandle, String catalogName,
                                     String schemaName, String tableName, List<String> tableTypes)
            throws SparkThriftServerSQLException {
        try {
            TGetTablesReq req = new TGetTablesReq(sessionHandle.toTSessionHandle());
            req.setTableName(tableName);
            req.setTableTypes(tableTypes);
            req.setSchemaName(schemaName);
            TGetTablesResp resp = cliService.GetTables(req);
            checkStatus(resp.getStatus());
            TProtocolVersion protocol = sessionHandle.getProtocolVersion();
            return new OperationHandle(resp.getOperationHandle(), protocol);
        } catch (SparkThriftServerSQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SparkThriftServerSQLException(e);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.spark.sql.thriftserver.cli.ICLIService#getTableTypes(org.apache.spark.sql.thriftserver.cli.SessionHandle)
     */
    @Override
    public OperationHandle getTableTypes(SessionHandle sessionHandle) throws SparkThriftServerSQLException {
        try {
            TGetTableTypesReq req = new TGetTableTypesReq(sessionHandle.toTSessionHandle());
            TGetTableTypesResp resp = cliService.GetTableTypes(req);
            checkStatus(resp.getStatus());
            TProtocolVersion protocol = sessionHandle.getProtocolVersion();
            return new OperationHandle(resp.getOperationHandle(), protocol);
        } catch (SparkThriftServerSQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SparkThriftServerSQLException(e);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.spark.sql.thriftserver.cli.ICLIService#getColumns(org.apache.spark.sql.thriftserver.cli.SessionHandle)
     */
    @Override
    public OperationHandle getColumns(SessionHandle sessionHandle,
                                      String catalogName, String schemaName, String tableName, String columnName)
            throws SparkThriftServerSQLException {
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
        } catch (SparkThriftServerSQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SparkThriftServerSQLException(e);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.spark.sql.thriftserver.cli.ICLIService#getFunctions(org.apache.spark.sql.thriftserver.cli.SessionHandle)
     */
    @Override
    public OperationHandle getFunctions(SessionHandle sessionHandle,
                                        String catalogName, String schemaName, String functionName) throws SparkThriftServerSQLException {
        try {
            TGetFunctionsReq req = new TGetFunctionsReq(sessionHandle.toTSessionHandle(), functionName);
            req.setCatalogName(catalogName);
            req.setSchemaName(schemaName);
            TGetFunctionsResp resp = cliService.GetFunctions(req);
            checkStatus(resp.getStatus());
            TProtocolVersion protocol = sessionHandle.getProtocolVersion();
            return new OperationHandle(resp.getOperationHandle(), protocol);
        } catch (SparkThriftServerSQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SparkThriftServerSQLException(e);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.spark.sql.thriftserver.cli.ICLIService#getOperationStatus(org.apache.spark.sql.thriftserver.cli.OperationHandle)
     */
    @Override
    public OperationStatus getOperationStatus(OperationHandle opHandle) throws SparkThriftServerSQLException {
        try {
            TGetOperationStatusReq req = new TGetOperationStatusReq(opHandle.toTOperationHandle());
            TGetOperationStatusResp resp = cliService.GetOperationStatus(req);
            // Checks the status of the RPC call, throws an exception in case of error
            checkStatus(resp.getStatus());
            OperationState opState = OperationState.getOperationState(resp.getOperationState());
            SparkThriftServerSQLException opException = null;
            if (opState == OperationState.ERROR) {
                opException = new SparkThriftServerSQLException(resp.getErrorMessage(), resp.getSqlState(), resp.getErrorCode());
            }
            return new OperationStatus(opState, opException);
        } catch (SparkThriftServerSQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SparkThriftServerSQLException(e);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.spark.sql.thriftserver.cli.ICLIService#cancelOperation(org.apache.spark.sql.thriftserver.cli.OperationHandle)
     */
    @Override
    public void cancelOperation(OperationHandle opHandle) throws SparkThriftServerSQLException {
        try {
            TCancelOperationReq req = new TCancelOperationReq(opHandle.toTOperationHandle());
            TCancelOperationResp resp = cliService.CancelOperation(req);
            checkStatus(resp.getStatus());
        } catch (SparkThriftServerSQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SparkThriftServerSQLException(e);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.spark.sql.thriftserver.cli.ICLIService#closeOperation(org.apache.spark.sql.thriftserver.cli.OperationHandle)
     */
    @Override
    public void closeOperation(OperationHandle opHandle)
            throws SparkThriftServerSQLException {
        try {
            TCloseOperationReq req  = new TCloseOperationReq(opHandle.toTOperationHandle());
            TCloseOperationResp resp = cliService.CloseOperation(req);
            checkStatus(resp.getStatus());
        } catch (SparkThriftServerSQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SparkThriftServerSQLException(e);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.spark.sql.thriftserver.cli.ICLIService#getResultSetMetadata(org.apache.spark.sql.thriftserver.cli.OperationHandle)
     */
    @Override
    public TableSchema getResultSetMetadata(OperationHandle opHandle)
            throws SparkThriftServerSQLException {
        try {
            TGetResultSetMetadataReq req = new TGetResultSetMetadataReq(opHandle.toTOperationHandle());
            TGetResultSetMetadataResp resp = cliService.GetResultSetMetadata(req);
            checkStatus(resp.getStatus());
            return new TableSchema(resp.getSchema());
        } catch (SparkThriftServerSQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SparkThriftServerSQLException(e);
        }
    }

    @Override
    public RowSet fetchResults(OperationHandle opHandle, FetchOrientation orientation, long maxRows,
                               FetchType fetchType) throws SparkThriftServerSQLException {
        try {
            TFetchResultsReq req = new TFetchResultsReq();
            req.setOperationHandle(opHandle.toTOperationHandle());
            req.setOrientation(orientation.toTFetchOrientation());
            req.setMaxRows(maxRows);
            req.setFetchType(fetchType.toTFetchType());
            TFetchResultsResp resp = cliService.FetchResults(req);
            checkStatus(resp.getStatus());
            return RowSetFactory.create(resp.getResults(), opHandle.getProtocolVersion());
        } catch (SparkThriftServerSQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SparkThriftServerSQLException(e);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.spark.sql.thriftserver.cli.ICLIService#fetchResults(org.apache.spark.sql.thriftserver.cli.OperationHandle)
     */
    @Override
    public RowSet fetchResults(OperationHandle opHandle) throws SparkThriftServerSQLException {
        return fetchResults(
                opHandle, FetchOrientation.FETCH_NEXT, defaultFetchRows, FetchType.QUERY_OUTPUT);
    }

    @Override
    public String getDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
                                     String owner, String renewer) throws SparkThriftServerSQLException {
        TGetDelegationTokenReq req = new TGetDelegationTokenReq(
                sessionHandle.toTSessionHandle(), owner, renewer);
        try {
            TGetDelegationTokenResp tokenResp = cliService.GetDelegationToken(req);
            checkStatus(tokenResp.getStatus());
            return tokenResp.getDelegationToken();
        } catch (Exception e) {
            throw new SparkThriftServerSQLException(e);
        }
    }

    @Override
    public void cancelDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
                                      String tokenStr) throws SparkThriftServerSQLException {
        TCancelDelegationTokenReq cancelReq = new TCancelDelegationTokenReq(
                sessionHandle.toTSessionHandle(), tokenStr);
        try {
            TCancelDelegationTokenResp cancelResp =
                    cliService.CancelDelegationToken(cancelReq);
            checkStatus(cancelResp.getStatus());
            return;
        } catch (TException e) {
            throw new SparkThriftServerSQLException(e);
        }
    }

    @Override
    public void renewDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
                                     String tokenStr) throws SparkThriftServerSQLException {
        TRenewDelegationTokenReq cancelReq = new TRenewDelegationTokenReq(
                sessionHandle.toTSessionHandle(), tokenStr);
        try {
            TRenewDelegationTokenResp renewResp =
                    cliService.RenewDelegationToken(cancelReq);
            checkStatus(renewResp.getStatus());
            return;
        } catch (Exception e) {
            throw new SparkThriftServerSQLException(e);
        }
    }

    @Override
    public OperationHandle getPrimaryKeys(SessionHandle sessionHandle,
                                          String catalog, String schema, String table) throws SparkThriftServerSQLException {
        try {
            TGetPrimaryKeysReq req = new TGetPrimaryKeysReq(sessionHandle.toTSessionHandle());
            req.setCatalogName(catalog);
            req.setSchemaName(schema);
            req.setTableName(table);
            TGetPrimaryKeysResp resp = cliService.GetPrimaryKeys(req);
            checkStatus(resp.getStatus());
            TProtocolVersion protocol = sessionHandle.getProtocolVersion();
            return new OperationHandle(resp.getOperationHandle(), protocol);
        } catch (SparkThriftServerSQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SparkThriftServerSQLException(e);
        }
    }

    @Override
    public OperationHandle getCrossReference(SessionHandle sessionHandle,
                                             String primaryCatalog, String primarySchema, String primaryTable,
                                             String foreignCatalog, String foreignSchema, String foreignTable)
            throws SparkThriftServerSQLException {
        try {
            TGetCrossReferenceReq req = new TGetCrossReferenceReq(sessionHandle.toTSessionHandle());
            req.setParentCatalogName(primaryCatalog);
            req.setParentSchemaName(primarySchema);
            req.setParentTableName(primaryTable);
            req.setForeignCatalogName(foreignCatalog);
            req.setForeignSchemaName(foreignSchema);
            req.setForeignTableName(foreignTable);
            TGetCrossReferenceResp resp = cliService.GetCrossReference(req);
            checkStatus(resp.getStatus());
            TProtocolVersion protocol = sessionHandle.getProtocolVersion();
            return new OperationHandle(resp.getOperationHandle(), protocol);
        } catch (SparkThriftServerSQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SparkThriftServerSQLException(e);
        }
    }

    @Override
    public String getQueryId(TOperationHandle operationHandle) throws SparkThriftServerSQLException {
        try {
            return cliService.GetQueryId(new TGetQueryIdReq(operationHandle)).getQueryId();
        } catch (TException e) {
            throw new SparkThriftServerSQLException(e);
        }
    }
}
