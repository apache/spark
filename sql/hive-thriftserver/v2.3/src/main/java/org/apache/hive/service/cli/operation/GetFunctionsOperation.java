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

package org.apache.hive.service.cli.operation;

import java.sql.DatabaseMetaData;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObjectUtils;
import org.apache.hadoop.hive.serde2.thrift.Type;
import org.apache.hive.service.cli.CLIServiceUtils;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.OperationType;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.thrift.TException;

/**
 * GetFunctionsOperation.
 *
 */
public class GetFunctionsOperation extends MetadataOperation {
  private static final TableSchema RESULT_SET_SCHEMA = new TableSchema()
  .addPrimitiveColumn("FUNCTION_CAT", Type.STRING_TYPE,
      "Function catalog (may be null)")
  .addPrimitiveColumn("FUNCTION_SCHEM", Type.STRING_TYPE,
      "Function schema (may be null)")
  .addPrimitiveColumn("FUNCTION_NAME", Type.STRING_TYPE,
      "Function name. This is the name used to invoke the function")
  .addPrimitiveColumn("REMARKS", Type.STRING_TYPE,
      "Explanatory comment on the function")
  .addPrimitiveColumn("FUNCTION_TYPE", Type.INT_TYPE,
      "Kind of function.")
  .addPrimitiveColumn("SPECIFIC_NAME", Type.STRING_TYPE,
      "The name which uniquely identifies this function within its schema");

  private final String catalogName;
  private final String schemaName;
  private final String functionName;

  protected final RowSet rowSet;

  public GetFunctionsOperation(HiveSession parentSession,
      String catalogName, String schemaName, String functionName) {
    super(parentSession, OperationType.GET_FUNCTIONS);
    this.catalogName = catalogName;
    this.schemaName = schemaName;
    this.functionName = functionName;
    this.rowSet = RowSetFactory.create(RESULT_SET_SCHEMA, getProtocolVersion(), false);
  }

  @Override
  public void runInternal() throws HiveSQLException {
    setState(OperationState.RUNNING);
    if (isAuthV2Enabled()) {
      // get databases for schema pattern
      IMetaStoreClient metastoreClient = getParentSession().getMetaStoreClient();
      String schemaPattern = convertSchemaPattern(schemaName);
      List<String> matchingDbs;
      try {
        matchingDbs = metastoreClient.getDatabases(schemaPattern);
      } catch (TException e) {
        setState(OperationState.ERROR);
        throw new HiveSQLException(e);
      }
      // authorize this call on the schema objects
      List<HivePrivilegeObject> privObjs = HivePrivilegeObjectUtils
          .getHivePrivDbObjects(matchingDbs);
      String cmdStr = "catalog : " + catalogName + ", schemaPattern : " + schemaName;
      authorizeMetaGets(HiveOperationType.GET_FUNCTIONS, privObjs, cmdStr);
    }

    try {
      if ((null == catalogName || "".equals(catalogName))
          && (null == schemaName || "".equals(schemaName))) {
        Set<String> functionNames =  FunctionRegistry
            .getFunctionNames(CLIServiceUtils.patternToRegex(functionName));
        for (String functionName : functionNames) {
          FunctionInfo functionInfo = FunctionRegistry.getFunctionInfo(functionName);
          Object[] rowData = new Object[] {
              null, // FUNCTION_CAT
              null, // FUNCTION_SCHEM
              functionInfo.getDisplayName(), // FUNCTION_NAME
              "", // REMARKS
              (functionInfo.isGenericUDTF() ?
                  DatabaseMetaData.functionReturnsTable
                  : DatabaseMetaData.functionNoTable), // FUNCTION_TYPE
             functionInfo.getClass().getCanonicalName()
          };
          rowSet.addRow(rowData);
        }
      }
      setState(OperationState.FINISHED);
    } catch (Exception e) {
      setState(OperationState.ERROR);
      throw new HiveSQLException(e);
    }
  }


  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.Operation#getResultSetSchema()
   */
  @Override
  public TableSchema getResultSetSchema() throws HiveSQLException {
    assertState(OperationState.FINISHED);
    return RESULT_SET_SCHEMA;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.Operation#getNextRowSet(org.apache.hive.service.cli.FetchOrientation, long)
   */
  @Override
  public RowSet getNextRowSet(FetchOrientation orientation, long maxRows) throws HiveSQLException {
    assertState(OperationState.FINISHED);
    validateDefaultFetchOrientation(orientation);
    if (orientation.equals(FetchOrientation.FETCH_FIRST)) {
      rowSet.setStartOffset(0);
    }
    return rowSet.extractSubset((int)maxRows);
  }
}
