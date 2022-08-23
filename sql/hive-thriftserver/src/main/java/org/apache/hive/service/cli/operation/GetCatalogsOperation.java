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

package org.apache.hive.service.cli.operation;

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hive.service.cli.*;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.hive.service.rpc.thrift.TRowSet;
import org.apache.hive.service.rpc.thrift.TTableSchema;

/**
 * GetCatalogsOperation.
 *
 */
public class GetCatalogsOperation extends MetadataOperation {
  private static final TableSchema RESULT_SET_SCHEMA = new TableSchema()
  .addStringColumn("TABLE_CAT", "Catalog name. NULL if not applicable.");

  protected final RowSet rowSet;

  protected GetCatalogsOperation(HiveSession parentSession) {
    super(parentSession, OperationType.GET_CATALOGS);
    rowSet = RowSetFactory.create(RESULT_SET_SCHEMA, getProtocolVersion(), false);
  }

  @Override
  public void runInternal() throws HiveSQLException {
    setState(OperationState.RUNNING);
    try {
      if (isAuthV2Enabled()) {
        authorizeMetaGets(HiveOperationType.GET_CATALOGS, null);
      }
      setState(OperationState.FINISHED);
    } catch (HiveSQLException e) {
      setState(OperationState.ERROR);
      throw e;
    }

  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.Operation#getResultSetSchema()
   */
  @Override
  public TTableSchema getResultSetSchema() throws HiveSQLException {
    return RESULT_SET_SCHEMA.toTTableSchema();
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.Operation#getNextRowSet(org.apache.hive.service.cli.FetchOrientation, long)
   */
  @Override
  public TRowSet getNextRowSet(FetchOrientation orientation, long maxRows) throws HiveSQLException {
    assertState(OperationState.FINISHED);
    validateDefaultFetchOrientation(orientation);
    if (orientation.equals(FetchOrientation.FETCH_FIRST)) {
      rowSet.setStartOffset(0);
    }
    return rowSet.extractSubset((int)maxRows).toTRowSet();
  }
}
