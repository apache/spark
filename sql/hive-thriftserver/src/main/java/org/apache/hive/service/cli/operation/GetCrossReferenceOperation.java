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

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.serde2.thrift.Type;
import org.apache.hive.service.cli.*;
import org.apache.hive.service.cli.session.HiveSession;

import java.util.List;

/**
 * GetCrossReferenceOperation.
 *
 */
public class GetCrossReferenceOperation extends MetadataOperation {
  /**
  PKTABLE_CAT String => parent key table catalog (may be null)
  PKTABLE_SCHEM String => parent key table schema (may be null)
  PKTABLE_NAME String => parent key table name
  PKCOLUMN_NAME String => parent key column name
  FKTABLE_CAT String => foreign key table catalog (may be null) being exported (may be null)
  FKTABLE_SCHEM String => foreign key table schema (may be null) being exported (may be null)
  FKTABLE_NAME String => foreign key table name being exported
  FKCOLUMN_NAME String => foreign key column name being exported
  KEY_SEQ short => sequence number within foreign key( a value of 1 represents the first column of the foreign key, a value of 2 would represent the second column within the foreign key).
  UPDATE_RULE short => What happens to foreign key when parent key is updated:
  importedNoAction - do not allow update of parent key if it has been imported
  importedKeyCascade - change imported key to agree with parent key update
  importedKeySetNull - change imported key to NULL if its parent key has been updated
  importedKeySetDefault - change imported key to default values if its parent key has been updated
  importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
  DELETE_RULE short => What happens to the foreign key when parent key is deleted.
  importedKeyNoAction - do not allow delete of parent key if it has been imported
  importedKeyCascade - delete rows that import a deleted key
  importedKeySetNull - change imported key to NULL if its primary key has been deleted
  importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
  importedKeySetDefault - change imported key to default if its parent key has been deleted
  FK_NAME String => foreign key name (may be null)
  PK_NAME String => parent key name (may be null)
  DEFERRABILITY short => can the evaluation of foreign key constraints be deferred until commit
  importedKeyInitiallyDeferred - see SQL92 for definition
  importedKeyInitiallyImmediate - see SQL92 for definition
  importedKeyNotDeferrable - see SQL92 for definition
 */
  private static final TableSchema RESULT_SET_SCHEMA = new TableSchema()
  .addPrimitiveColumn("PKTABLE_CAT", Type.STRING_TYPE,
      "Parent key table catalog (may be null)")
  .addPrimitiveColumn("PKTABLE_SCHEM", Type.STRING_TYPE,
      "Parent key table schema (may be null)")
  .addPrimitiveColumn("PKTABLE_NAME", Type.STRING_TYPE,
      "Parent Key table name")
  .addPrimitiveColumn("PKCOLUMN_NAME", Type.STRING_TYPE,
      "Parent Key column name")
  .addPrimitiveColumn("FKTABLE_CAT", Type.STRING_TYPE,
      "Foreign key table catalog (may be null)")
  .addPrimitiveColumn("FKTABLE_SCHEM", Type.STRING_TYPE,
      "Foreign key table schema (may be null)")
  .addPrimitiveColumn("FKTABLE_NAME", Type.STRING_TYPE,
      "Foreign Key table name")
  .addPrimitiveColumn("FKCOLUMN_NAME", Type.STRING_TYPE,
      "Foreign Key column name")
  .addPrimitiveColumn("KEQ_SEQ", Type.INT_TYPE,
      "Sequence number within primary key")
  .addPrimitiveColumn("UPDATE_RULE", Type.INT_TYPE,
      "What happens to foreign key when parent key is updated")
  .addPrimitiveColumn("DELETE_RULE", Type.INT_TYPE,
      "What happens to foreign key when parent key is deleted")
  .addPrimitiveColumn("FK_NAME", Type.STRING_TYPE,
      "Foreign key name (may be null)")
  .addPrimitiveColumn("PK_NAME", Type.STRING_TYPE,
      "Primary key name (may be null)")
  .addPrimitiveColumn("DEFERRABILITY", Type.INT_TYPE,
      "Can the evaluation of foreign key constraints be deferred until commit");
  private final String parentCatalogName;
  private final String parentSchemaName;
  private final String parentTableName;
  private final String foreignCatalogName;
  private final String foreignSchemaName;
  private final String foreignTableName;
  private final RowSet rowSet;

  public GetCrossReferenceOperation(HiveSession parentSession,
                                    String parentCatalogName, String parentSchemaName, String parentTableName,
                                    String foreignCatalog, String foreignSchema, String foreignTable) {
    super(parentSession, OperationType.GET_FUNCTIONS);
    this.parentCatalogName = parentCatalogName;
    this.parentSchemaName = parentSchemaName;
    this.parentTableName = parentTableName;
    this.foreignCatalogName = foreignCatalog;
    this.foreignSchemaName = foreignSchema;
    this.foreignTableName = foreignTable;
    this.rowSet = RowSetFactory.create(RESULT_SET_SCHEMA, getProtocolVersion(), false);
  }

  @Override
  public void runInternal() throws HiveSQLException {
    setState(OperationState.RUNNING);
    try {
       IMetaStoreClient metastoreClient = getParentSession().getMetaStoreClient();
     ForeignKeysRequest fkReq = new ForeignKeysRequest(parentSchemaName, parentTableName, foreignSchemaName, foreignTableName);
     List<SQLForeignKey> fks = metastoreClient.getForeignKeys(fkReq);
      if (fks == null) {
        return;
      }
      for (SQLForeignKey fk : fks) {
        rowSet.addRow(new Object[] {parentCatalogName,
        fk.getPktable_db(), fk.getPktable_name(), fk.getPkcolumn_name(),
        foreignCatalogName,
        fk.getFktable_db(), fk.getFktable_name(), fk.getFkcolumn_name(),
        fk.getKey_seq(), fk.getUpdate_rule(), fk.getDelete_rule(), fk.getFk_name(),
        fk.getPk_name(), 0});
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
