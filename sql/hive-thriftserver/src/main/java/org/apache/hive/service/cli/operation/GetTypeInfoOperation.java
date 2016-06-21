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

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.OperationType;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.Type;
import org.apache.hive.service.cli.session.HiveSession;

/**
 * GetTypeInfoOperation.
 *
 */
public class GetTypeInfoOperation extends MetadataOperation {

  private static final TableSchema RESULT_SET_SCHEMA = new TableSchema()
  .addPrimitiveColumn("TYPE_NAME", Type.STRING_TYPE,
      "Type name")
  .addPrimitiveColumn("DATA_TYPE", Type.INT_TYPE,
      "SQL data type from java.sql.Types")
  .addPrimitiveColumn("PRECISION", Type.INT_TYPE,
      "Maximum precision")
  .addPrimitiveColumn("LITERAL_PREFIX", Type.STRING_TYPE,
      "Prefix used to quote a literal (may be null)")
  .addPrimitiveColumn("LITERAL_SUFFIX", Type.STRING_TYPE,
      "Suffix used to quote a literal (may be null)")
  .addPrimitiveColumn("CREATE_PARAMS", Type.STRING_TYPE,
      "Parameters used in creating the type (may be null)")
  .addPrimitiveColumn("NULLABLE", Type.SMALLINT_TYPE,
      "Can you use NULL for this type")
  .addPrimitiveColumn("CASE_SENSITIVE", Type.BOOLEAN_TYPE,
      "Is it case sensitive")
  .addPrimitiveColumn("SEARCHABLE", Type.SMALLINT_TYPE,
      "Can you use \"WHERE\" based on this type")
  .addPrimitiveColumn("UNSIGNED_ATTRIBUTE", Type.BOOLEAN_TYPE,
      "Is it unsigned")
  .addPrimitiveColumn("FIXED_PREC_SCALE", Type.BOOLEAN_TYPE,
      "Can it be a money value")
  .addPrimitiveColumn("AUTO_INCREMENT", Type.BOOLEAN_TYPE,
      "Can it be used for an auto-increment value")
  .addPrimitiveColumn("LOCAL_TYPE_NAME", Type.STRING_TYPE,
      "Localized version of type name (may be null)")
  .addPrimitiveColumn("MINIMUM_SCALE", Type.SMALLINT_TYPE,
      "Minimum scale supported")
  .addPrimitiveColumn("MAXIMUM_SCALE", Type.SMALLINT_TYPE,
      "Maximum scale supported")
  .addPrimitiveColumn("SQL_DATA_TYPE", Type.INT_TYPE,
      "Unused")
  .addPrimitiveColumn("SQL_DATETIME_SUB", Type.INT_TYPE,
      "Unused")
  .addPrimitiveColumn("NUM_PREC_RADIX", Type.INT_TYPE,
      "Usually 2 or 10");

  private final RowSet rowSet;

  protected GetTypeInfoOperation(HiveSession parentSession) {
    super(parentSession, OperationType.GET_TYPE_INFO);
    rowSet = RowSetFactory.create(RESULT_SET_SCHEMA, getProtocolVersion());
  }

  @Override
  public void runInternal() throws HiveSQLException {
    setState(OperationState.RUNNING);
    if (isAuthV2Enabled()) {
      authorizeMetaGets(HiveOperationType.GET_TYPEINFO, null);
    }
    try {
      for (Type type : Type.values()) {
        Object[] rowData = new Object[] {
            type.getName(), // TYPE_NAME
            type.toJavaSQLType(), // DATA_TYPE
            type.getMaxPrecision(), // PRECISION
            type.getLiteralPrefix(), // LITERAL_PREFIX
            type.getLiteralSuffix(), // LITERAL_SUFFIX
            type.getCreateParams(), // CREATE_PARAMS
            type.getNullable(), // NULLABLE
            type.isCaseSensitive(), // CASE_SENSITIVE
            type.getSearchable(), // SEARCHABLE
            type.isUnsignedAttribute(), // UNSIGNED_ATTRIBUTE
            type.isFixedPrecScale(), // FIXED_PREC_SCALE
            type.isAutoIncrement(), // AUTO_INCREMENT
            type.getLocalizedName(), // LOCAL_TYPE_NAME
            type.getMinimumScale(), // MINIMUM_SCALE
            type.getMaximumScale(), // MAXIMUM_SCALE
            null, // SQL_DATA_TYPE, unused
            null, // SQL_DATETIME_SUB, unused
            type.getNumPrecRadix() //NUM_PREC_RADIX
        };
        rowSet.addRow(rowData);
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
