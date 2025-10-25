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

package org.apache.spark.sql.connect.client.jdbc

import java.sql.{Array => _, _}

import org.apache.spark.SparkBuildInfo.{spark_version => SPARK_VERSION}
import org.apache.spark.util.VersionUtils

class SparkConnectDatabaseMetaData(conn: SparkConnectConnection) extends DatabaseMetaData {

  override def allProceduresAreCallable: Boolean =
    throw new SQLFeatureNotSupportedException

  override def allTablesAreSelectable: Boolean =
    throw new SQLFeatureNotSupportedException

  override def getURL: String = conn.url

  override def getUserName: String = conn.spark.client.configuration.userName

  override def isReadOnly: Boolean = false

  override def nullsAreSortedHigh: Boolean =
    throw new SQLFeatureNotSupportedException

  override def nullsAreSortedLow: Boolean =
    throw new SQLFeatureNotSupportedException

  override def nullsAreSortedAtStart: Boolean =
    throw new SQLFeatureNotSupportedException

  override def nullsAreSortedAtEnd: Boolean =
    throw new SQLFeatureNotSupportedException

  override def getDatabaseProductName: String = "Apache Spark Connect Server"

  override def getDatabaseProductVersion: String = conn.spark.version

  override def getDriverName: String = "Apache Spark Connect JDBC Driver"

  override def getDriverVersion: String = SPARK_VERSION

  override def getDriverMajorVersion: Int = VersionUtils.majorVersion(SPARK_VERSION)

  override def getDriverMinorVersion: Int = VersionUtils.minorVersion(SPARK_VERSION)

  override def usesLocalFiles: Boolean =
    throw new SQLFeatureNotSupportedException

  override def usesLocalFilePerTable: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsMixedCaseIdentifiers: Boolean =
    throw new SQLFeatureNotSupportedException

  override def storesUpperCaseIdentifiers: Boolean =
    throw new SQLFeatureNotSupportedException

  override def storesLowerCaseIdentifiers: Boolean =
    throw new SQLFeatureNotSupportedException

  override def storesMixedCaseIdentifiers: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsMixedCaseQuotedIdentifiers: Boolean =
    throw new SQLFeatureNotSupportedException

  override def storesUpperCaseQuotedIdentifiers: Boolean =
    throw new SQLFeatureNotSupportedException

  override def storesLowerCaseQuotedIdentifiers: Boolean =
    throw new SQLFeatureNotSupportedException

  override def storesMixedCaseQuotedIdentifiers: Boolean =
    throw new SQLFeatureNotSupportedException

  override def getIdentifierQuoteString: String = "`"

  override def getSQLKeywords: String =
    throw new SQLFeatureNotSupportedException

  override def getNumericFunctions: String =
    throw new SQLFeatureNotSupportedException

  override def getStringFunctions: String =
    throw new SQLFeatureNotSupportedException

  override def getSystemFunctions: String =
    throw new SQLFeatureNotSupportedException

  override def getTimeDateFunctions: String =
    throw new SQLFeatureNotSupportedException

  override def getSearchStringEscape: String =
    throw new SQLFeatureNotSupportedException

  override def getExtraNameCharacters: String = ""

  override def supportsAlterTableWithAddColumn: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsAlterTableWithDropColumn: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsColumnAliasing: Boolean =
    throw new SQLFeatureNotSupportedException

  override def nullPlusNonNullIsNull: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsConvert: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsConvert(fromType: Int, toType: Int): Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsTableCorrelationNames: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsDifferentTableCorrelationNames: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsExpressionsInOrderBy: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsOrderByUnrelated: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsGroupBy: Boolean = true

  override def supportsGroupByUnrelated: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsGroupByBeyondSelect: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsLikeEscapeClause: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsMultipleResultSets: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsMultipleTransactions: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsNonNullableColumns: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsMinimumSQLGrammar: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsCoreSQLGrammar: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsExtendedSQLGrammar: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsANSI92EntryLevelSQL: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsANSI92IntermediateSQL: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsANSI92FullSQL: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsIntegrityEnhancementFacility: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsOuterJoins: Boolean = true

  override def supportsFullOuterJoins: Boolean = true

  override def supportsLimitedOuterJoins: Boolean = true

  override def getSchemaTerm: String = "schema"

  override def getProcedureTerm: String = "procedure"

  override def getCatalogTerm: String = "catalog"

  override def isCatalogAtStart: Boolean = true

  override def getCatalogSeparator: String = "."

  override def supportsSchemasInDataManipulation: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsSchemasInProcedureCalls: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsSchemasInTableDefinitions: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsSchemasInIndexDefinitions: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsSchemasInPrivilegeDefinitions: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsCatalogsInDataManipulation: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsCatalogsInProcedureCalls: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsCatalogsInTableDefinitions: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsCatalogsInIndexDefinitions: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsCatalogsInPrivilegeDefinitions: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsPositionedDelete: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsPositionedUpdate: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsSelectForUpdate: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsStoredProcedures: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsSubqueriesInComparisons: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsSubqueriesInExists: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsSubqueriesInIns: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsSubqueriesInQuantifieds: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsCorrelatedSubqueries: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsUnion: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsUnionAll: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsOpenCursorsAcrossCommit: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsOpenCursorsAcrossRollback: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsOpenStatementsAcrossCommit: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsOpenStatementsAcrossRollback: Boolean =
    throw new SQLFeatureNotSupportedException

  override def getMaxBinaryLiteralLength: Int =
    throw new SQLFeatureNotSupportedException

  override def getMaxCharLiteralLength: Int =
    throw new SQLFeatureNotSupportedException

  override def getMaxColumnNameLength: Int =
    throw new SQLFeatureNotSupportedException

  override def getMaxColumnsInGroupBy: Int =
    throw new SQLFeatureNotSupportedException

  override def getMaxColumnsInIndex: Int =
    throw new SQLFeatureNotSupportedException

  override def getMaxColumnsInOrderBy: Int =
    throw new SQLFeatureNotSupportedException

  override def getMaxColumnsInSelect: Int =
    throw new SQLFeatureNotSupportedException

  override def getMaxColumnsInTable: Int =
    throw new SQLFeatureNotSupportedException

  override def getMaxConnections: Int =
    throw new SQLFeatureNotSupportedException

  override def getMaxCursorNameLength: Int =
    throw new SQLFeatureNotSupportedException

  override def getMaxIndexLength: Int =
    throw new SQLFeatureNotSupportedException

  override def getMaxSchemaNameLength: Int =
    throw new SQLFeatureNotSupportedException

  override def getMaxProcedureNameLength: Int =
    throw new SQLFeatureNotSupportedException

  override def getMaxCatalogNameLength: Int =
    throw new SQLFeatureNotSupportedException

  override def getMaxRowSize: Int =
    throw new SQLFeatureNotSupportedException

  override def doesMaxRowSizeIncludeBlobs: Boolean =
    throw new SQLFeatureNotSupportedException

  override def getMaxStatementLength: Int =
    throw new SQLFeatureNotSupportedException

  override def getMaxStatements: Int =
    throw new SQLFeatureNotSupportedException

  override def getMaxTableNameLength: Int =
    throw new SQLFeatureNotSupportedException

  override def getMaxTablesInSelect: Int =
    throw new SQLFeatureNotSupportedException

  override def getMaxUserNameLength: Int =
    throw new SQLFeatureNotSupportedException

  override def getDefaultTransactionIsolation: Int =
    throw new SQLFeatureNotSupportedException

  override def supportsTransactions: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsTransactionIsolationLevel(level: Int): Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsDataDefinitionAndDataManipulationTransactions: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsDataManipulationTransactionsOnly: Boolean =
    throw new SQLFeatureNotSupportedException

  override def dataDefinitionCausesTransactionCommit: Boolean =
    throw new SQLFeatureNotSupportedException

  override def dataDefinitionIgnoredInTransactions: Boolean =
    throw new SQLFeatureNotSupportedException

  override def getProcedures(
      catalog: String,
      schemaPattern: String,
      procedureNamePattern: String): ResultSet =
    throw new SQLFeatureNotSupportedException

  override def getProcedureColumns(
      catalog: String,
      schemaPattern: String,
      procedureNamePattern: String,
      columnNamePattern: String): ResultSet =
    throw new SQLFeatureNotSupportedException

  override def getCatalogs: ResultSet =
    throw new SQLFeatureNotSupportedException

  override def getSchemas: ResultSet =
    throw new SQLFeatureNotSupportedException

  override def getSchemas(catalog: String, schemaPattern: String): ResultSet =
    throw new SQLFeatureNotSupportedException

  override def getTableTypes: ResultSet =
    throw new SQLFeatureNotSupportedException

  override def getTables(
      catalog: String,
      schemaPattern: String,
      tableNamePattern: String,
      types: Array[String]): ResultSet =
    throw new SQLFeatureNotSupportedException

  override def getColumns(
      catalog: String,
      schemaPattern: String,
      tableNamePattern: String,
      columnNamePattern: String): ResultSet =
    throw new SQLFeatureNotSupportedException

  override def getColumnPrivileges(
      catalog: String,
      schema: String,
      table: String,
      columnNamePattern: String): ResultSet =
    throw new SQLFeatureNotSupportedException

  override def getTablePrivileges(
      catalog: String,
      schemaPattern: String,
      tableNamePattern: String): ResultSet =
    throw new SQLFeatureNotSupportedException

  override def getBestRowIdentifier(
      catalog: String,
      schema: String,
      table: String,
      scope: Int,
      nullable: Boolean): ResultSet =
    throw new SQLFeatureNotSupportedException

  override def getVersionColumns(
      catalog: String, schema: String, table: String): ResultSet =
    throw new SQLFeatureNotSupportedException

  override def getPrimaryKeys(catalog: String, schema: String, table: String): ResultSet =
    throw new SQLFeatureNotSupportedException

  override def getImportedKeys(catalog: String, schema: String, table: String): ResultSet =
    throw new SQLFeatureNotSupportedException

  override def getExportedKeys(catalog: String, schema: String, table: String): ResultSet =
    throw new SQLFeatureNotSupportedException

  override def getCrossReference(
      parentCatalog: String,
      parentSchema: String,
      parentTable: String,
      foreignCatalog: String,
      foreignSchema: String,
      foreignTable: String): ResultSet =
    throw new SQLFeatureNotSupportedException

  override def getTypeInfo: ResultSet =
    throw new SQLFeatureNotSupportedException

  override def getIndexInfo(
      catalog: String,
      schema: String,
      table: String,
      unique: Boolean,
      approximate: Boolean): ResultSet =
    throw new SQLFeatureNotSupportedException

  override def supportsResultSetType(`type`: Int): Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsResultSetConcurrency(`type`: Int, concurrency: Int): Boolean =
    throw new SQLFeatureNotSupportedException

  override def ownUpdatesAreVisible(`type`: Int): Boolean =
    throw new SQLFeatureNotSupportedException

  override def ownDeletesAreVisible(`type`: Int): Boolean =
    throw new SQLFeatureNotSupportedException

  override def ownInsertsAreVisible(`type`: Int): Boolean =
    throw new SQLFeatureNotSupportedException

  override def othersUpdatesAreVisible(`type`: Int): Boolean =
    throw new SQLFeatureNotSupportedException

  override def othersDeletesAreVisible(`type`: Int): Boolean =
    throw new SQLFeatureNotSupportedException

  override def othersInsertsAreVisible(`type`: Int): Boolean =
    throw new SQLFeatureNotSupportedException

  override def updatesAreDetected(`type`: Int): Boolean =
    throw new SQLFeatureNotSupportedException

  override def deletesAreDetected(`type`: Int): Boolean =
    throw new SQLFeatureNotSupportedException

  override def insertsAreDetected(`type`: Int): Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsBatchUpdates: Boolean =
    throw new SQLFeatureNotSupportedException

  override def getUDTs(
      catalog: String,
      schemaPattern: String,
      typeNamePattern: String,
      types: Array[Int]): ResultSet =
    throw new SQLFeatureNotSupportedException

  override def getConnection: Connection = conn

  override def supportsSavepoints: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsNamedParameters: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsMultipleOpenResults: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsGetGeneratedKeys: Boolean =
    throw new SQLFeatureNotSupportedException

  override def getSuperTypes(
      catalog: String,
      schemaPattern: String,
      typeNamePattern: String): ResultSet =
    throw new SQLFeatureNotSupportedException

  override def getSuperTables(
      catalog: String,
      schemaPattern: String,
      tableNamePattern: String): ResultSet =
    throw new SQLFeatureNotSupportedException

  override def getAttributes(
      catalog: String,
      schemaPattern: String,
      typeNamePattern: String,
      attributeNamePattern: String): ResultSet =
    throw new SQLFeatureNotSupportedException

  override def supportsResultSetHoldability(holdability: Int): Boolean =
    holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT

  override def getResultSetHoldability: Int = ResultSet.CLOSE_CURSORS_AT_COMMIT

  override def getDatabaseMajorVersion: Int = VersionUtils.majorVersion(conn.spark.version)

  override def getDatabaseMinorVersion: Int = VersionUtils.minorVersion(conn.spark.version)

  // JSR-221 defines JDBC 4.0 API Specification - https://jcp.org/en/jsr/detail?id=221
  // JDBC 4.3 is the latest Maintenance version of the JDBC 4.0 specification as of JDK 17
  // https://docs.oracle.com/en/java/javase/17/docs/api/java.sql/java/sql/package-summary.html
  override def getJDBCMajorVersion: Int = 4

  override def getJDBCMinorVersion: Int = 3

  override def getSQLStateType: Int =
    throw new SQLFeatureNotSupportedException

  override def locatorsUpdateCopy: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsStatementPooling: Boolean = false

  override def getRowIdLifetime: RowIdLifetime = RowIdLifetime.ROWID_UNSUPPORTED

  override def supportsStoredFunctionsUsingCallSyntax: Boolean =
    throw new SQLFeatureNotSupportedException

  override def autoCommitFailureClosesAllResultSets: Boolean =
    throw new SQLFeatureNotSupportedException

  override def getClientInfoProperties: ResultSet =
    throw new SQLFeatureNotSupportedException

  override def getFunctions(
      catalog: String,
      schemaPattern: String,
      functionNamePattern: String): ResultSet =
    throw new SQLFeatureNotSupportedException

  override def getFunctionColumns(
      catalog: String,
      schemaPattern: String,
      functionNamePattern: String,
      columnNamePattern: String): ResultSet =
    throw new SQLFeatureNotSupportedException

  override def getPseudoColumns(
      catalog: String,
      schemaPattern: String,
      tableNamePattern: String,
      columnNamePattern: String): ResultSet =
    throw new SQLFeatureNotSupportedException

  override def generatedKeyAlwaysReturned: Boolean = false

  override def getMaxLogicalLobSize: Long = 0

  override def supportsRefCursors: Boolean = false

  override def supportsSharding: Boolean = false

  override def unwrap[T](iface: Class[T]): T = if (isWrapperFor(iface)) {
    iface.asInstanceOf[T]
  } else {
    throw new SQLException(s"${this.getClass.getName} not unwrappable from ${iface.getName}")
  }

  override def isWrapperFor(iface: Class[_]): Boolean = iface.isInstance(this)
}
