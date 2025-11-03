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
import org.apache.spark.sql.connect.client.jdbc.SparkConnectDatabaseMetaData._
import org.apache.spark.util.VersionUtils

class SparkConnectDatabaseMetaData(conn: SparkConnectConnection) extends DatabaseMetaData {

  override def allProceduresAreCallable: Boolean = false

  override def allTablesAreSelectable: Boolean = false

  override def getURL: String = conn.url

  override def getUserName: String = conn.spark.client.configuration.userName

  override def isReadOnly: Boolean = false

  override def nullsAreSortedHigh: Boolean = false

  override def nullsAreSortedLow: Boolean = false

  override def nullsAreSortedAtStart: Boolean = false

  override def nullsAreSortedAtEnd: Boolean = false

  override def getDatabaseProductName: String = "Apache Spark Connect Server"

  override def getDatabaseProductVersion: String = conn.spark.version

  override def getDriverName: String = "Apache Spark Connect JDBC Driver"

  override def getDriverVersion: String = SPARK_VERSION

  override def getDriverMajorVersion: Int = VersionUtils.majorVersion(SPARK_VERSION)

  override def getDriverMinorVersion: Int = VersionUtils.minorVersion(SPARK_VERSION)

  override def usesLocalFiles: Boolean = false

  override def usesLocalFilePerTable: Boolean = false

  override def supportsMixedCaseIdentifiers: Boolean = false

  override def storesUpperCaseIdentifiers: Boolean = false

  override def storesLowerCaseIdentifiers: Boolean = false

  override def storesMixedCaseIdentifiers: Boolean = false

  override def supportsMixedCaseQuotedIdentifiers: Boolean = false

  override def storesUpperCaseQuotedIdentifiers: Boolean = false

  override def storesLowerCaseQuotedIdentifiers: Boolean = false

  override def storesMixedCaseQuotedIdentifiers: Boolean = false

  override def getIdentifierQuoteString: String = "`"

  override def getSQLKeywords: String = {
    conn.checkOpen()
    conn.spark.sql("SELECT keyword FROM sql_keywords()").collect()
      .map(_.getString(0)).diff(SQL_2003_RESERVED_KEYWORDS).mkString(",")
  }

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

  override def supportsAlterTableWithAddColumn: Boolean = true

  override def supportsAlterTableWithDropColumn: Boolean = true

  override def supportsColumnAliasing: Boolean = true

  override def nullPlusNonNullIsNull: Boolean = true

  override def supportsConvert: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsConvert(fromType: Int, toType: Int): Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsTableCorrelationNames: Boolean = true

  override def supportsDifferentTableCorrelationNames: Boolean = false

  override def supportsExpressionsInOrderBy: Boolean = true

  override def supportsOrderByUnrelated: Boolean = true

  override def supportsGroupBy: Boolean = true

  override def supportsGroupByUnrelated: Boolean = true

  override def supportsGroupByBeyondSelect: Boolean = true

  override def supportsLikeEscapeClause: Boolean = true

  override def supportsMultipleResultSets: Boolean = false

  override def supportsMultipleTransactions: Boolean = false

  override def supportsNonNullableColumns: Boolean = true

  override def supportsMinimumSQLGrammar: Boolean = true

  override def supportsCoreSQLGrammar: Boolean = true

  override def supportsExtendedSQLGrammar: Boolean = false

  override def supportsANSI92EntryLevelSQL: Boolean = true

  override def supportsANSI92IntermediateSQL(): Boolean = false

  override def supportsANSI92FullSQL: Boolean = false

  override def supportsIntegrityEnhancementFacility: Boolean = false

  override def supportsOuterJoins: Boolean = true

  override def supportsFullOuterJoins: Boolean = true

  override def supportsLimitedOuterJoins: Boolean = true

  override def getSchemaTerm: String = "schema"

  override def getProcedureTerm: String = "procedure"

  override def getCatalogTerm: String = "catalog"

  override def isCatalogAtStart: Boolean = true

  override def getCatalogSeparator: String = "."

  override def supportsSchemasInDataManipulation: Boolean = true

  override def supportsSchemasInProcedureCalls: Boolean = true

  override def supportsSchemasInTableDefinitions: Boolean = true

  override def supportsSchemasInIndexDefinitions: Boolean = true

  override def supportsSchemasInPrivilegeDefinitions: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsCatalogsInDataManipulation: Boolean = true

  override def supportsCatalogsInProcedureCalls: Boolean = true

  override def supportsCatalogsInTableDefinitions: Boolean = true

  override def supportsCatalogsInIndexDefinitions: Boolean = true

  override def supportsCatalogsInPrivilegeDefinitions: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsPositionedDelete: Boolean = false

  override def supportsPositionedUpdate: Boolean = false

  override def supportsSelectForUpdate: Boolean = false

  override def supportsStoredProcedures: Boolean = true

  override def supportsSubqueriesInComparisons: Boolean = true

  override def supportsSubqueriesInExists: Boolean = true

  override def supportsSubqueriesInIns: Boolean = true

  override def supportsSubqueriesInQuantifieds: Boolean = true

  override def supportsCorrelatedSubqueries: Boolean = true

  override def supportsUnion: Boolean = true

  override def supportsUnionAll: Boolean = true

  override def supportsOpenCursorsAcrossCommit: Boolean = false

  override def supportsOpenCursorsAcrossRollback: Boolean = false

  override def supportsOpenStatementsAcrossCommit: Boolean = false

  override def supportsOpenStatementsAcrossRollback: Boolean = false

  override def getMaxBinaryLiteralLength: Int = 0

  override def getMaxCharLiteralLength: Int = 0

  override def getMaxColumnNameLength: Int = 0

  override def getMaxColumnsInGroupBy: Int = 0

  override def getMaxColumnsInIndex: Int = 0

  override def getMaxColumnsInOrderBy: Int = 0

  override def getMaxColumnsInSelect: Int = 0

  override def getMaxColumnsInTable: Int = 0

  override def getMaxConnections: Int = 0

  override def getMaxCursorNameLength: Int = 0

  override def getMaxIndexLength: Int = 0

  override def getMaxSchemaNameLength: Int = 0

  override def getMaxProcedureNameLength: Int = 0

  override def getMaxCatalogNameLength: Int = 0

  override def getMaxRowSize: Int = 0

  override def doesMaxRowSizeIncludeBlobs: Boolean = false

  override def getMaxStatementLength: Int = 0

  override def getMaxStatements: Int = 0

  override def getMaxTableNameLength: Int = 0

  override def getMaxTablesInSelect: Int = 0

  override def getMaxUserNameLength: Int = 0

  override def getDefaultTransactionIsolation: Int = Connection.TRANSACTION_NONE

  override def supportsTransactions: Boolean = false

  override def supportsTransactionIsolationLevel(level: Int): Boolean =
    level == Connection.TRANSACTION_NONE

  override def supportsDataDefinitionAndDataManipulationTransactions: Boolean = false

  override def supportsDataManipulationTransactionsOnly: Boolean = false

  override def dataDefinitionCausesTransactionCommit: Boolean = false

  override def dataDefinitionIgnoredInTransactions: Boolean = false

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
    `type` == ResultSet.TYPE_FORWARD_ONLY

  override def supportsResultSetConcurrency(`type`: Int, concurrency: Int): Boolean =
    `type` == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY

  override def ownUpdatesAreVisible(`type`: Int): Boolean = false

  override def ownDeletesAreVisible(`type`: Int): Boolean = false

  override def ownInsertsAreVisible(`type`: Int): Boolean = false

  override def othersUpdatesAreVisible(`type`: Int): Boolean = false

  override def othersDeletesAreVisible(`type`: Int): Boolean = false

  override def othersInsertsAreVisible(`type`: Int): Boolean = false

  override def updatesAreDetected(`type`: Int): Boolean = false

  override def deletesAreDetected(`type`: Int): Boolean = false

  override def insertsAreDetected(`type`: Int): Boolean = false

  override def supportsBatchUpdates: Boolean = false

  override def getUDTs(
      catalog: String,
      schemaPattern: String,
      typeNamePattern: String,
      types: Array[Int]): ResultSet =
    throw new SQLFeatureNotSupportedException

  override def getConnection: Connection = conn

  override def supportsSavepoints: Boolean = false

  override def supportsNamedParameters: Boolean =
    throw new SQLFeatureNotSupportedException

  override def supportsMultipleOpenResults: Boolean = false

  override def supportsGetGeneratedKeys: Boolean = false

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

  override def getSQLStateType: Int = DatabaseMetaData.sqlStateSQL

  override def locatorsUpdateCopy: Boolean = false

  override def supportsStatementPooling: Boolean = false

  override def getRowIdLifetime: RowIdLifetime = RowIdLifetime.ROWID_UNSUPPORTED

  override def supportsStoredFunctionsUsingCallSyntax: Boolean = false

  override def autoCommitFailureClosesAllResultSets: Boolean = false

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

object SparkConnectDatabaseMetaData {

  // SQL:2003 reserved keywords refers to PostgreSQL 9.1 docs:
  // https://www.postgresql.org/docs/9.1/sql-keywords-appendix.html
  private[jdbc] val SQL_2003_RESERVED_KEYWORDS = Array(
    "ABS", "ALL", "ALLOCATE", "ALTER", "AND", "ANY", "ARE", "ARRAY", "AS", "ASENSITIVE",
    "ASYMMETRIC", "AT", "ATOMIC", "AUTHORIZATION", "AVG",
    "BEGIN", "BETWEEN", "BIGINT", "BINARY", "BLOB", "BOOLEAN", "BOTH", "BY",
    "CALL", "CALLED", "CARDINALITY", "CASCADED", "CASE", "CAST", "CEIL", "CEILING", "CHAR",
    "CHARACTER", "CHARACTER_LENGTH", "CHAR_LENGTH", "CHECK", "CLOB", "CLOSE", "COALESCE",
    "COLLATE", "COLLECT", "COLUMN", "COMMIT", "CONDITION", "CONNECT", "CONSTRAINT", "CONVERT",
    "CORR", "CORRESPONDING", "COUNT", "COVAR_POP", "COVAR_SAMP", "CREATE", "CROSS", "CUBE",
    "CUME_DIST", "CURRENT", "CURRENT_DATE", "CURRENT_DEFAULT_TRANSFORM_GROUP", "CURRENT_PATH",
    "CURRENT_ROLE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_TRANSFORM_GROUP_FOR_TYPE",
    "CURRENT_USER", "CURSOR", "CYCLE",
    "DATALINK", "DATE", "DAY", "DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DELETE",
    "DENSE_RANK", "DEREF", "DESCRIBE", "DETERMINISTIC", "DISCONNECT", "DISTINCT", "DLNEWCOPY",
    "DLPREVIOUSCOPY", "DLURLCOMPLETE", "DLURLCOMPLETEONLY", "DLURLCOMPLETEWRITE", "DLURLPATH",
    "DLURLPATHONLY", "DLURLPATHWRITE", "DLURLSCHEME", "DLURLSERVER", "DLVALUE", "DOUBLE",
    "DROP", "DYNAMIC",
    "EACH", "ELEMENT", "ELSE", "END", "END-EXEC", "ESCAPE", "EVERY", "EXCEPT", "EXEC",
    "EXECUTE", "EXISTS", "EXP", "EXTERNAL", "EXTRACT",
    "FALSE", "FETCH", "FILTER", "FLOAT", "FLOOR", "FOR", "FOREIGN", "FREE", "FROM", "FULL",
    "FUNCTION", "FUSION",
    "GET", "GLOBAL", "GRANT", "GROUP", "GROUPING",
    "HAVING", "HOLD", "HOUR",
    "IDENTITY", "IMPORT", "IN", "INDICATOR", "INNER", "INOUT", "INSENSITIVE", "INSERT", "INT",
    "INTEGER", "INTERSECT", "INTERSECTION", "INTERVAL", "INTO", "IS",
    "JOIN",
    "LANGUAGE", "LARGE", "LATERAL", "LEADING", "LEFT", "LIKE", "LN", "LOCAL", "LOCALTIME",
    "LOCALTIMESTAMP", "LOWER",
    "MATCH", "MAX", "MEMBER", "MERGE", "METHOD", "MIN", "MINUTE", "MOD", "MODIFIES", "MODULE",
    "MONTH", "MULTISET",
    "NATIONAL", "NATURAL", "NCHAR", "NCLOB", "NEW", "NO", "NONE", "NORMALIZE", "NOT", "NULL",
    "NULLIF", "NUMERIC",
    "OCTET_LENGTH", "OF", "OLD", "ON", "ONLY", "OPEN", "OR", "ORDER", "OUT", "OUTER", "OVER",
    "OVERLAPS", "OVERLAY",
    "PARAMETER", "PARTITION", "PERCENTILE_CONT", "PERCENTILE_DISC", "PERCENT_RANK", "POSITION",
    "POWER", "PRECISION", "PREPARE", "PRIMARY", "PROCEDURE",
    "RANGE", "RANK", "READS", "REAL", "RECURSIVE", "REF", "REFERENCES", "REFERENCING",
    "REGR_AVGX", "REGR_AVGY", "REGR_COUNT", "REGR_INTERCEPT", "REGR_R2", "REGR_SLOPE",
    "REGR_SXX", "REGR_SXY", "REGR_SYY", "RELEASE", "RESULT", "RETURN", "RETURNS", "REVOKE",
    "RIGHT", "ROLLBACK", "ROLLUP", "ROW", "ROWS", "ROW_NUMBER",
    "SAVEPOINT", "SCOPE", "SCROLL", "SEARCH", "SECOND", "SELECT", "SENSITIVE", "SESSION_USER",
    "SET", "SIMILAR", "SMALLINT", "SOME", "SPECIFIC", "SPECIFICTYPE", "SQL", "SQLEXCEPTION",
    "SQLSTATE", "SQLWARNING", "SQRT", "START", "STATIC", "STDDEV_POP", "STDDEV_SAMP",
    "SUBMULTISET", "SUBSTRING", "SUM", "SYMMETRIC", "SYSTEM", "SYSTEM_USER",
    "TABLE", "TABLESAMPLE", "THEN", "TIME", "TIMESTAMP", "TIMEZONE_HOUR", "TIMEZONE_MINUTE",
    "TO", "TRAILING", "TRANSLATE", "TRANSLATION", "TREAT", "TRIGGER", "TRIM", "TRUE",
    "UESCAPE", "UNION", "UNIQUE", "UNKNOWN", "UNNEST", "UPDATE", "UPPER", "USER", "USING",
    "VALUE", "VALUES", "VARCHAR", "VARYING", "VAR_POP", "VAR_SAMP",
    "WHEN", "WHENEVER", "WHERE", "WIDTH_BUCKET", "WINDOW", "WITH", "WITHIN", "WITHOUT",
    "XML", "XMLAGG", "XMLATTRIBUTES", "XMLBINARY", "XMLCOMMENT", "XMLCONCAT", "XMLELEMENT",
    "XMLFOREST", "XMLNAMESPACES", "XMLPARSE", "XMLPI", "XMLROOT", "XMLSERIALIZE",
    "YEAR"
  )
}
