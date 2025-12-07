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
import java.sql.DatabaseMetaData._

import org.apache.spark.SparkBuildInfo.{spark_version => SPARK_VERSION}
import org.apache.spark.SparkThrowable
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.util.QuotingUtils._
import org.apache.spark.sql.connect
import org.apache.spark.sql.connect.client.jdbc.SparkConnectDatabaseMetaData._
import org.apache.spark.sql.connect.client.jdbc.util.JdbcTypeUtils
import org.apache.spark.sql.functions._
import org.apache.spark.util.VersionUtils

class SparkConnectDatabaseMetaData(conn: SparkConnectConnection) extends DatabaseMetaData {

  import conn.spark.implicits._

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

  override def getSearchStringEscape: String = "\\"

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

  private def isNullOrWildcard(pattern: String): Boolean =
    pattern == null || pattern == "%"

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

  override def getCatalogs: ResultSet = {
    conn.checkOpen()

    val df = conn.spark.sql("SHOW CATALOGS")
      .select($"catalog".as("TABLE_CAT"))
      .orderBy("TABLE_CAT")
    new SparkConnectResultSet(df.collectResult())
  }

  override def getSchemas: ResultSet = {
    conn.checkOpen()

    getSchemas(null, null)
  }

  // Schema of the returned DataFrame is:
  // |-- TABLE_SCHEM: string (nullable = false)
  // |-- TABLE_CATALOG: string (nullable = false)
  private def getSchemasDataFrame(
      catalog: String, schemaPatternOpt: Option[String]): connect.DataFrame = {

    val schemaFilterExpr = schemaPatternOpt match {
      case None => $"TABLE_SCHEM".equalTo(conn.spark.catalog.currentDatabase)
      case Some(schemaPattern) if isNullOrWildcard(schemaPattern) => lit(true)
      case Some(schemaPattern) => $"TABLE_SCHEM".like(schemaPattern)
    }

    lazy val emptyDf = conn.spark.emptyDataFrame
      .withColumn("TABLE_SCHEM", lit(""))
      .withColumn("TABLE_CATALOG", lit(""))

    def internalGetSchemas(
        catalogOpt: Option[String],
        schemaFilterExpr: Column): connect.DataFrame = {
      val catalog = catalogOpt.getOrElse(conn.getCatalog)
      try {
        // Spark SQL supports LIKE clause in SHOW SCHEMAS command, but we can't use that
        // because the LIKE pattern does not follow SQL standard.
        conn.spark.sql(s"SHOW SCHEMAS IN ${quoteIdentifier(catalog)}")
          .select($"namespace".as("TABLE_SCHEM"))
          .filter(schemaFilterExpr)
          .withColumn("TABLE_CATALOG", lit(catalog))
      } catch {
        case st: SparkThrowable if st.getCondition == "MISSING_CATALOG_ABILITY.NAMESPACES" =>
          emptyDf
      }
    }

    if (catalog == null) {
      // search in all catalogs
      conn.spark.catalog.listCatalogs().collect().map(_.name).map { c =>
        internalGetSchemas(Some(c), schemaFilterExpr)
      }.fold(emptyDf) { (l, r) => l.unionAll(r) }
    } else if (catalog == "") {
      // search only in current catalog
      internalGetSchemas(None, schemaFilterExpr)
    } else {
      // search in the specific catalog
      internalGetSchemas(Some(catalog), schemaFilterExpr)
    }
  }

  override def getSchemas(catalog: String, schemaPattern: String): ResultSet = {
    conn.checkOpen()

    val df = getSchemasDataFrame(catalog, Some(schemaPattern))
      .orderBy("TABLE_CATALOG", "TABLE_SCHEM")
    new SparkConnectResultSet(df.collectResult())
  }

  override def getTableTypes: ResultSet = {
    conn.checkOpen()

    val df = TABLE_TYPES.toDF("TABLE_TYPE")
      .orderBy("TABLE_TYPE")
    new SparkConnectResultSet(df.collectResult())
  }

  // Schema of the returned DataFrame is:
  // |-- TABLE_CAT: string (nullable = false)
  // |-- TABLE_SCHEM: string (nullable = false)
  // |-- TABLE_NAME: string (nullable = false)
  // |-- TABLE_TYPE: string (nullable = false)
  // |-- REMARKS: string (nullable = false)
  // |-- TYPE_CAT: string (nullable = false)
  // |-- TYPE_SCHEM: string (nullable = false)
  // |-- TYPE_NAME: string (nullable = false)
  // |-- SELF_REFERENCING_COL_NAME: string (nullable = false)
  // |-- REF_GENERATION: string (nullable = false)
  private def getTablesDataFrame(
      catalog: String,
      schemaPattern: String,
      tableNamePattern: String): connect.DataFrame = {

    val catalogSchemasDf = if (schemaPattern == "") {
      getSchemasDataFrame(catalog, None)
    } else {
      getSchemasDataFrame(catalog, Some(schemaPattern))
    }

    val catalogSchemas = catalogSchemasDf.collect()
      .map { row => (row.getString(1), row.getString(0)) }

    val tableNameFilterExpr = if (isNullOrWildcard(tableNamePattern)) {
      lit(true)
    } else {
      $"TABLE_NAME".like(tableNamePattern)
    }

    lazy val emptyDf = conn.spark.emptyDataFrame
      .withColumn("TABLE_CAT", lit(""))
      .withColumn("TABLE_SCHEM", lit(""))
      .withColumn("TABLE_NAME", lit(""))
      .withColumn("TABLE_TYPE", lit(""))
      .withColumn("REMARKS", lit(""))
      .withColumn("TYPE_CAT", lit(""))
      .withColumn("TYPE_SCHEM", lit(""))
      .withColumn("TYPE_NAME", lit(""))
      .withColumn("SELF_REFERENCING_COL_NAME", lit(""))
      .withColumn("REF_GENERATION", lit(""))

    catalogSchemas.map { case (catalog, schema) =>
      val viewDf = try {
        conn.spark
          .sql(s"SHOW VIEWS IN ${quoteNameParts(Seq(catalog, schema))}")
          .select($"namespace".as("TABLE_SCHEM"), $"viewName".as("TABLE_NAME"))
          .filter(tableNameFilterExpr)
      } catch {
        case st: SparkThrowable if st.getCondition == "MISSING_CATALOG_ABILITY.VIEWS" =>
          emptyDf.select("TABLE_SCHEM", "TABLE_NAME")
      }

      val tableDf = try {
        conn.spark
          .sql(s"SHOW TABLES IN ${quoteNameParts(Seq(catalog, schema))}")
          .select($"namespace".as("TABLE_SCHEM"), $"tableName".as("TABLE_NAME"))
          .filter(tableNameFilterExpr)
          .exceptAll(viewDf)
      } catch {
        case st: SparkThrowable if st.getCondition == "MISSING_CATALOG_ABILITY.TABLES" =>
          emptyDf.select("TABLE_SCHEM", "TABLE_NAME")
      }

      tableDf.withColumn("TABLE_TYPE", lit("TABLE"))
        .unionAll(viewDf.withColumn("TABLE_TYPE", lit("VIEW")))
        .withColumn("TABLE_CAT", lit(catalog))
        .withColumn("REMARKS", lit(""))
        .withColumn("TYPE_CAT", lit(""))
        .withColumn("TYPE_SCHEM", lit(""))
        .withColumn("TYPE_NAME", lit(""))
        .withColumn("SELF_REFERENCING_COL_NAME", lit(""))
        .withColumn("REF_GENERATION", lit(""))
        .select("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS",
          "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME",
          "REF_GENERATION")
    }.fold(emptyDf) { (l, r) => l.unionAll(r) }
  }

  override def getTables(
      catalog: String,
      schemaPattern: String,
      tableNamePattern: String,
      types: Array[String]): ResultSet = {
    conn.checkOpen()

    if (types != null) {
      val unsupported = types.diff(TABLE_TYPES)
      if (unsupported.nonEmpty) {
        throw new SQLException(
          "The requested table types contains unsupported items: " +
            s"${unsupported.mkString(", ")}. Available table types are: " +
            s"${TABLE_TYPES.mkString(", ")}.")
      }
    }

    var df = getTablesDataFrame(catalog, schemaPattern, tableNamePattern)
      .orderBy("TABLE_TYPE", "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME")

    if (types != null) {
      df = df.filter($"TABLE_TYPE".isInCollection(types))
    }
    new SparkConnectResultSet(df.collectResult())
  }

  override def getColumns(
      catalog: String,
      schemaPattern: String,
      tableNamePattern: String,
      columnNamePattern: String): ResultSet = {
    conn.checkOpen()

    val columnNameFilterExpr = if (isNullOrWildcard(columnNamePattern)) {
      lit(true)
    } else {
      $"COLUMN_NAME".like(columnNamePattern)
    }

    lazy val emptyDf = conn.spark.emptyDataFrame
      .withColumn("TABLE_CAT", lit(""))
      .withColumn("TABLE_SCHEM", lit(""))
      .withColumn("TABLE_NAME", lit(""))
      .withColumn("COLUMN_NAME", lit(""))
      .withColumn("DATA_TYPE", lit(0))
      .withColumn("TYPE_NAME", lit(""))
      .withColumn("COLUMN_SIZE", lit(0))
      .withColumn("BUFFER_LENGTH", lit(0))
      .withColumn("DECIMAL_DIGITS", lit(0))
      .withColumn("NUM_PREC_RADIX", lit(0))
      .withColumn("NULLABLE", lit(0))
      .withColumn("REMARKS", lit(""))
      .withColumn("COLUMN_DEF", lit(""))
      .withColumn("SQL_DATA_TYPE", lit(0))
      .withColumn("SQL_DATETIME_SUB", lit(0))
      .withColumn("CHAR_OCTET_LENGTH", lit(0))
      .withColumn("ORDINAL_POSITION", lit(0))
      .withColumn("IS_NULLABLE", lit(""))
      .withColumn("SCOPE_CATALOG", lit(""))
      .withColumn("SCOPE_SCHEMA", lit(""))
      .withColumn("SCOPE_TABLE", lit(""))
      .withColumn("SOURCE_DATA_TYPE", lit(0.toShort))
      .withColumn("IS_AUTOINCREMENT", lit(""))
      .withColumn("IS_GENERATEDCOLUMN", lit(""))

    val catalogSchemaTables =
      getTablesDataFrame(catalog, schemaPattern, tableNamePattern)
        .select("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME")
        .collect().map { row => (row.getString(0), row.getString(1), row.getString(2)) }

    val df = catalogSchemaTables.map { case (catalog, schema, table) =>
      val columns = conn.spark.table(quoteNameParts(Seq(catalog, schema, table)))
        .schema.zipWithIndex.map { case (field, i) =>
          (
            field.name, // COLUMN_NAME
            JdbcTypeUtils.getColumnType(field), // DATA_TYPE
            field.dataType.sql, // TYPE_NAME
            JdbcTypeUtils.getDisplaySize(field), // COLUMN_SIZE
            JdbcTypeUtils.getDecimalDigits(field), // DECIMAL_DIGITS
            JdbcTypeUtils.getNumPrecRadix(field), // NUM_PREC_RADIX
            if (field.nullable) columnNullable else columnNoNulls, // NULLABLE
            field.getComment().orNull, // REMARKS
            field.getCurrentDefaultValue().orNull, // COLUMN_DEF
            0, // CHAR_OCTET_LENGTH
            i + 1, // ORDINAL_POSITION
            if (field.nullable) "YES" else "NO", // IS_NULLABLE
            "", // IS_AUTOINCREMENT
            "" // IS_GENERATEDCOLUMN
          )
        }
        columns.toDF("COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "DECIMAL_DIGITS",
            "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF", "CHAR_OCTET_LENGTH",
            "ORDINAL_POSITION", "IS_NULLABLE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN")
          .filter(columnNameFilterExpr)
          .withColumn("TABLE_CAT", lit(catalog))
          .withColumn("TABLE_SCHEM", lit(schema))
          .withColumn("TABLE_NAME", lit(table))
          .withColumn("BUFFER_LENGTH", lit(0))
          .withColumn("SQL_DATA_TYPE", lit(0))
          .withColumn("SQL_DATETIME_SUB", lit(0))
          .withColumn("SCOPE_CATALOG", lit(""))
          .withColumn("SCOPE_SCHEMA", lit(""))
          .withColumn("SCOPE_TABLE", lit(""))
          .withColumn("SOURCE_DATA_TYPE", lit(0.toShort))
          .select("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE",
            "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX",
            "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB",
            "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG",
            "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT",
            "IS_GENERATEDCOLUMN")
      }.fold(emptyDf) { (l, r) => l.unionAll(r) }
      .orderBy("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "ORDINAL_POSITION")

    new SparkConnectResultSet(df.collectResult())
  }

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

  private[jdbc] val TABLE_TYPES = Seq("TABLE", "VIEW")
}
