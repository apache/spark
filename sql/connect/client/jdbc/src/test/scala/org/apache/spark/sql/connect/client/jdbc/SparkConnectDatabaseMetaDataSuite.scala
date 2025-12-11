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

import scala.util.Using

import org.apache.spark.SparkBuildInfo.{spark_version => SPARK_VERSION}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.client.jdbc.test.JdbcHelper
import org.apache.spark.sql.connect.test.{ConnectFunSuite, RemoteSparkSession, SQLHelper}
import org.apache.spark.util.VersionUtils

class SparkConnectDatabaseMetaDataSuite extends ConnectFunSuite with RemoteSparkSession
    with JdbcHelper with SQLHelper {

  def jdbcUrl: String = s"jdbc:sc://localhost:$serverPort"

  // catalyst test jar is inaccessible here, but presents at the testing connect server classpath
  private val TEST_IN_MEMORY_CATALOG = "org.apache.spark.sql.connector.catalog.InMemoryCatalog"
  private val TEST_BASIC_IN_MEMORY_CATALOG =
    "org.apache.spark.sql.connector.catalog.BasicInMemoryTableCatalog"

  private def registerCatalog(
      name: String, className: String)(implicit spark: SparkSession): Unit = {
    spark.conf.set(s"spark.sql.catalog.$name", className)
  }

  test("SparkConnectDatabaseMetaData simple methods") {
    withConnection { conn =>
      val spark = conn.asInstanceOf[SparkConnectConnection].spark
      val metadata = conn.getMetaData
      assert(metadata.allProceduresAreCallable === false)
      assert(metadata.allTablesAreSelectable === false)
      assert(metadata.getURL === jdbcUrl)
      assert(metadata.isReadOnly === false)
      assert(metadata.nullsAreSortedHigh === false)
      assert(metadata.nullsAreSortedLow === false)
      assert(metadata.nullsAreSortedAtStart === false)
      assert(metadata.nullsAreSortedAtEnd === false)
      assert(metadata.getUserName === spark.client.configuration.userName)
      assert(metadata.getDatabaseProductName === "Apache Spark Connect Server")
      assert(metadata.getDatabaseProductVersion === spark.version)
      assert(metadata.getDriverVersion === SPARK_VERSION)
      assert(metadata.getDriverMajorVersion === VersionUtils.majorVersion(SPARK_VERSION))
      assert(metadata.getDriverMinorVersion === VersionUtils.minorVersion(SPARK_VERSION))
      assert(metadata.usesLocalFiles === false)
      assert(metadata.usesLocalFilePerTable === false)
      assert(metadata.supportsMixedCaseIdentifiers === false)
      assert(metadata.storesUpperCaseIdentifiers === false)
      assert(metadata.storesLowerCaseIdentifiers === false)
      assert(metadata.storesMixedCaseIdentifiers === false)
      assert(metadata.supportsMixedCaseQuotedIdentifiers === false)
      assert(metadata.storesUpperCaseQuotedIdentifiers === false)
      assert(metadata.storesLowerCaseQuotedIdentifiers === false)
      assert(metadata.storesMixedCaseQuotedIdentifiers === false)
      assert(metadata.getIdentifierQuoteString === "`")
      assert(metadata.getSearchStringEscape === "\\")
      assert(metadata.getExtraNameCharacters === "")
      assert(metadata.supportsAlterTableWithAddColumn === true)
      assert(metadata.supportsAlterTableWithDropColumn === true)
      assert(metadata.supportsColumnAliasing === true)
      assert(metadata.nullPlusNonNullIsNull === true)
      assert(metadata.supportsTableCorrelationNames === true)
      assert(metadata.supportsDifferentTableCorrelationNames === false)
      assert(metadata.supportsExpressionsInOrderBy === true)
      assert(metadata.supportsOrderByUnrelated === true)
      assert(metadata.supportsGroupBy === true)
      assert(metadata.supportsGroupByUnrelated === true)
      assert(metadata.supportsGroupByBeyondSelect === true)
      assert(metadata.supportsLikeEscapeClause === true)
      assert(metadata.supportsMultipleResultSets === false)
      assert(metadata.supportsMultipleTransactions === false)
      assert(metadata.supportsNonNullableColumns === true)
      assert(metadata.supportsMinimumSQLGrammar === true)
      assert(metadata.supportsCoreSQLGrammar === true)
      assert(metadata.supportsExtendedSQLGrammar === false)
      assert(metadata.supportsANSI92EntryLevelSQL === true)
      assert(metadata.supportsANSI92IntermediateSQL === false)
      assert(metadata.supportsANSI92FullSQL === false)
      assert(metadata.supportsIntegrityEnhancementFacility === false)
      assert(metadata.supportsOuterJoins === true)
      assert(metadata.supportsFullOuterJoins === true)
      assert(metadata.supportsLimitedOuterJoins === true)
      assert(metadata.getSchemaTerm === "schema")
      assert(metadata.getProcedureTerm === "procedure")
      assert(metadata.getCatalogTerm === "catalog")
      assert(metadata.isCatalogAtStart === true)
      assert(metadata.getCatalogSeparator === ".")
      assert(metadata.supportsSchemasInDataManipulation === true)
      assert(metadata.supportsSchemasInProcedureCalls === true)
      assert(metadata.supportsSchemasInTableDefinitions === true)
      assert(metadata.supportsSchemasInIndexDefinitions === true)
      assert(metadata.supportsCatalogsInDataManipulation === true)
      assert(metadata.supportsCatalogsInProcedureCalls === true)
      assert(metadata.supportsCatalogsInTableDefinitions === true)
      assert(metadata.supportsCatalogsInIndexDefinitions === true)
      assert(metadata.supportsPositionedDelete === false)
      assert(metadata.supportsPositionedUpdate === false)
      assert(metadata.supportsSelectForUpdate === false)
      assert(metadata.supportsStoredProcedures === true)
      assert(metadata.supportsSubqueriesInComparisons === true)
      assert(metadata.supportsSubqueriesInExists === true)
      assert(metadata.supportsSubqueriesInIns === true)
      assert(metadata.supportsSubqueriesInQuantifieds === true)
      assert(metadata.supportsCorrelatedSubqueries === true)
      assert(metadata.supportsUnion === true)
      assert(metadata.supportsUnionAll === true)
      assert(metadata.supportsOpenCursorsAcrossCommit === false)
      assert(metadata.supportsOpenCursorsAcrossRollback === false)
      assert(metadata.supportsOpenStatementsAcrossCommit === false)
      assert(metadata.supportsOpenStatementsAcrossRollback === false)
      assert(metadata.getMaxBinaryLiteralLength === 0)
      assert(metadata.getMaxCharLiteralLength === 0)
      assert(metadata.getMaxColumnNameLength === 0)
      assert(metadata.getMaxColumnsInGroupBy === 0)
      assert(metadata.getMaxColumnsInIndex === 0)
      assert(metadata.getMaxColumnsInOrderBy === 0)
      assert(metadata.getMaxColumnsInSelect === 0)
      assert(metadata.getMaxColumnsInTable === 0)
      assert(metadata.getMaxConnections === 0)
      assert(metadata.getMaxCursorNameLength === 0)
      assert(metadata.getMaxIndexLength === 0)
      assert(metadata.getMaxSchemaNameLength === 0)
      assert(metadata.getMaxProcedureNameLength === 0)
      assert(metadata.getMaxCatalogNameLength === 0)
      assert(metadata.getMaxRowSize === 0)
      assert(metadata.doesMaxRowSizeIncludeBlobs === false)
      assert(metadata.getMaxStatementLength === 0)
      assert(metadata.getMaxStatements === 0)
      assert(metadata.getMaxTableNameLength === 0)
      assert(metadata.getMaxTablesInSelect === 0)
      assert(metadata.getMaxUserNameLength === 0)
      assert(metadata.getDefaultTransactionIsolation === Connection.TRANSACTION_NONE)
      assert(metadata.supportsTransactions === false)
      Seq(Connection.TRANSACTION_NONE, Connection.TRANSACTION_READ_UNCOMMITTED,
        Connection.TRANSACTION_READ_COMMITTED, Connection.TRANSACTION_REPEATABLE_READ,
        Connection.TRANSACTION_SERIALIZABLE).foreach { level =>
        val actual = metadata.supportsTransactionIsolationLevel(level)
        val expected = level == Connection.TRANSACTION_NONE
        assert(actual === expected)
      }
      assert(metadata.supportsDataDefinitionAndDataManipulationTransactions === false)
      assert(metadata.supportsDataManipulationTransactionsOnly === false)
      assert(metadata.dataDefinitionCausesTransactionCommit === false)
      assert(metadata.dataDefinitionIgnoredInTransactions === false)
      Seq(ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
        ResultSet.TYPE_SCROLL_SENSITIVE).foreach { typ =>
        var actual = metadata.supportsResultSetType(typ)
        var expected = typ == ResultSet.TYPE_FORWARD_ONLY
        assert(actual === expected)
        Seq(ResultSet.CONCUR_READ_ONLY, ResultSet.CONCUR_UPDATABLE).foreach { concur =>
          actual = metadata.supportsResultSetConcurrency(typ, concur)
          expected = typ == ResultSet.TYPE_FORWARD_ONLY && concur == ResultSet.CONCUR_READ_ONLY
          assert(actual === expected)
        }
        assert(metadata.ownUpdatesAreVisible(typ) === false)
        assert(metadata.ownDeletesAreVisible(typ) === false)
        assert(metadata.ownInsertsAreVisible(typ) === false)
        assert(metadata.othersUpdatesAreVisible(typ) === false)
        assert(metadata.othersDeletesAreVisible(typ) === false)
        assert(metadata.othersInsertsAreVisible(typ) === false)
        assert(metadata.updatesAreDetected(typ) === false)
        assert(metadata.deletesAreDetected(typ) === false)
        assert(metadata.insertsAreDetected(typ) === false)
      }
      assert(metadata.supportsBatchUpdates === false)
      assert(metadata.getConnection === conn)
      assert(metadata.supportsSavepoints === false)
      assert(metadata.supportsMultipleOpenResults === false)
      assert(metadata.supportsGetGeneratedKeys === false)
      assert(metadata.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT) === false)
      assert(metadata.supportsResultSetHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT) === true)
      assert(metadata.getResultSetHoldability === ResultSet.CLOSE_CURSORS_AT_COMMIT)
      assert(metadata.getDatabaseMajorVersion === VersionUtils.majorVersion(spark.version))
      assert(metadata.getDatabaseMinorVersion === VersionUtils.minorVersion(spark.version))
      assert(metadata.getJDBCMajorVersion === 4)
      assert(metadata.getJDBCMinorVersion === 3)
      assert(metadata.getSQLStateType === DatabaseMetaData.sqlStateSQL)
      assert(metadata.locatorsUpdateCopy === false)
      assert(metadata.supportsStoredFunctionsUsingCallSyntax === false)
      assert(metadata.autoCommitFailureClosesAllResultSets === false)
      assert(metadata.supportsStatementPooling === false)
      assert(metadata.getRowIdLifetime === RowIdLifetime.ROWID_UNSUPPORTED)
      assert(metadata.generatedKeyAlwaysReturned === false)
      assert(metadata.getMaxLogicalLobSize === 0)
      assert(metadata.supportsRefCursors === false)
      assert(metadata.supportsSharding === false)
    }
  }

  test("SparkConnectDatabaseMetaData getSQLKeywords") {
    withConnection { conn =>
      val metadata = conn.getMetaData
      // scalastyle:off line.size.limit
      assert(metadata.getSQLKeywords === "ADD,AFTER,AGGREGATE,ALWAYS,ANALYZE,ANTI,ANY_VALUE,ARCHIVE,ASC,BINDING,BUCKET,BUCKETS,BYTE,CACHE,CASCADE,CATALOG,CATALOGS,CHANGE,CLEAR,CLUSTER,CLUSTERED,CODEGEN,COLLATION,COLLECTION,COLUMNS,COMMENT,COMPACT,COMPACTIONS,COMPENSATION,COMPUTE,CONCATENATE,CONTAINS,CONTINUE,COST,DATA,DATABASE,DATABASES,DATEADD,DATEDIFF,DATE_ADD,DATE_DIFF,DAYOFYEAR,DAYS,DBPROPERTIES,DEFINED,DEFINER,DELAY,DELIMITED,DESC,DFS,DIRECTORIES,DIRECTORY,DISTRIBUTE,DIV,DO,ELSEIF,ENFORCED,ESCAPED,EVOLUTION,EXCHANGE,EXCLUDE,EXIT,EXPLAIN,EXPORT,EXTEND,EXTENDED,FIELDS,FILEFORMAT,FIRST,FLOW,FOLLOWING,FORMAT,FORMATTED,FOUND,FUNCTIONS,GENERATED,GEOGRAPHY,GEOMETRY,HANDLER,HOURS,IDENTIFIER,IF,IGNORE,ILIKE,IMMEDIATE,INCLUDE,INCREMENT,INDEX,INDEXES,INPATH,INPUT,INPUTFORMAT,INVOKER,ITEMS,ITERATE,JSON,KEY,KEYS,LAST,LAZY,LEAVE,LEVEL,LIMIT,LINES,LIST,LOAD,LOCATION,LOCK,LOCKS,LOGICAL,LONG,LOOP,MACRO,MAP,MATCHED,MATERIALIZED,MICROSECOND,MICROSECONDS,MILLISECOND,MILLISECONDS,MINUS,MINUTES,MONTHS,MSCK,NAME,NAMESPACE,NAMESPACES,NANOSECOND,NANOSECONDS,NORELY,NULLS,OFFSET,OPTION,OPTIONS,OUTPUTFORMAT,OVERWRITE,PARTITIONED,PARTITIONS,PERCENT,PIVOT,PLACING,PRECEDING,PRINCIPALS,PROCEDURES,PROPERTIES,PURGE,QUARTER,QUERY,RECORDREADER,RECORDWRITER,RECOVER,RECURSION,REDUCE,REFRESH,RELY,RENAME,REPAIR,REPEAT,REPEATABLE,REPLACE,RESET,RESPECT,RESTRICT,ROLE,ROLES,SCHEMA,SCHEMAS,SECONDS,SECURITY,SEMI,SEPARATED,SERDE,SERDEPROPERTIES,SETS,SHORT,SHOW,SINGLE,SKEWED,SORT,SORTED,SOURCE,STATISTICS,STORED,STRATIFY,STREAM,STREAMING,STRING,STRUCT,SUBSTR,SYNC,SYSTEM_TIME,SYSTEM_VERSION,TABLES,TARGET,TBLPROPERTIES,TERMINATED,TIMEDIFF,TIMESTAMPADD,TIMESTAMPDIFF,TIMESTAMP_LTZ,TIMESTAMP_NTZ,TINYINT,TOUCH,TRANSACTION,TRANSACTIONS,TRANSFORM,TRUNCATE,TRY_CAST,TYPE,UNARCHIVE,UNBOUNDED,UNCACHE,UNLOCK,UNPIVOT,UNSET,UNTIL,USE,VAR,VARIABLE,VARIANT,VERSION,VIEW,VIEWS,VOID,WATERMARK,WEEK,WEEKS,WHILE,X,YEARS,ZONE")
      // scalastyle:on line.size.limit
    }
  }

  test("SparkConnectDatabaseMetaData getCatalogs") {
    withConnection { conn =>
      implicit val spark: SparkSession = conn.asInstanceOf[SparkConnectConnection].spark

      registerCatalog("testcat", TEST_IN_MEMORY_CATALOG)
      registerCatalog("testcat2", TEST_IN_MEMORY_CATALOG)

      // forcibly initialize the registered catalogs because SHOW CATALOGS only
      // returns the initialized catalogs.
      spark.sql("USE testcat")
      spark.sql("USE testcat2")
      spark.sql("USE spark_catalog")

      val metadata = conn.getMetaData
      Using.resource(metadata.getCatalogs) { rs =>
        val catalogs = new Iterator[String] {
          def hasNext: Boolean = rs.next()
          def next(): String = rs.getString("TABLE_CAT")
        }.toSeq
        // results are ordered by TABLE_CAT
        assert(catalogs === Seq("spark_catalog", "testcat", "testcat2"))
      }
    }
  }

  test("SparkConnectDatabaseMetaData getSchemas") {

    def verifyGetSchemas(
        getSchemas: () => ResultSet)(verify: Seq[(String, String)] => Unit): Unit = {
      Using.resource(getSchemas()) { rs =>
        val catalogDatabases = new Iterator[(String, String)] {
          def hasNext: Boolean = rs.next()
          def next(): (String, String) =
            (rs.getString("TABLE_CATALOG"), rs.getString("TABLE_SCHEM"))
        }.toSeq
        verify(catalogDatabases)
      }
    }

    withConnection { conn =>
      implicit val spark: SparkSession = conn.asInstanceOf[SparkConnectConnection].spark

      // this catalog does not support namespace
      registerCatalog("test_noop", TEST_BASIC_IN_MEMORY_CATALOG)
      // Spark loads catalog plugins lazily, we must initialize it first,
      // otherwise it won't be listed by SHOW CATALOGS
      conn.setCatalog("test_noop")

      registerCatalog("test`cat", TEST_IN_MEMORY_CATALOG)

      spark.sql("CREATE DATABASE IF NOT EXISTS `test``cat`.t_db1")
      spark.sql("CREATE DATABASE IF NOT EXISTS `test``cat`.t_db2")
      spark.sql("CREATE DATABASE IF NOT EXISTS `test``cat`.t_db_")

      spark.sql("CREATE DATABASE IF NOT EXISTS spark_catalog.db1")
      spark.sql("CREATE DATABASE IF NOT EXISTS spark_catalog.db2")
      spark.sql("CREATE DATABASE IF NOT EXISTS spark_catalog.test_db3")

      val metadata = conn.getMetaData

      // no need to care about "test`cat" because it is memory based and session isolated,
      // also is inaccessible from another SparkSession
      withDatabase("spark_catalog.db1", "spark_catalog.db2", "spark_catalog.test_db3") {
        // list schemas in all catalogs
        val getSchemasInAllCatalogs = (() => metadata.getSchemas) ::
          List(null, "%").map { database => () => metadata.getSchemas(null, database) } ::: Nil

        getSchemasInAllCatalogs.foreach { getSchemas =>
          verifyGetSchemas(getSchemas) { catalogDatabases =>
            // results are ordered by TABLE_CATALOG, TABLE_SCHEM
            assert {
              catalogDatabases === Seq(
                ("spark_catalog", "db1"),
                ("spark_catalog", "db2"),
                ("spark_catalog", "default"),
                ("spark_catalog", "test_db3"),
                ("test`cat", "t_db1"),
                ("test`cat", "t_db2"),
                ("test`cat", "t_db_"))
            }
          }
        }

        // list schemas in current catalog
        conn.setCatalog("spark_catalog")
        assert(conn.getCatalog === "spark_catalog")
        val getSchemasInCurrentCatalog =
          List(null, "%").map { database => () => metadata.getSchemas("", database) }
        getSchemasInCurrentCatalog.foreach { getSchemas =>
          verifyGetSchemas(getSchemas) { catalogDatabases =>
            // results are ordered by TABLE_CATALOG, TABLE_SCHEM
            assert {
              catalogDatabases === Seq(
                ("spark_catalog", "db1"),
                ("spark_catalog", "db2"),
                ("spark_catalog", "default"),
                ("spark_catalog", "test_db3"))
            }
          }
        }

        // list schemas with schema pattern
        verifyGetSchemas { () => metadata.getSchemas(null, "db%") } { catalogDatabases =>
          // results are ordered by TABLE_CATALOG, TABLE_SCHEM
          assert {
            catalogDatabases === Seq(
              ("spark_catalog", "db1"),
              ("spark_catalog", "db2"))
          }
        }

        verifyGetSchemas { () => metadata.getSchemas(null, "db_") } { catalogDatabases =>
          // results are ordered by TABLE_CATALOG, TABLE_SCHEM
          assert {
            catalogDatabases === Seq(
              ("spark_catalog", "db1"),
              ("spark_catalog", "db2"))
          }
        }

        // escape backtick in catalog, and _ in schema pattern
        verifyGetSchemas {
          () => metadata.getSchemas("test`cat", "t\\_db\\_")
        } { catalogDatabases =>
          assert(catalogDatabases === Seq(("test`cat", "t_db_")))
        }

        // skip testing escape ', % in schema pattern, because Spark SQL does not
        // allow using those chars in schema table name.
        //
        //   CREATE DATABASE IF NOT EXISTS `t_db1'`;
        //
        // the above SQL fails with error condition:
        //   [INVALID_SCHEMA_OR_RELATION_NAME] `t_db1'` is not a valid name for tables/schemas.
        //   Valid names only contain alphabet characters, numbers and _. SQLSTATE: 42602
      }
    }
  }

  test("SparkConnectDatabaseMetaData getTableTypes") {
    withConnection { conn =>
      val metadata = conn.getMetaData
      Using.resource(metadata.getTableTypes) { rs =>
        val types = new Iterator[String] {
          def hasNext: Boolean = rs.next()
          def next(): String = rs.getString("TABLE_TYPE")
        }.toSeq
        // results are ordered by TABLE_TYPE
        assert(types === Seq("TABLE", "VIEW"))
      }
    }
  }

  test("SparkConnectDatabaseMetaData getTables") {

    case class GetTableResult(
       TABLE_CAT: String,
       TABLE_SCHEM: String,
       TABLE_NAME: String,
       TABLE_TYPE: String,
       REMARKS: String,
       TYPE_CAT: String,
       TYPE_SCHEM: String,
       TYPE_NAME: String,
       SELF_REFERENCING_COL_NAME: String,
       REF_GENERATION: String)

    def verifyEmptyStringFields(result: GetTableResult): Unit = {
      assert(result.REMARKS === "")
      assert(result.TYPE_CAT === "")
      assert(result.TYPE_SCHEM === "")
      assert(result.TYPE_NAME === "")
      assert(result.SELF_REFERENCING_COL_NAME === "")
      assert(result.REF_GENERATION === "")
    }

    def verifyGetTables(
        getTables: () => ResultSet)(verify: Seq[GetTableResult] => Unit): Unit = {
      Using.resource(getTables()) { rs =>
        val getTableResults = new Iterator[GetTableResult] {
          def hasNext: Boolean = rs.next()
          def next(): GetTableResult = GetTableResult(
            TABLE_CAT = rs.getString("TABLE_CAT"),
            TABLE_SCHEM = rs.getString("TABLE_SCHEM"),
            TABLE_NAME = rs.getString("TABLE_NAME"),
            TABLE_TYPE = rs.getString("TABLE_TYPE"),
            REMARKS = rs.getString("REMARKS"),
            TYPE_CAT = rs.getString("TYPE_CAT"),
            TYPE_SCHEM = rs.getString("TYPE_SCHEM"),
            TYPE_NAME = rs.getString("TYPE_NAME"),
            SELF_REFERENCING_COL_NAME = rs.getString("SELF_REFERENCING_COL_NAME"),
            REF_GENERATION = rs.getString("REF_GENERATION"))
        }.toSeq
        verify(getTableResults)
      }
    }

    withConnection { conn =>
      implicit val spark: SparkSession = conn.asInstanceOf[SparkConnectConnection].spark

      // this catalog does not support namespace
      registerCatalog("test_noop", TEST_BASIC_IN_MEMORY_CATALOG)
      // Spark loads catalog plugins lazily, we must initialize it first,
      // otherwise it won't be listed by SHOW CATALOGS
      conn.setCatalog("test_noop")

      // this catalog does not support view
      registerCatalog("testcat", TEST_IN_MEMORY_CATALOG)

      spark.sql("CREATE DATABASE IF NOT EXISTS testcat.t_db1")
      spark.sql("CREATE TABLE IF NOT EXISTS testcat.t_db1.t_t1 (id INT)")

      spark.sql("CREATE DATABASE IF NOT EXISTS spark_catalog.db1")
      spark.sql("CREATE TABLE IF NOT EXISTS spark_catalog.db1.t1 (id INT)")
      spark.sql("CREATE TABLE IF NOT EXISTS spark_catalog.db1.t_2 (id INT)")
      spark.sql(
        """CREATE VIEW IF NOT EXISTS spark_catalog.db1.t1_v AS
          |SELECT id FROM spark_catalog.db1.t1
          |""".stripMargin)

      spark.sql("CREATE DATABASE IF NOT EXISTS spark_catalog.db_2")
      spark.sql("CREATE TABLE IF NOT EXISTS spark_catalog.db_2.t_2 (id INT)")
      spark.sql(
        """CREATE VIEW IF NOT EXISTS spark_catalog.db_2.t_2_v AS
          |SELECT id FROM spark_catalog.db_2.t_2
          |""".stripMargin)

      spark.sql("CREATE DATABASE IF NOT EXISTS spark_catalog.db_")
      spark.sql("CREATE TABLE IF NOT EXISTS spark_catalog.db_.t_ (id INT)")

      val metadata = conn.getMetaData

      // no need to care about "testcat" because it is memory based and session isolated,
      // also is inaccessible from another SparkSession
      withDatabase("spark_catalog.db1", "spark_catalog.db_2", "spark_catalog.db_") {
        // list tables in all catalogs and schemas
        val getTablesInAllCatalogsAndSchemas = List(null, "%").flatMap { database =>
          List(null, "%").flatMap { table =>
            List(null, Array("TABLE", "VIEW")).map { tableTypes =>
              () => metadata.getTables(null, database, table, tableTypes)
            }
          }
        }

        getTablesInAllCatalogsAndSchemas.foreach { getTables =>
          verifyGetTables(getTables) { getTableResults =>
            // results are ordered by TABLE_TYPE, TABLE_CAT, TABLE_SCHEM and TABLE_NAME
            assert {
              getTableResults.map { result =>
                (result.TABLE_TYPE, result.TABLE_CAT, result.TABLE_SCHEM, result.TABLE_NAME)
              } === Seq(
                ("TABLE", "spark_catalog", "db1", "t1"),
                ("TABLE", "spark_catalog", "db1", "t_2"),
                ("TABLE", "spark_catalog", "db_", "t_"),
                ("TABLE", "spark_catalog", "db_2", "t_2"),
                ("TABLE", "testcat", "t_db1", "t_t1"),
                ("VIEW", "spark_catalog", "db1", "t1_v"),
                ("VIEW", "spark_catalog", "db_2", "t_2_v"))
            }
            getTableResults.foreach(verifyEmptyStringFields)
          }
        }

        // list tables with table types
        val se = intercept[SQLException] {
          metadata.getTables("spark_catalog", "foo", "bar", Array("TABLE", "MATERIALIZED VIEW"))
        }
        assert(se.getMessage ===
          "The requested table types contains unsupported items: MATERIALIZED VIEW. " +
            "Available table types are: TABLE, VIEW.")

        verifyGetTables {
          () => metadata.getTables("spark_catalog", "db1", "%", Array("TABLE"))
        } { getTableResults =>
          // results are ordered by TABLE_TYPE, TABLE_CAT, TABLE_SCHEM and TABLE_NAME
          assert {
            getTableResults.map { result =>
              (result.TABLE_TYPE, result.TABLE_CAT, result.TABLE_SCHEM, result.TABLE_NAME)
            } === Seq(
              ("TABLE", "spark_catalog", "db1", "t1"),
              ("TABLE", "spark_catalog", "db1", "t_2"))
          }
          getTableResults.foreach(verifyEmptyStringFields)
        }

        verifyGetTables {
          () => metadata.getTables("spark_catalog", "db1", "%", Array("VIEW"))
        } { getTableResults =>
          // results are ordered by TABLE_TYPE, TABLE_CAT, TABLE_SCHEM and TABLE_NAME
          assert {
            getTableResults.map { result =>
              (result.TABLE_TYPE, result.TABLE_CAT, result.TABLE_SCHEM, result.TABLE_NAME)
            } === Seq(("VIEW", "spark_catalog", "db1", "t1_v"))
          }
          getTableResults.foreach(verifyEmptyStringFields)
        }

        // list tables in the current catalog and schema
        conn.setCatalog("spark_catalog")
        conn.setSchema("db1")
        assert(conn.getCatalog === "spark_catalog")
        assert(conn.getSchema === "db1")

        verifyGetTables {
          () => metadata.getTables("", "", "%", null)
        } { getTableResults =>
          assert {
            getTableResults.map { result =>
              (result.TABLE_TYPE, result.TABLE_CAT, result.TABLE_SCHEM, result.TABLE_NAME)
            } === Seq(
              ("TABLE", "spark_catalog", "db1", "t1"),
              ("TABLE", "spark_catalog", "db1", "t_2"),
              ("VIEW", "spark_catalog", "db1", "t1_v"))
          }
          getTableResults.foreach(verifyEmptyStringFields)
        }

        // list tables with schema pattern and table mame pattern
        verifyGetTables {
          () => metadata.getTables(null, "db%", "t_", null)
        } { getTableResults =>
          assert {
            getTableResults.map { result =>
              (result.TABLE_TYPE, result.TABLE_CAT, result.TABLE_SCHEM, result.TABLE_NAME)
            } === Seq(
              ("TABLE", "spark_catalog", "db1", "t1"),
              ("TABLE", "spark_catalog", "db_", "t_"))
          }
          getTableResults.foreach(verifyEmptyStringFields)
        }

        // escape _ in schema pattern and table mame pattern
        verifyGetTables {
          () => metadata.getTables(null, "db\\_", "t\\_", null)
        } { getTableResults =>
          assert {
            getTableResults.map { result =>
              (result.TABLE_TYPE, result.TABLE_CAT, result.TABLE_SCHEM, result.TABLE_NAME)
            } === Seq(("TABLE", "spark_catalog", "db_", "t_"))
          }
          getTableResults.foreach(verifyEmptyStringFields)
        }

        // skip testing escape ', % in schema pattern, because Spark SQL does not
        // allow using those chars in schema table name.
      }
    }
  }

  test("SparkConnectDatabaseMetaData getColumns") {

     case class GetColumnResult(
       TABLE_CAT: String,
       TABLE_SCHEM: String,
       TABLE_NAME: String,
       COLUMN_NAME: String,
       DATA_TYPE: Int,
       TYPE_NAME: String,
       COLUMN_SIZE: Int,
       BUFFER_LENGTH: Int,
       DECIMAL_DIGITS: Int,
       NUM_PREC_RADIX: Int,
       NULLABLE: Int,
       REMARKS: String,
       COLUMN_DEF: String,
       SQL_DATA_TYPE: Int,
       SQL_DATETIME_SUB: Int,
       CHAR_OCTET_LENGTH: Int,
       ORDINAL_POSITION: Int,
       IS_NULLABLE: String,
       SCOPE_CATALOG: String,
       SCOPE_SCHEMA: String,
       SCOPE_TABLE: String,
       SOURCE_DATA_TYPE: Short,
       IS_AUTOINCREMENT: String,
       IS_GENERATEDCOLUMN: String)

    def verifyEmptyFields(result: GetColumnResult): Unit = {
        assert(result.BUFFER_LENGTH === 0)
        assert(result.SQL_DATA_TYPE === 0)
        assert(result.SQL_DATETIME_SUB === 0)
        assert(result.SCOPE_CATALOG === "")
        assert(result.SCOPE_SCHEMA === "")
        assert(result.SCOPE_TABLE === "")
        assert(result.SOURCE_DATA_TYPE === 0.toShort)
    }

    def verifyGetColumns(
        getColumns: () => ResultSet)(verify: Seq[GetColumnResult] => Unit): Unit = {
      Using.resource(getColumns()) { rs =>
        val getTableResults = new Iterator[GetColumnResult] {
          def hasNext: Boolean = rs.next()

          def next(): GetColumnResult = GetColumnResult(
            TABLE_CAT = rs.getString("TABLE_CAT"),
            TABLE_SCHEM = rs.getString("TABLE_SCHEM"),
            TABLE_NAME = rs.getString("TABLE_NAME"),
            COLUMN_NAME = rs.getString("COLUMN_NAME"),
            DATA_TYPE = rs.getInt("DATA_TYPE"),
            TYPE_NAME = rs.getString("TYPE_NAME"),
            COLUMN_SIZE = rs.getInt("COLUMN_SIZE"),
            BUFFER_LENGTH = rs.getInt("BUFFER_LENGTH"),
            DECIMAL_DIGITS = rs.getInt("DECIMAL_DIGITS"),
            NUM_PREC_RADIX = rs.getInt("NUM_PREC_RADIX"),
            NULLABLE = rs.getInt("NULLABLE"),
            REMARKS = rs.getString("REMARKS"),
            COLUMN_DEF = rs.getString("COLUMN_DEF"),
            SQL_DATA_TYPE = rs.getInt("SQL_DATA_TYPE"),
            SQL_DATETIME_SUB = rs.getInt("SQL_DATETIME_SUB"),
            CHAR_OCTET_LENGTH = rs.getInt("CHAR_OCTET_LENGTH"),
            ORDINAL_POSITION = rs.getInt("ORDINAL_POSITION"),
            IS_NULLABLE = rs.getString("IS_NULLABLE"),
            SCOPE_CATALOG = rs.getString("SCOPE_CATALOG"),
            SCOPE_SCHEMA = rs.getString("SCOPE_SCHEMA"),
            SCOPE_TABLE = rs.getString("SCOPE_TABLE"),
            SOURCE_DATA_TYPE = rs.getShort("SOURCE_DATA_TYPE"),
            IS_AUTOINCREMENT = rs.getString("IS_AUTOINCREMENT"),
            IS_GENERATEDCOLUMN = rs.getString("IS_GENERATEDCOLUMN"))
        }.toSeq
        verify(getTableResults)
      }
    }

    withConnection { conn =>
      implicit val spark: SparkSession = conn.asInstanceOf[SparkConnectConnection].spark

      spark.sql("CREATE DATABASE IF NOT EXISTS testcat.t_db1")
      spark.sql("CREATE TABLE IF NOT EXISTS testcat.t_db1.t_t1 (id INT)")

      spark.sql("CREATE DATABASE IF NOT EXISTS spark_catalog.db1")
      spark.sql(
        """CREATE TABLE IF NOT EXISTS spark_catalog.db1.t1 (
          |  id INT NOT NULL,
          |  i_ INT,
          |  location STRING COMMENT 'city name' DEFAULT 'unknown')
          |""".stripMargin)
      spark.sql(
        """CREATE TABLE IF NOT EXISTS spark_catalog.db1.t2 (
          |  col_null VOID,
          |  col_boolean BOOLEAN,
          |  col_byte BYTE,
          |  col_short SHORT,
          |  col_int INT,
          |  col_long LONG,
          |  col_float FLOAT,
          |  col_double DOUBLE,
          |  col_string STRING,
          |  col_decimal DECIMAL(10, 5),
          |  col_date DATE,
          |  col_timestamp TIMESTAMP,
          |  col_timestamp_ntz TIMESTAMP_NTZ,
          |  col_binary BINARY,
          |  col_time TIME)""".stripMargin)

      spark.sql(
        """CREATE VIEW IF NOT EXISTS spark_catalog.db1.t1_v AS
          |SELECT id FROM spark_catalog.db1.t1
          |""".stripMargin)

      spark.sql("CREATE DATABASE IF NOT EXISTS spark_catalog.db_")
      spark.sql("CREATE TABLE IF NOT EXISTS spark_catalog.db_.t_ (id INT, i_ INT)")

      spark.sql("CREATE DATABASE IF NOT EXISTS spark_catalog.db_2")
      spark.sql("CREATE TABLE IF NOT EXISTS spark_catalog.db_2.t_2 (id INT)")

      val metadata = conn.getMetaData

      // no need to care about "testcat" because it is memory based and session isolated,
      // also is inaccessible from another SparkSession
      withDatabase("spark_catalog.db1", "spark_catalog.db_2", "spark_catalog.db_") {
        // list columns of all tables in all catalogs and schemas
        val getColumnsInAllTables = List(null, "%").flatMap { database =>
          List(null, "%").flatMap { table =>
            List(null, "%").map { column =>
              () => metadata.getColumns(null, database, table, column)
            }
          }
        }

        getColumnsInAllTables.foreach { getColumns =>
          verifyGetColumns(getColumns) { getColumnResults =>
            // results are ordered by TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION
            assert {
              getColumnResults.map { r =>
                (r.TABLE_CAT, r.TABLE_SCHEM, r.TABLE_NAME, r.ORDINAL_POSITION, r.COLUMN_NAME)
              } === Seq(
                ("spark_catalog", "db1", "t1", 1, "id"),
                ("spark_catalog", "db1", "t1", 2, "i_"),
                ("spark_catalog", "db1", "t1", 3, "location"),
                ("spark_catalog", "db1", "t1_v", 1, "id"),
                ("spark_catalog", "db1", "t2", 1, "col_null"),
                ("spark_catalog", "db1", "t2", 2, "col_boolean"),
                ("spark_catalog", "db1", "t2", 3, "col_byte"),
                ("spark_catalog", "db1", "t2", 4, "col_short"),
                ("spark_catalog", "db1", "t2", 5, "col_int"),
                ("spark_catalog", "db1", "t2", 6, "col_long"),
                ("spark_catalog", "db1", "t2", 7, "col_float"),
                ("spark_catalog", "db1", "t2", 8, "col_double"),
                ("spark_catalog", "db1", "t2", 9, "col_string"),
                ("spark_catalog", "db1", "t2", 10, "col_decimal"),
                ("spark_catalog", "db1", "t2", 11, "col_date"),
                ("spark_catalog", "db1", "t2", 12, "col_timestamp"),
                ("spark_catalog", "db1", "t2", 13, "col_timestamp_ntz"),
                ("spark_catalog", "db1", "t2", 14, "col_binary"),
                ("spark_catalog", "db1", "t2", 15, "col_time"),
                ("spark_catalog", "db_", "t_", 1, "id"),
                ("spark_catalog", "db_", "t_", 2, "i_"),
                ("spark_catalog", "db_2", "t_2", 1, "id"),
                ("testcat", "t_db1", "t_t1", 1, "id"))
            }

            // TODO verify the remaining attributes
            // DATA_TYPE = rs.getInt("DATA_TYPE"),
            // TYPE_NAME = rs.getString("TYPE_NAME"),
            // COLUMN_SIZE = rs.getInt("COLUMN_SIZE"),
            // DECIMAL_DIGITS = rs.getInt("DECIMAL_DIGITS"),
            // NUM_PREC_RADIX = rs.getInt("NUM_PREC_RADIX"),
            // NULLABLE = rs.getInt("NULLABLE"),
            // REMARKS = rs.getString("REMARKS"),
            // COLUMN_DEF = rs.getString("COLUMN_DEF"),
            // CHAR_OCTET_LENGTH = rs.getInt("CHAR_OCTET_LENGTH"),
            // IS_NULLABLE = rs.getString("IS_NULLABLE"),
            // IS_AUTOINCREMENT = rs.getString("IS_AUTOINCREMENT"),
            // IS_GENERATEDCOLUMN = rs.getString("IS_GENERATEDCOLUMN")

            getColumnResults.foreach(verifyEmptyFields)
          }
        }

        // list columns of all tables in the current catalog and schema
        conn.setCatalog("spark_catalog")
        conn.setSchema("db1")
        assert(conn.getCatalog === "spark_catalog")
        assert(conn.getSchema === "db1")

        verifyGetColumns(() => metadata.getColumns("", "", "%", "id")) { getColumnResults =>
            // results are ordered by TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION
            assert {
              getColumnResults.map { r =>
                (r.TABLE_CAT, r.TABLE_SCHEM, r.TABLE_NAME, r.ORDINAL_POSITION, r.COLUMN_NAME)
              } === Seq(
                ("spark_catalog", "db1", "t1", 1, "id"),
                ("spark_catalog", "db1", "t1_v", 1, "id"))
            }

          getColumnResults.foreach(verifyEmptyFields)
        }

        // list columns of tables with schema pattern, table mame pattern, and column name pattern
        verifyGetColumns {
          () => metadata.getColumns(null, "%db_", "%t_", "%d%")
        } { getColumnResults =>
          // results are ordered by TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION
          assert {
            getColumnResults.map { r =>
              (r.TABLE_CAT, r.TABLE_SCHEM, r.TABLE_NAME, r.ORDINAL_POSITION, r.COLUMN_NAME)
            } === Seq(
              ("spark_catalog", "db1", "t1", 1, "id"),
              ("spark_catalog", "db1", "t2", 8, "col_double"),
              ("spark_catalog", "db1", "t2", 10, "col_decimal"),
              ("spark_catalog", "db1", "t2", 11, "col_date"),
              ("spark_catalog", "db_", "t_", 1, "id"),
              ("testcat", "t_db1", "t_t1", 1, "id"))
          }

          getColumnResults.foreach(verifyEmptyFields)
        }

        // escape _ in schema pattern and table mame pattern
        verifyGetColumns {
          () => metadata.getColumns(null, "db\\_", "t\\_", "i\\_")
        } { getColumnResults =>
          // results are ordered by TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION
          assert {
            getColumnResults.map { r =>
              (r.TABLE_CAT, r.TABLE_SCHEM, r.TABLE_NAME, r.ORDINAL_POSITION, r.COLUMN_NAME)
            } === Seq(("spark_catalog", "db_", "t_", 2, "i_"))
          }

          getColumnResults.foreach(verifyEmptyFields)
        }

        // skip testing escape ', % in schema pattern, because Spark SQL does not
        // allow using those chars in schema table name.
      }
    }
  }
}
