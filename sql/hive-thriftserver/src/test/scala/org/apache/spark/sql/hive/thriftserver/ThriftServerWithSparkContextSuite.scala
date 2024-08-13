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

package org.apache.spark.sql.hive.thriftserver

import java.sql.{ResultSet, SQLException}
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.hive.service.cli.{GetInfoType, HiveSQLException, OperationHandle}

import org.apache.spark.{ErrorMessageFormat, TaskKilled}
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.internal.SQLConf

trait ThriftServerWithSparkContextSuite extends SharedThriftServer {

  test("the scratch dir will not be exist") {
    assert(!tempScratchDir.exists())
  }

  test("SPARK-29911: Uncache cached tables when session closed") {
    val cacheManager = spark.sharedState.cacheManager
    val globalTempDB = spark.sharedState.globalTempDB
    withJdbcStatement() { statement =>
      statement.execute("CACHE TABLE tempTbl AS SELECT 1")
    }
    // the cached data of local temporary view should be uncached
    assert(cacheManager.isEmpty)
    try {
      withJdbcStatement() { statement =>
        statement.execute("CREATE GLOBAL TEMP VIEW globalTempTbl AS SELECT 1, 2")
        statement.execute(s"CACHE TABLE $globalTempDB.globalTempTbl")
      }
      // the cached data of global temporary view shouldn't be uncached
      assert(!cacheManager.isEmpty)
    } finally {
      withJdbcStatement() { statement =>
        statement.execute(s"UNCACHE TABLE IF EXISTS $globalTempDB.globalTempTbl")
      }
      assert(cacheManager.isEmpty)
    }
  }

  test("Full stack traces as error message for jdbc or thrift client") {
    val sql = "select from_json('a', 'a INT', map('mode', 'FAILFAST'))"
    withCLIServiceClient() { client =>
      val sessionHandle = client.openSession(user, "")

      val confOverlay = new java.util.HashMap[java.lang.String, java.lang.String]
      val e = intercept[HiveSQLException] {
        client.executeStatement(
          sessionHandle,
          sql,
          confOverlay)
      }
      assert(e.getMessage.contains("JsonParseException: Unrecognized token 'a'"))
      assert(!e.getMessage.contains(
        "SparkException: [MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION]"))
    }

    withJdbcStatement() { statement =>
      val e = intercept[SQLException] {
        statement.executeQuery(sql)
      }
      assert(e.getMessage.contains("JsonParseException: Unrecognized token 'a'"))
      assert(e.getMessage.contains(
        "SparkException: [MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION]"))
    }
  }

  test("SPARK-33526: Add config to control if cancel invoke interrupt task on thriftserver") {
    withJdbcStatement() { statement =>
      val forceCancel = new AtomicBoolean(false)
      val listener = new SparkListener {
        override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
          assert(taskEnd.reason.isInstanceOf[TaskKilled])
          if (forceCancel.get()) {
            assert(System.currentTimeMillis() - taskEnd.taskInfo.launchTime < 1000)
          } else {
            // avoid accuracy, we check 2s instead of 3s.
            assert(System.currentTimeMillis() - taskEnd.taskInfo.launchTime >= 2000)
          }
        }
      }

      spark.sparkContext.addSparkListener(listener)
      try {
        Seq(true, false).foreach { force =>
          statement.setQueryTimeout(0)
          statement.execute(s"SET ${SQLConf.THRIFTSERVER_FORCE_CANCEL.key}=$force")
          statement.setQueryTimeout(1)
          forceCancel.set(force)
          val e = intercept[SQLException] {
            statement.execute("select java_method('java.lang.Thread', 'sleep', 3000L)")
          }.getMessage
          assert(e.contains("Query timed out"))
        }
      } finally {
        spark.sparkContext.removeSparkListener(listener)
      }
    }
  }

  test("SPARK-21957: get current_user, user, session_user through thrift server") {
    val clientUser = "storm_earth_fire_heed_my_call"
    val sql = "select current_user()"

    withCLIServiceClient(clientUser) { client =>
      val sessionHandle = client.openSession(clientUser, "")
      val confOverlay = new java.util.HashMap[java.lang.String, java.lang.String]
      val exec: String => OperationHandle = client.executeStatement(sessionHandle, _, confOverlay)

      exec(s"set ${SQLConf.ANSI_ENABLED.key}=false")

      val userFuncs = Seq("user", "current_user", "session_user")
      userFuncs.foreach { func =>
        val opHandle1 = exec(s"select $func(), $func")
        val rowSet1 = client.fetchResults(opHandle1)
        rowSet1.getColumns.forEach { col =>
          assert(col.getStringVal.getValues.get(0) === clientUser)
        }
      }

      exec(s"set ${SQLConf.ANSI_ENABLED.key}=true")
      exec(s"set ${SQLConf.ENFORCE_RESERVED_KEYWORDS.key}=true")
      userFuncs.foreach { func =>
        val opHandle2 = exec(s"select $func")
        assert(client.fetchResults(opHandle2)
          .getColumns.get(0).getStringVal.getValues.get(0) === clientUser)
      }

      userFuncs.foreach { func =>
        val e = intercept[HiveSQLException](exec(s"select $func()"))
        assert(e.getMessage.contains(func))
      }
    }
  }

  test("formats of error messages") {
    val sql = "select 1 / 0"
    withCLIServiceClient() { client =>
      val sessionHandle = client.openSession(user, "")
      val confOverlay = new java.util.HashMap[java.lang.String, java.lang.String]
      val exec: String => OperationHandle = client.executeStatement(sessionHandle, _, confOverlay)

      exec(s"set ${SQLConf.ANSI_ENABLED.key}=true")
      exec(s"set ${SQLConf.ERROR_MESSAGE_FORMAT.key}=${ErrorMessageFormat.PRETTY}")
      val e1 = intercept[HiveSQLException](exec(sql))
      // scalastyle:off line.size.limit
      assert(e1.getMessage ===
        """Error running query: [DIVIDE_BY_ZERO] org.apache.spark.SparkArithmeticException: [DIVIDE_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 and return NULL instead. If necessary set "spark.sql.ansi.enabled" to "false" to bypass this error. SQLSTATE: 22012
          |== SQL (line 1, position 8) ==
          |select 1 / 0
          |       ^^^^^
          |""".stripMargin)

      exec(s"set ${SQLConf.ERROR_MESSAGE_FORMAT.key}=${ErrorMessageFormat.MINIMAL}")
      val e2 = intercept[HiveSQLException](exec(sql))
      assert(e2.getMessage ===
        """{
          |  "errorClass" : "DIVIDE_BY_ZERO",
          |  "sqlState" : "22012",
          |  "messageParameters" : {
          |    "config" : "\"spark.sql.ansi.enabled\""
          |  },
          |  "queryContext" : [ {
          |    "objectType" : "",
          |    "objectName" : "",
          |    "startIndex" : 8,
          |    "stopIndex" : 12,
          |    "fragment" : "1 / 0"
          |  } ]
          |}""".stripMargin)

      exec(s"set ${SQLConf.ERROR_MESSAGE_FORMAT.key}=${ErrorMessageFormat.STANDARD}")
      val e3 = intercept[HiveSQLException](exec(sql))
      assert(e3.getMessage ===
        """{
          |  "errorClass" : "DIVIDE_BY_ZERO",
          |  "messageTemplate" : "Division by zero. Use `try_divide` to tolerate divisor being 0 and return NULL instead. If necessary set <config> to \"false\" to bypass this error.",
          |  "sqlState" : "22012",
          |  "messageParameters" : {
          |    "config" : "\"spark.sql.ansi.enabled\""
          |  },
          |  "queryContext" : [ {
          |    "objectType" : "",
          |    "objectName" : "",
          |    "startIndex" : 8,
          |    "stopIndex" : 12,
          |    "fragment" : "1 / 0"
          |  } ]
          |}""".stripMargin)
      // scalastyle:on line.size.limit
    }
  }

  test("SPARK-43119: Get SQL Keywords") {
    withCLIServiceClient() { client =>
      val sessionHandle = client.openSession(user, "")
      val infoValue = client.getInfo(sessionHandle, GetInfoType.CLI_ODBC_KEYWORDS)
      // scalastyle:off line.size.limit
      assert(infoValue.getStringValue == "ADD,AFTER,ALL,ALTER,ALWAYS,ANALYZE,AND,ANTI,ANY,ANY_VALUE,ARCHIVE,ARRAY,AS,ASC,AT,AUTHORIZATION,BEGIN,BETWEEN,BIGINT,BINARY,BINDING,BOOLEAN,BOTH,BUCKET,BUCKETS,BY,BYTE,CACHE,CALLED,CASCADE,CASE,CAST,CATALOG,CATALOGS,CHANGE,CHAR,CHARACTER,CHECK,CLEAR,CLUSTER,CLUSTERED,CODEGEN,COLLATE,COLLATION,COLLECTION,COLUMN,COLUMNS,COMMENT,COMMIT,COMPACT,COMPACTIONS,COMPENSATION,COMPUTE,CONCATENATE,CONDITION,CONSTRAINT,CONTAINS,CONTINUE,COST,CREATE,CROSS,CUBE,CURRENT,CURRENT_DATE,CURRENT_TIME,CURRENT_TIMESTAMP,CURRENT_USER,DATA,DATABASE,DATABASES,DATE,DATEADD,DATEDIFF,DATE_ADD,DATE_DIFF,DAY,DAYOFYEAR,DAYS,DBPROPERTIES,DEC,DECIMAL,DECLARE,DEFAULT,DEFINED,DEFINER,DELETE,DELIMITED,DESC,DESCRIBE,DETERMINISTIC,DFS,DIRECTORIES,DIRECTORY,DISTINCT,DISTRIBUTE,DIV,DO,DOUBLE,DROP,ELSE,END,ESCAPE,ESCAPED,EVOLUTION,EXCEPT,EXCHANGE,EXCLUDE,EXECUTE,EXISTS,EXIT,EXPLAIN,EXPORT,EXTENDED,EXTERNAL,EXTRACT,FALSE,FETCH,FIELDS,FILEFORMAT,FILTER,FIRST,FLOAT,FOLLOWING,FOR,FOREIGN,FORMAT,FORMATTED,FOUND,FROM,FULL,FUNCTION,FUNCTIONS,GENERATED,GLOBAL,GRANT,GROUP,GROUPING,HANDLER,HAVING,HOUR,HOURS,IDENTIFIER,IF,IGNORE,ILIKE,IMMEDIATE,IMPORT,IN,INCLUDE,INDEX,INDEXES,INNER,INPATH,INPUT,INPUTFORMAT,INSERT,INT,INTEGER,INTERSECT,INTERVAL,INTO,INVOKER,IS,ITEMS,JOIN,KEYS,LANGUAGE,LAST,LATERAL,LAZY,LEADING,LEFT,LIKE,LIMIT,LINES,LIST,LOAD,LOCAL,LOCATION,LOCK,LOCKS,LOGICAL,LONG,MACRO,MAP,MATCHED,MERGE,MICROSECOND,MICROSECONDS,MILLISECOND,MILLISECONDS,MINUS,MINUTE,MINUTES,MODIFIES,MONTH,MONTHS,MSCK,NAME,NAMESPACE,NAMESPACES,NANOSECOND,NANOSECONDS,NATURAL,NO,NONE,NOT,NULL,NULLS,NUMERIC,OF,OFFSET,ON,ONLY,OPTION,OPTIONS,OR,ORDER,OUT,OUTER,OUTPUTFORMAT,OVER,OVERLAPS,OVERLAY,OVERWRITE,PARTITION,PARTITIONED,PARTITIONS,PERCENT,PIVOT,PLACING,POSITION,PRECEDING,PRIMARY,PRINCIPALS,PROPERTIES,PURGE,QUARTER,QUERY,RANGE,READS,REAL,RECORDREADER,RECORDWRITER,RECOVER,REDUCE,REFERENCES,REFRESH,RENAME,REPAIR,REPEATABLE,REPLACE,RESET,RESPECT,RESTRICT,RETURN,RETURNS,REVOKE,RIGHT,ROLE,ROLES,ROLLBACK,ROLLUP,ROW,ROWS,SCHEMA,SCHEMAS,SECOND,SECONDS,SECURITY,SELECT,SEMI,SEPARATED,SERDE,SERDEPROPERTIES,SESSION_USER,SET,SETS,SHORT,SHOW,SINGLE,SKEWED,SMALLINT,SOME,SORT,SORTED,SOURCE,SPECIFIC,SQL,SQLEXCEPTION,START,STATISTICS,STORED,STRATIFY,STRING,STRUCT,SUBSTR,SUBSTRING,SYNC,SYSTEM_TIME,SYSTEM_VERSION,TABLE,TABLES,TABLESAMPLE,TARGET,TBLPROPERTIES,TERMINATED,THEN,TIME,TIMEDIFF,TIMESTAMP,TIMESTAMPADD,TIMESTAMPDIFF,TIMESTAMP_LTZ,TIMESTAMP_NTZ,TINYINT,TO,TOUCH,TRAILING,TRANSACTION,TRANSACTIONS,TRANSFORM,TRIM,TRUE,TRUNCATE,TRY_CAST,TYPE,UNARCHIVE,UNBOUNDED,UNCACHE,UNION,UNIQUE,UNKNOWN,UNLOCK,UNPIVOT,UNSET,UPDATE,USE,USER,USING,VALUES,VAR,VARCHAR,VARIABLE,VARIANT,VERSION,VIEW,VIEWS,VOID,WEEK,WEEKS,WHEN,WHERE,WHILE,WINDOW,WITH,WITHIN,X,YEAR,YEARS,ZONE")
      // scalastyle:on line.size.limit
    }
  }

  test("Support column display size for char/varchar") {
    withTable("t") {
      sql("CREATE TABLE t (c char(10), v varchar(11)) using parquet")

      withJdbcStatement() { stmt =>
        val rs = stmt.executeQuery("SELECT * FROM t")
        val metaData = rs.getMetaData
        assert(metaData.getColumnDisplaySize(1) === 10)
        assert(metaData.getColumnDisplaySize(2) === 11)
      }
    }
  }


  test("SPARK-43572: ResultSet supports TYPE_SCROLL_INSENSITIVE") {
    withJdbcStatement(ResultSet.TYPE_SCROLL_INSENSITIVE) { stmt =>
      val rs = stmt.executeQuery("SELECT * FROM RANGE(5)")
      (0 until 5).foreach { i =>
        rs.next()
        assert(rs.getInt(1) === i)
      }
      assert(!rs.next(), "the result set shall be exhausted")
      // Moves the cursor to the front of `rs`
      rs.beforeFirst()
      (0 until 5).foreach { i =>
        rs.next()
        assert(rs.getInt(1) === i)
      }

      val rs1 = stmt.getConnection.getMetaData.getSchemas
      rs1.next()
      assert(rs1.getString(1) === "default")
      assert(rs1.getType === ResultSet.TYPE_FORWARD_ONLY)
      assertThrows[SQLException](rs1.beforeFirst())
    }
  }

  test("SPARK-45454: Set table owner to current_user") {
    val testOwner = "test_table_owner"
    val tableName = "t"
    withTable(tableName) {
      withCLIServiceClient(testOwner) { client =>
        val sessionHandle = client.openSession(testOwner, "")
        val confOverlay = new java.util.HashMap[java.lang.String, java.lang.String]
        val exec: String => OperationHandle = client.executeStatement(sessionHandle, _, confOverlay)
        exec(s"CREATE TABLE $tableName(id int) using parquet")
        val owner = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName)).owner
        assert(owner === testOwner)
      }
    }
  }
}

class ThriftServerWithSparkContextInBinarySuite extends ThriftServerWithSparkContextSuite {
  override def mode: ServerMode.Value = ServerMode.binary
}

class ThriftServerWithSparkContextInHttpSuite extends ThriftServerWithSparkContextSuite {
  override def mode: ServerMode.Value = ServerMode.http
}
