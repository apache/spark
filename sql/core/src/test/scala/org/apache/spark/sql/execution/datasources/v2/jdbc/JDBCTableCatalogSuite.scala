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
package org.apache.spark.sql.execution.datasources.v2.jdbc

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.logging.log4j.Level

import org.apache.spark.{SparkConf, SparkIllegalArgumentException}
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.errors.DataTypeErrors.{toSQLConf, toSQLStmt}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class JDBCTableCatalogSuite extends QueryTest with SharedSparkSession {

  val tempDir = Utils.createTempDir()
  val url = s"jdbc:h2:${tempDir.getCanonicalPath};user=testUser;password=testPass"

  def defaultMetadata(dataType: DataType): Metadata = new MetadataBuilder()
    .putLong("scale", 0)
    .putBoolean("isTimestampNTZ", false)
    .putBoolean("isSigned", dataType.isInstanceOf[NumericType])
    .build()

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.h2", classOf[JDBCTableCatalog].getName)
    .set("spark.sql.catalog.h2.url", url)
    .set("spark.sql.catalog.h2.driver", "org.h2.Driver")

  private def withConnection[T](f: Connection => T): T = {
    val conn = DriverManager.getConnection(url, new Properties())
    try {
      f(conn)
    } finally {
      conn.close()
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    Utils.classForName("org.h2.Driver")
    withConnection { conn =>
      conn.prepareStatement("""CREATE SCHEMA "test"""").executeUpdate()
      conn.prepareStatement(
        """CREATE TABLE "test"."people" (name TEXT(32) NOT NULL, id INTEGER NOT NULL)""")
        .executeUpdate()
    }
  }

  override def afterAll(): Unit = {
    Utils.deleteRecursively(tempDir)
    super.afterAll()
  }

  test("show tables") {
    checkAnswer(sql("SHOW TABLES IN h2.test"), Seq(Row("test", "people", false)))
    // Check not existing namespace
    checkAnswer(sql("SHOW TABLES IN h2.bad_test"), Seq())
  }

  test("drop a table and test whether the table exists") {
    withConnection { conn =>
      conn.prepareStatement("""CREATE TABLE "test"."to_drop" (id INTEGER)""").executeUpdate()
    }
    checkAnswer(sql("SHOW TABLES IN h2.test"),
      Seq(Row("test", "to_drop", false), Row("test", "people", false)))
    sql("DROP TABLE h2.test.to_drop")
    checkAnswer(sql("SHOW TABLES IN h2.test"), Seq(Row("test", "people", false)))
    Seq(
      "h2.test.not_existing_table" -> "`h2`.`test`.`not_existing_table`",
      "h2.bad_test.not_existing_table" -> "`h2`.`bad_test`.`not_existing_table`"
    ).foreach { case (table, expected) =>
      val e = intercept[AnalysisException] {
        sql(s"DROP TABLE $table")
      }
      checkErrorTableNotFound(e, expected)
    }
  }

  test("rename a table") {
    withTable("h2.test.dst_table") {
      withConnection { conn =>
        conn.prepareStatement("""CREATE TABLE "test"."src_table" (id INTEGER)""").executeUpdate()
      }
      checkAnswer(
        sql("SHOW TABLES IN h2.test"),
        Seq(Row("test", "src_table", false), Row("test", "people", false)))
      sql("ALTER TABLE h2.test.src_table RENAME TO test.dst_table")
      checkAnswer(
        sql("SHOW TABLES IN h2.test"),
        Seq(Row("test", "dst_table", false), Row("test", "people", false)))
    }
    // Rename not existing table or namespace
    val exp1 = intercept[AnalysisException] {
      sql("ALTER TABLE h2.test.not_existing_table RENAME TO test.dst_table")
    }
    checkErrorTableNotFound(exp1, "`h2`.`test`.`not_existing_table`",
      ExpectedContext("h2.test.not_existing_table", 12, 11 + "h2.test.not_existing_table".length))
    val exp2 = intercept[AnalysisException] {
      sql("ALTER TABLE h2.bad_test.not_existing_table RENAME TO test.dst_table")
    }
    checkErrorTableNotFound(exp2, "`h2`.`bad_test`.`not_existing_table`",
      ExpectedContext("h2.bad_test.not_existing_table", 12,
        11 + "h2.bad_test.not_existing_table".length))
    // Rename to an existing table
    withTable("h2.test.dst_table") {
      withConnection { conn =>
        conn.prepareStatement("""CREATE TABLE "test"."dst_table" (id INTEGER)""").executeUpdate()
      }
      withTable("h2.test.src_table") {
        withConnection { conn =>
          conn.prepareStatement("""CREATE TABLE "test"."src_table" (id INTEGER)""").executeUpdate()
        }
        val exp = intercept[TableAlreadyExistsException] {
          sql("ALTER TABLE h2.test.src_table RENAME TO test.dst_table")
        }
        checkErrorTableAlreadyExists(exp, "`dst_table`")
      }
    }
  }

  test("load a table") {
    val t = spark.table("h2.test.people")
    val expectedSchema = new StructType()
      .add("NAME", VarcharType(32), true, defaultMetadata(VarcharType(32)))
      .add("ID", IntegerType, true, defaultMetadata(IntegerType))
    assert(t.schema === CharVarcharUtils.replaceCharVarcharWithStringInSchema(expectedSchema))
    Seq(
      "h2.test.not_existing_table" -> "`h2`.`test`.`not_existing_table`",
      "h2.bad_test.not_existing_table" -> "`h2`.`bad_test`.`not_existing_table`"
    ).foreach { case (table, expected) =>
      val e = intercept[AnalysisException] {
        spark.table(table).schema
      }
      checkErrorTableNotFound(e, expected)
    }
  }

  test("create a table") {
    withTable("h2.test.new_table") {
      sql("CREATE TABLE h2.test.new_table(i INT, j STRING)")
      checkAnswer(
        sql("SHOW TABLES IN h2.test"),
        Seq(Row("test", "people", false), Row("test", "new_table", false)))
    }
    withTable("h2.test.new_table") {
      sql("CREATE TABLE h2.test.new_table(i INT, j STRING)")
      val e = intercept[TableAlreadyExistsException] {
        sql("CREATE TABLE h2.test.new_table(i INT, j STRING)")
      }
      checkErrorTableAlreadyExists(e, "`test`.`new_table`")
    }
    val exp = intercept[NoSuchNamespaceException] {
      sql("CREATE TABLE h2.bad_test.new_table(i INT, j STRING)")
    }
    checkError(exp,
      condition = "SCHEMA_NOT_FOUND",
      parameters = Map("schemaName" -> "`bad_test`"))
  }

  test("ALTER TABLE ... add column") {
    val tableName = "h2.test.alt_table"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (ID INTEGER)")
      sql(s"ALTER TABLE $tableName ADD COLUMNS (C1 INTEGER, C2 STRING)")
      var t = spark.table(tableName)
      var expectedSchema = new StructType()
        .add("ID", IntegerType, true, defaultMetadata(IntegerType))
        .add("C1", IntegerType, true, defaultMetadata(IntegerType))
        .add("C2", StringType, true, defaultMetadata(StringType))
      assert(t.schema === expectedSchema)
      sql(s"ALTER TABLE $tableName ADD COLUMNS (c3 DOUBLE)")
      t = spark.table(tableName)
      expectedSchema = expectedSchema.add("c3", DoubleType, true, defaultMetadata(DoubleType))
      assert(t.schema === expectedSchema)
      // Add already existing column
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"ALTER TABLE $tableName ADD COLUMNS (c3 DOUBLE)")
        },
        condition = "FIELD_ALREADY_EXISTS",
        parameters = Map(
          "op" -> "add",
          "fieldNames" -> "`c3`",
          "struct" -> "\"STRUCT<ID: INT, C1: INT, C2: STRING, c3: DOUBLE>\""),
        context = ExpectedContext(
          fragment = s"ALTER TABLE $tableName ADD COLUMNS (c3 DOUBLE)",
          start = 0,
          stop = 52)
      )
    }
    // Add a column to not existing table and namespace
    Seq(
      "h2.test.not_existing_table" -> "`h2`.`test`.`not_existing_table`",
      "h2.bad_test.not_existing_table" -> "`h2`.`bad_test`.`not_existing_table`"
    ).foreach { case (table, expected) =>
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $table ADD COLUMNS (C4 STRING)")
      }
      checkErrorTableNotFound(e, expected,
        ExpectedContext(table, 12, 11 + table.length))
    }
  }

  test("ALTER TABLE ... rename column") {
    val tableName = "h2.test.alt_table"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (id INTEGER, C0 INTEGER)")
      sql(s"ALTER TABLE $tableName RENAME COLUMN id TO C")
      val t = spark.table(tableName)
      val expectedSchema = new StructType()
        .add("C", IntegerType, true, defaultMetadata(IntegerType))
        .add("C0", IntegerType, true, defaultMetadata(IntegerType))
      assert(t.schema === expectedSchema)
      // Rename to already existing column
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"ALTER TABLE $tableName RENAME COLUMN C TO C0")
        },
        condition = "FIELD_ALREADY_EXISTS",
        parameters = Map(
          "op" -> "rename",
          "fieldNames" -> "`C0`",
          "struct" -> "\"STRUCT<C: INT, C0: INT>\""),
        context = ExpectedContext(
          fragment = s"ALTER TABLE $tableName RENAME COLUMN C TO C0",
          start = 0,
          stop = 50)
      )
    }
    // Rename a column in not existing table and namespace
    Seq(
      "h2.test.not_existing_table" -> "`h2`.`test`.`not_existing_table`",
      "h2.bad_test.not_existing_table" -> "`h2`.`bad_test`.`not_existing_table`"
    ).foreach { case (table, expected) =>
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $table RENAME COLUMN ID TO C")
      }
      checkErrorTableNotFound(e, expected,
        ExpectedContext(table, 12, 11 + table.length))
    }
  }

  test("ALTER TABLE ... drop column") {
    val tableName = "h2.test.alt_table"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (C1 INTEGER, C2 INTEGER, c3 INTEGER)")
      sql(s"ALTER TABLE $tableName DROP COLUMN C1")
      sql(s"ALTER TABLE $tableName DROP COLUMN c3")
      val t = spark.table(tableName)
      val expectedSchema = new StructType()
        .add("C2", IntegerType, true, defaultMetadata(IntegerType))
      assert(t.schema === expectedSchema)
      // Drop not existing column
      val sqlText = s"ALTER TABLE $tableName DROP COLUMN bad_column"
      checkError(
        exception = intercept[AnalysisException] {
          sql(sqlText)
        },
        condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        sqlState = "42703",
        parameters = Map(
          "objectName" -> "`bad_column`",
          "proposal" -> "`C2`"),
        context = ExpectedContext(sqlText, 0, 51))
    }
    // Drop a column to not existing table and namespace
    Seq(
      "h2.test.not_existing_table" -> "`h2`.`test`.`not_existing_table`",
      "h2.bad_test.not_existing_table" -> "`h2`.`bad_test`.`not_existing_table`"
    ).foreach { case (table, expected) =>
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $table DROP COLUMN C1")
      }
      checkErrorTableNotFound(e, expected,
        ExpectedContext(table, 12, 11 + table.length))
    }
  }

  test("ALTER TABLE ... update column type") {
    val tableName = "h2.test.alt_table"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (ID INTEGER, deptno INTEGER)")
      sql(s"ALTER TABLE $tableName ALTER COLUMN id TYPE DOUBLE")
      sql(s"ALTER TABLE $tableName ALTER COLUMN deptno TYPE DOUBLE")
      val t = spark.table(tableName)
      val expectedSchema = new StructType()
        .add("ID", DoubleType, true, defaultMetadata(DoubleType))
        .add("deptno", DoubleType, true, defaultMetadata(DoubleType))
      assert(t.schema === expectedSchema)
      // Update not existing column
      val sqlText = s"ALTER TABLE $tableName ALTER COLUMN bad_column TYPE DOUBLE"
      checkError(
        exception = intercept[AnalysisException] {
          sql(sqlText)
        },
        condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        sqlState = "42703",
        parameters = Map(
          "objectName" -> "`bad_column`",
          "proposal" -> "`ID`, `deptno`"),
        context = ExpectedContext(sqlText, 0, 64))
      // Update column to wrong type
      checkError(
        exception = intercept[ParseException] {
          sql(s"ALTER TABLE $tableName ALTER COLUMN id TYPE bad_type")
        },
        condition = "UNSUPPORTED_DATATYPE",
        parameters = Map("typeName" -> "\"BAD_TYPE\""),
        context = ExpectedContext("bad_type", 51, 58))
    }
    // Update column type in not existing table and namespace
    Seq(
      "h2.test.not_existing_table" -> "`h2`.`test`.`not_existing_table`",
      "h2.bad_test.not_existing_table" -> "`h2`.`bad_test`.`not_existing_table`"
    ).foreach { case (table, expected) =>
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $table ALTER COLUMN id TYPE DOUBLE")
      }
      checkErrorTableNotFound(e, expected,
        ExpectedContext(table, 12, 11 + table.length))
    }
  }

  test("ALTER TABLE ... update column nullability") {
    val tableName = "h2.test.alt_table"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (ID INTEGER NOT NULL, deptno INTEGER NOT NULL)")
      sql(s"ALTER TABLE $tableName ALTER COLUMN ID DROP NOT NULL")
      sql(s"ALTER TABLE $tableName ALTER COLUMN deptno DROP NOT NULL")
      val t = spark.table(tableName)
      val expectedSchema = new StructType()
        .add("ID", IntegerType, true, defaultMetadata(IntegerType))
        .add("deptno", IntegerType, true, defaultMetadata(IntegerType))
      assert(t.schema === expectedSchema)
      // Update nullability of not existing column
      val sqlText = s"ALTER TABLE $tableName ALTER COLUMN bad_column DROP NOT NULL"
      checkError(
        exception = intercept[AnalysisException] {
          sql(sqlText)
        },
        condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        sqlState = "42703",
        parameters = Map(
          "objectName" -> "`bad_column`",
          "proposal" -> "`ID`, `deptno`"),
        context = ExpectedContext(sqlText, 0, 66))
    }
    // Update column nullability in not existing table and namespace
    Seq(
      "h2.test.not_existing_table" -> "`h2`.`test`.`not_existing_table`",
      "h2.bad_test.not_existing_table" -> "`h2`.`bad_test`.`not_existing_table`"
    ).foreach { case (table, expected) =>
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $table ALTER COLUMN ID DROP NOT NULL")
      }
      checkErrorTableNotFound(e, expected,
        ExpectedContext(table, 12, 11 + table.length))
    }
  }

  test("CREATE NAMESPACE with LOCATION for JDBC catalog should throw an error") {
    withTempDir { tempDir =>
      val url = s"jdbc:h2:${tempDir.getCanonicalPath};user=testUser;password=testPass"
      Utils.classForName("org.h2.Driver")
      withSQLConf(
        "spark.sql.catalog.h2" -> classOf[JDBCTableCatalog].getName,
        "spark.sql.catalog.h2.url" -> url,
        "spark.sql.catalog.h2.driver" -> "org.h2.Driver") {
        checkError(
          exception = intercept[AnalysisException] {
            sql("CREATE NAMESPACE h2.test_namespace LOCATION './samplepath'")
          },
          condition = "NOT_SUPPORTED_IN_JDBC_CATALOG.COMMAND",
          sqlState = "0A000",
          parameters = Map("cmd" -> toSQLStmt("CREATE NAMESPACE ... LOCATION ...")))
      }
    }
  }

  test("ALTER NAMESPACE with property other than COMMENT " +
    "for JDBC catalog should throw an exception") {
    withTempDir { tempDir =>
      val url = s"jdbc:h2:${tempDir.getCanonicalPath};user=testUser;password=testPass"
      Utils.classForName("org.h2.Driver")
      withSQLConf(
        "spark.sql.catalog.h2" -> classOf[JDBCTableCatalog].getName,
        "spark.sql.catalog.h2.url" -> url,
        "spark.sql.catalog.h2.driver" -> "org.h2.Driver") {
        val namespace = "h2.test_namespace"
        withNamespace(namespace) {
          sql(s"CREATE NAMESPACE $namespace")
          checkError(
            exception = intercept[AnalysisException] {
              sql(s"ALTER NAMESPACE h2.test_namespace SET LOCATION '/tmp/loc_test_2'")
            },
            condition = "NOT_SUPPORTED_IN_JDBC_CATALOG.COMMAND_WITH_PROPERTY",
            sqlState = "0A000",
            parameters = Map(
              "cmd" -> toSQLStmt("SET NAMESPACE"),
              "property" -> toSQLConf("location")))

          checkError(
            exception = intercept[AnalysisException] {
              sql(s"ALTER NAMESPACE h2.test_namespace SET PROPERTIES('a'='b')")
            },
            condition = "NOT_SUPPORTED_IN_JDBC_CATALOG.COMMAND_WITH_PROPERTY",
            sqlState = "0A000",
            parameters = Map(
              "cmd" -> toSQLStmt("SET NAMESPACE"),
              "property" -> toSQLConf("a")))
        }
      }
    }
  }

  test("ALTER TABLE ... update column comment not supported") {
    val tableName = "h2.test.alt_table"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (ID INTEGER)")
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"ALTER TABLE $tableName ALTER COLUMN ID COMMENT 'test'")
        },
        condition = "UNSUPPORTED_TABLE_CHANGE_IN_JDBC_CATALOG",
        parameters = Map(
          "change" -> "org.apache.spark.sql.connector.catalog.TableChange\\$UpdateColumnComment.*",
          "tableName" -> "`test`.`alt_table`"
        ),
        matchPVals = true)
      // Update comment for not existing column
      val sqlText = s"ALTER TABLE $tableName ALTER COLUMN bad_column COMMENT 'test'"
      checkError(
        exception = intercept[AnalysisException] {
          sql(sqlText)
        },
        condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        sqlState = "42703",
        parameters = Map(
          "objectName" -> "`bad_column`",
          "proposal" -> "`ID`"),
        context = ExpectedContext(sqlText, 0, 67))
    }
    // Update column comments in not existing table and namespace
    Seq(
      "h2.test.not_existing_table" -> "`h2`.`test`.`not_existing_table`",
      "h2.bad_test.not_existing_table" -> "`h2`.`bad_test`.`not_existing_table`"
    ).foreach { case (table, expected) =>
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $table ALTER COLUMN ID COMMENT 'test'")
      }
      checkErrorTableNotFound(e, expected,
        ExpectedContext(table, 12, 11 + table.length))
    }
  }

  test("ALTER TABLE case sensitivity") {
    val tableName = "h2.test.alt_table"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (c1 INTEGER NOT NULL, c2 INTEGER)")
      var t = spark.table(tableName)
      var expectedSchema = new StructType()
        .add("c1", IntegerType, true, defaultMetadata(IntegerType))
        .add("c2", IntegerType, true, defaultMetadata(IntegerType))
      assert(t.schema === expectedSchema)

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        val sqlText = s"ALTER TABLE $tableName RENAME COLUMN C2 TO c3"
        checkError(
          exception = intercept[AnalysisException] {
            sql(sqlText)
          },
          condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
          sqlState = "42703",
          parameters = Map(
            "objectName" -> "`C2`",
            "proposal" -> "`c1`, `c2`"),
          context = ExpectedContext(sqlText, 0, 51))
      }

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        sql(s"ALTER TABLE $tableName RENAME COLUMN C2 TO c3")
        expectedSchema = new StructType()
          .add("c1", IntegerType, true, defaultMetadata(IntegerType))
          .add("c3", IntegerType, true, defaultMetadata(IntegerType))
        t = spark.table(tableName)
        assert(t.schema === expectedSchema)
      }

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        val sqlText = s"ALTER TABLE $tableName DROP COLUMN C3"
        checkError(
          exception = intercept[AnalysisException] {
            sql(sqlText)
          },
          condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
          sqlState = "42703",
          parameters = Map(
            "objectName" -> "`C3`",
            "proposal" -> "`c1`, `c3`"),
          context = ExpectedContext(sqlText, 0, sqlText.length - 1))
      }

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        sql(s"ALTER TABLE $tableName DROP COLUMN C3")
        expectedSchema = new StructType()
          .add("c1", IntegerType, true, defaultMetadata(IntegerType))
        t = spark.table(tableName)
        assert(t.schema === expectedSchema)
      }

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        val sqlText = s"ALTER TABLE $tableName ALTER COLUMN C1 TYPE DOUBLE"
        checkError(
          exception = intercept[AnalysisException] {
            sql(sqlText)
          },
          condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
          sqlState = "42703",
          parameters = Map(
            "objectName" -> "`C1`",
            "proposal" -> "`c1`"),
          context = ExpectedContext(sqlText, 0, sqlText.length - 1))
      }

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        sql(s"ALTER TABLE $tableName ALTER COLUMN C1 TYPE DOUBLE")
        expectedSchema = new StructType()
          .add("c1", DoubleType, true, defaultMetadata(DoubleType))
        t = spark.table(tableName)
        assert(t.schema === expectedSchema)
      }

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        val sqlText = s"ALTER TABLE $tableName ALTER COLUMN C1 DROP NOT NULL"
        checkError(
          exception = intercept[AnalysisException] {
            sql(sqlText)
          },
          condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
          sqlState = "42703",
          parameters = Map(
            "objectName" -> "`C1`",
            "proposal" -> "`c1`"),
          context = ExpectedContext(sqlText, 0, sqlText.length - 1))
      }

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        sql(s"ALTER TABLE $tableName ALTER COLUMN C1 DROP NOT NULL")
        expectedSchema = new StructType()
          .add("c1", DoubleType, true, defaultMetadata(IntegerType))
        t = spark.table(tableName)
        assert(t.schema === expectedSchema)
      }
    }
  }

  test("CREATE TABLE with table comment") {
    withTable("h2.test.new_table") {
      val logAppender = new LogAppender("table comment")
      withLogAppender(logAppender) {
        sql("CREATE TABLE h2.test.new_table(i INT, j STRING) COMMENT 'this is a comment'")
      }
      val createCommentWarning = logAppender.loggingEvents
        .filter(_.getLevel == Level.WARN)
        .map(_.getMessage.getFormattedMessage)
        .exists(_.contains("Cannot create JDBC table comment"))
      assert(createCommentWarning === false)
    }
  }

  test("CREATE TABLE with table property") {
    withTable("h2.test.new_table") {
      checkErrorMatchPVals(
        exception = intercept[AnalysisException] {
          sql("CREATE TABLE h2.test.new_table(i INT, j STRING)" +
            " TBLPROPERTIES('ENGINE'='tableEngineName')")
        },
        condition = "FAILED_JDBC.CREATE_TABLE",
        parameters = Map(
          "url" -> "jdbc:.*",
          "tableName" -> "`test`.`new_table`"))
    }
  }

  test("SPARK-42904: CREATE TABLE with char/varchar") {
    withTable("h2.test.new_table") {
      sql("CREATE TABLE h2.test.new_table(c CHAR(10), v VARCHAR(100))")
      checkAnswer(sql("SHOW TABLES IN h2.test LIKE 'new*'"), Row("test", "new_table", false))
    }
  }

  test("SPARK-42904: CREATE TABLE with char/varchar with invalid char length") {
    checkErrorMatchPVals(
      exception = intercept[AnalysisException]{
        sql("CREATE TABLE h2.test.new_table(c CHAR(1000000001))")
      },
      condition = "FAILED_JDBC.CREATE_TABLE",
      parameters = Map(
        "url" -> "jdbc:.*",
        "tableName" -> "`test`.`new_table`"))
  }

  test("SPARK-42955: Skip classifyException and wrap AnalysisException for SparkThrowable") {
    checkError(
      exception = intercept[SparkIllegalArgumentException](
        sql("CREATE TABLE h2.test.new_table(c array<int>)")
      ),
      condition = "_LEGACY_ERROR_TEMP_2082",
      parameters = Map("catalogString" -> "array<int>")
    )
  }

  test("SPARK-42916: Keep Char/Varchar meta information on the read-side") {
    val tableName = "h2.test.alt_table"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (ID CHAR(10), deptno VARCHAR(20))")
      sql(s"ALTER TABLE $tableName ALTER COLUMN deptno TYPE VARCHAR(30)")
      val t = spark.table(tableName)
      val expected = new StructType()
        .add("ID", CharType(10), true, defaultMetadata(CharType(10)))
        .add("deptno", VarcharType(30), true, defaultMetadata(VarcharType(30)))
      val replaced = CharVarcharUtils.replaceCharVarcharWithStringInSchema(expected)
      assert(t.schema === replaced)
    }
  }

  test("SPARK-46822: Respect charVarcharAsString when casting jdbc type to catalyst type in jdbc") {
    try {
      withConnection(
        _.prepareStatement("""CREATE TABLE "test"."char_tbl" (ID CHAR(5), deptno VARCHAR(10))""")
        .executeUpdate())
      withSQLConf(SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING.key -> "true") {
        val expected = new StructType()
          .add("ID", StringType, true, defaultMetadata(StringType))
          .add("DEPTNO", StringType, true, defaultMetadata(StringType))
        assert(sql(s"SELECT * FROM h2.test.char_tbl").schema === expected)
      }
      val expected = new StructType()
        .add("ID", CharType(5), true, defaultMetadata(CharType(5)))
        .add("DEPTNO", VarcharType(10), true, defaultMetadata(VarcharType(10)))
      val replaced = CharVarcharUtils.replaceCharVarcharWithStringInSchema(expected)
      assert(sql(s"SELECT * FROM h2.test.char_tbl").schema === replaced)
    } finally {
      withConnection(
        _.prepareStatement("""DROP TABLE IF EXISTS "test"."char_tbl"""").executeUpdate())
    }
  }

  test("SPARK-45449: Cache Invalidation Issue with JDBC Table") {
    withTable("h2.test.cache_t") {
      withConnection { conn =>
        conn.prepareStatement(
          """CREATE TABLE "test"."cache_t" (id decimal(25) PRIMARY KEY NOT NULL,
            |name TEXT(32) NOT NULL)""".stripMargin).executeUpdate()
      }
      sql("INSERT OVERWRITE h2.test.cache_t SELECT 1 AS id, 'a' AS name")
      sql("CACHE TABLE t1 SELECT id, name FROM h2.test.cache_t")
      val plan = sql("select * from t1").queryExecution.sparkPlan
      assert(plan.isInstanceOf[InMemoryTableScanExec])
    }
  }
}
