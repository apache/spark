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

package org.apache.spark.sql.execution.command

import java.util.Locale

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, GlobalTempView, LocalTempView, UnresolvedAttribute, UnresolvedDBObjectName}
import org.apache.spark.sql.catalyst.catalog.{ArchiveResource, FileResource, FunctionResource, JarResource}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans
import org.apache.spark.sql.catalyst.dsl.plans.DslLogicalPlan
import org.apache.spark.sql.catalyst.expressions.JsonTuple
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.test.SharedSparkSession

class DDLParserSuite extends AnalysisTest with SharedSparkSession {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
  private lazy val parser = new SparkSqlParser()

  private def assertUnsupported(sql: String, containsThesePhrases: Seq[String] = Seq()): Unit = {
    val e = intercept[ParseException] {
      parser.parsePlan(sql)
    }
    assert(e.getMessage.toLowerCase(Locale.ROOT).contains("operation not allowed"))
    containsThesePhrases.foreach { p =>
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains(p.toLowerCase(Locale.ROOT)))
    }
  }

  private def intercept(sqlCommand: String, messages: String*): Unit =
    interceptParseException(parser.parsePlan)(sqlCommand, messages: _*)

  private def compareTransformQuery(sql: String, expected: LogicalPlan): Unit = {
    val plan = parser.parsePlan(sql).asInstanceOf[ScriptTransformation].copy(ioschema = null)
    comparePlans(plan, expected, checkAnalysis = false)
  }

  test("show current namespace") {
    comparePlans(
      parser.parsePlan("SHOW CURRENT NAMESPACE"),
      ShowCurrentNamespaceCommand())
  }

  test("alter database - property values must be set") {
    assertUnsupported(
      sql = "ALTER DATABASE my_db SET DBPROPERTIES('key_without_value', 'key_with_value'='x')",
      containsThesePhrases = Seq("key_without_value"))
  }

  test("insert overwrite directory") {
    val v1 = "INSERT OVERWRITE DIRECTORY '/tmp/file' USING parquet SELECT 1 as a"
    parser.parsePlan(v1) match {
      case InsertIntoDir(_, storage, provider, query, overwrite) =>
        assert(storage.locationUri.isDefined && storage.locationUri.get.toString == "/tmp/file")
      case other =>
        fail(s"Expected to parse ${classOf[InsertIntoDataSourceDirCommand].getClass.getName}" +
          " from query," + s" got ${other.getClass.getName}: $v1")
    }

    val v2 = "INSERT OVERWRITE DIRECTORY USING parquet SELECT 1 as a"
    val e2 = intercept[ParseException] {
      parser.parsePlan(v2)
    }
    assert(e2.message.contains(
      "Directory path and 'path' in OPTIONS should be specified one, but not both"))

    val v3 =
      """
        | INSERT OVERWRITE DIRECTORY USING json
        | OPTIONS ('path' '/tmp/file', a 1, b 0.1, c TRUE)
        | SELECT 1 as a
      """.stripMargin
    parser.parsePlan(v3) match {
      case InsertIntoDir(_, storage, provider, query, overwrite) =>
        assert(storage.locationUri.isDefined && provider == Some("json"))
        assert(storage.properties.get("a") == Some("1"))
        assert(storage.properties.get("b") == Some("0.1"))
        assert(storage.properties.get("c") == Some("true"))
        assert(!storage.properties.contains("abc"))
        assert(!storage.properties.contains("path"))
      case other =>
        fail(s"Expected to parse ${classOf[InsertIntoDataSourceDirCommand].getClass.getName}" +
          " from query," + s"got ${other.getClass.getName}: $v1")
    }

    val v4 =
      """
        | INSERT OVERWRITE DIRECTORY '/tmp/file' USING json
        | OPTIONS ('path' '/tmp/file', a 1, b 0.1, c TRUE)
        | SELECT 1 as a
      """.stripMargin
    val e4 = intercept[ParseException] {
      parser.parsePlan(v4)
    }
    assert(e4.message.contains(
      "Directory path and 'path' in OPTIONS should be specified one, but not both"))
  }

  test("alter table - property values must be set") {
    assertUnsupported(
      sql = "ALTER TABLE my_tab SET TBLPROPERTIES('key_without_value', 'key_with_value'='x')",
      containsThesePhrases = Seq("key_without_value"))
  }

  test("alter table unset properties - property values must NOT be set") {
    assertUnsupported(
      sql = "ALTER TABLE my_tab UNSET TBLPROPERTIES('key_without_value', 'key_with_value'='x')",
      containsThesePhrases = Seq("key_with_value"))
  }

  test("alter table - SerDe property values must be set") {
    assertUnsupported(
      sql = "ALTER TABLE my_tab SET SERDE 'serde' " +
        "WITH SERDEPROPERTIES('key_without_value', 'key_with_value'='x')",
      containsThesePhrases = Seq("key_without_value"))
  }

  test("alter table: exchange partition (not supported)") {
    assertUnsupported(
      """
       |ALTER TABLE table_name_1 EXCHANGE PARTITION
       |(dt='2008-08-08', country='us') WITH TABLE table_name_2
      """.stripMargin)
  }

  test("alter table: archive partition (not supported)") {
    assertUnsupported("ALTER TABLE table_name ARCHIVE PARTITION (dt='2008-08-08', country='us')")
  }

  test("alter table: unarchive partition (not supported)") {
    assertUnsupported("ALTER TABLE table_name UNARCHIVE PARTITION (dt='2008-08-08', country='us')")
  }

  test("alter table: set file format (not allowed)") {
    assertUnsupported(
      "ALTER TABLE table_name SET FILEFORMAT INPUTFORMAT 'test' OUTPUTFORMAT 'test'")
    assertUnsupported(
      "ALTER TABLE table_name PARTITION (dt='2008-08-08', country='us') " +
        "SET FILEFORMAT PARQUET")
  }

  test("alter table: touch (not supported)") {
    assertUnsupported("ALTER TABLE table_name TOUCH")
    assertUnsupported("ALTER TABLE table_name TOUCH PARTITION (dt='2008-08-08', country='us')")
  }

  test("alter table: compact (not supported)") {
    assertUnsupported("ALTER TABLE table_name COMPACT 'compaction_type'")
    assertUnsupported(
      """
        |ALTER TABLE table_name PARTITION (dt='2008-08-08', country='us')
        |COMPACT 'MAJOR'
      """.stripMargin)
  }

  test("alter table: concatenate (not supported)") {
    assertUnsupported("ALTER TABLE table_name CONCATENATE")
    assertUnsupported(
      "ALTER TABLE table_name PARTITION (dt='2008-08-08', country='us') CONCATENATE")
  }

  test("alter table: cluster by (not supported)") {
    assertUnsupported(
      "ALTER TABLE table_name CLUSTERED BY (col_name) SORTED BY (col2_name) INTO 3 BUCKETS")
    assertUnsupported("ALTER TABLE table_name CLUSTERED BY (col_name) INTO 3 BUCKETS")
    assertUnsupported("ALTER TABLE table_name NOT CLUSTERED")
    assertUnsupported("ALTER TABLE table_name NOT SORTED")
  }

  test("alter table: skewed by (not supported)") {
    assertUnsupported("ALTER TABLE table_name NOT SKEWED")
    assertUnsupported("ALTER TABLE table_name NOT STORED AS DIRECTORIES")
    assertUnsupported("ALTER TABLE table_name SET SKEWED LOCATION (col_name1=\"location1\"")
    assertUnsupported("ALTER TABLE table_name SKEWED BY (key) ON (1,5,6) STORED AS DIRECTORIES")
  }

  test("alter table: replace columns (not allowed)") {
    assertUnsupported(
      """
       |ALTER TABLE table_name REPLACE COLUMNS (new_col1 INT
       |COMMENT 'test_comment', new_col2 LONG COMMENT 'test_comment2') RESTRICT
      """.stripMargin)
  }

  test("SPARK-14383: DISTRIBUTE and UNSET as non-keywords") {
    val sql = "SELECT distribute, unset FROM x"
    val parsed = parser.parsePlan(sql)
    assert(parsed.isInstanceOf[Project])
  }

  test("duplicate keys in table properties") {
    val e = intercept[ParseException] {
      parser.parsePlan("ALTER TABLE dbx.tab1 SET TBLPROPERTIES ('key1' = '1', 'key1' = '2')")
    }.getMessage
    assert(e.contains("Found duplicate keys 'key1'"))
  }

  test("duplicate columns in partition specs") {
    val e = intercept[ParseException] {
      parser.parsePlan(
        "ALTER TABLE dbx.tab1 PARTITION (a='1', a='2') RENAME TO PARTITION (a='100', a='200')")
    }.getMessage
    assert(e.contains("Found duplicate keys 'a'"))
  }

  test("unsupported operations") {
    intercept[ParseException] {
      parser.parsePlan(
        """
          |CREATE TEMPORARY TABLE ctas2
          |ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe"
          |WITH SERDEPROPERTIES("serde_p1"="p1","serde_p2"="p2")
          |STORED AS RCFile
          |TBLPROPERTIES("tbl_p1"="p11", "tbl_p2"="p22")
          |AS SELECT key, value FROM src ORDER BY key, value
        """.stripMargin)
    }
    intercept[ParseException] {
      parser.parsePlan(
        """
          |CREATE TABLE user_info_bucketed(user_id BIGINT, firstname STRING, lastname STRING)
          |CLUSTERED BY(user_id) INTO 256 BUCKETS
          |AS SELECT key, value FROM src ORDER BY key, value
        """.stripMargin)
    }
    intercept[ParseException] {
      parser.parsePlan(
        """
          |CREATE TABLE user_info_bucketed(user_id BIGINT, firstname STRING, lastname STRING)
          |SKEWED BY (key) ON (1,5,6)
          |AS SELECT key, value FROM src ORDER BY key, value
        """.stripMargin)
    }
    intercept[ParseException] {
      parser.parsePlan(
        """
          |SELECT TRANSFORM (key, value) USING 'cat' AS (tKey, tValue)
          |ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.TypedBytesSerDe'
          |RECORDREADER 'org.apache.hadoop.hive.contrib.util.typedbytes.TypedBytesRecordReader'
          |FROM testData
        """.stripMargin)
    }
  }

  test("Invalid interval term should throw AnalysisException") {
    def assertError(sql: String, errorMessage: String): Unit = {
      val e = intercept[AnalysisException] {
        parser.parsePlan(sql)
      }
      assert(e.getMessage.contains(errorMessage))
    }
    assertError("select interval '42-32' year to month",
      "month 32 outside range [0, 11]")
    assertError("select interval '5 49:12:15' day to second",
      "hour 49 outside range [0, 23]")
    assertError("select interval '23:61:15' hour to second",
      "minute 61 outside range [0, 59]")
    assertError("select interval '.1111111111' second",
      "'.1111111111' is out of range")
  }

  test("use native json_tuple instead of hive's UDTF in LATERAL VIEW") {
    val analyzer = spark.sessionState.analyzer
    val plan = analyzer.execute(parser.parsePlan(
      """
        |SELECT *
        |FROM (SELECT '{"f1": "value1", "f2": 12}' json) test
        |LATERAL VIEW json_tuple(json, 'f1', 'f2') jt AS a, b
      """.stripMargin))

    assert(plan.children.head.asInstanceOf[Generate].generator.isInstanceOf[JsonTuple])
  }

  test("transform query spec") {
    val p = Project(Seq(UnresolvedAttribute("a"), UnresolvedAttribute("b")), plans.table("e"))
    val s = ScriptTransformation("func", Seq.empty, p, null)

    compareTransformQuery("select transform(a, b) using 'func' from e where f < 10",
      s.copy(child = p.copy(child = p.child.where('f < 10)),
        output = Seq('key.string, 'value.string)))
    compareTransformQuery("map a, b using 'func' as c, d from e",
      s.copy(output = Seq('c.string, 'd.string)))
    compareTransformQuery("reduce a, b using 'func' as (c int, d decimal(10, 0)) from e",
      s.copy(output = Seq('c.int, 'd.decimal(10, 0))))
  }

  test("use backticks in output of Script Transform") {
    parser.parsePlan(
      """SELECT `t`.`thing1`
        |FROM (SELECT TRANSFORM (`parquet_t1`.`key`, `parquet_t1`.`value`)
        |USING 'cat' AS (`thing1` int, `thing2` string) FROM `default`.`parquet_t1`) AS t
      """.stripMargin)
  }

  test("use backticks in output of Generator") {
    parser.parsePlan(
      """
        |SELECT `gentab2`.`gencol2`
        |FROM `default`.`src`
        |LATERAL VIEW explode(array(array(1, 2, 3))) `gentab1` AS `gencol1`
        |LATERAL VIEW explode(`gentab1`.`gencol1`) `gentab2` AS `gencol2`
      """.stripMargin)
  }

  test("use escaped backticks in output of Generator") {
    parser.parsePlan(
      """
        |SELECT `gen``tab2`.`gen``col2`
        |FROM `default`.`src`
        |LATERAL VIEW explode(array(array(1, 2,  3))) `gen``tab1` AS `gen``col1`
        |LATERAL VIEW explode(`gen``tab1`.`gen``col1`) `gen``tab2` AS `gen``col2`
      """.stripMargin)
  }

  test("create view -- basic") {
    val v1 = "CREATE VIEW view1 AS SELECT * FROM tab1"
    val parsed1 = parser.parsePlan(v1)

    val expected1 = CreateView(
      UnresolvedDBObjectName(Seq("view1"), false),
      Seq.empty[(String, Option[String])],
      None,
      Map.empty[String, String],
      Some("SELECT * FROM tab1"),
      parser.parsePlan("SELECT * FROM tab1"),
      false,
      false)
    comparePlans(parsed1, expected1)

    val v2 = "CREATE TEMPORARY VIEW a AS SELECT * FROM tab1"
    val parsed2 = parser.parsePlan(v2)

    val expected2 = CreateViewCommand(
      Seq("a").asTableIdentifier,
      Seq.empty[(String, Option[String])],
      None,
      Map.empty[String, String],
      Some("SELECT * FROM tab1"),
      parser.parsePlan("SELECT * FROM tab1"),
      false,
      false,
      LocalTempView)
    comparePlans(parsed2, expected2)
  }

  test("create temp view - full") {
    val v1 =
      """
        |CREATE OR REPLACE VIEW view1
        |(col1, col3 COMMENT 'hello')
        |TBLPROPERTIES('prop1Key'="prop1Val")
        |COMMENT 'BLABLA'
        |AS SELECT * FROM tab1
      """.stripMargin
    val parsed1 = parser.parsePlan(v1)
    val expected1 = CreateView(
      UnresolvedDBObjectName(Seq("view1"), false),
      Seq("col1" -> None, "col3" -> Some("hello")),
      Some("BLABLA"),
      Map("prop1Key" -> "prop1Val"),
      Some("SELECT * FROM tab1"),
      parser.parsePlan("SELECT * FROM tab1"),
      false,
      true)
    comparePlans(parsed1, expected1)

    val v2 =
      """
        |CREATE OR REPLACE GLOBAL TEMPORARY VIEW a
        |(col1, col3 COMMENT 'hello')
        |COMMENT 'BLABLA'
        |AS SELECT * FROM tab1
          """.stripMargin
    val parsed2 = parser.parsePlan(v2)
    val expected2 = CreateViewCommand(
      Seq("a").asTableIdentifier,
      Seq("col1" -> None, "col3" -> Some("hello")),
      Some("BLABLA"),
      Map(),
      Some("SELECT * FROM tab1"),
      parser.parsePlan("SELECT * FROM tab1"),
      false,
      true,
      GlobalTempView)
    comparePlans(parsed2, expected2)
  }

  test("create view -- partitioned view") {
    val v1 = "CREATE VIEW view1 partitioned on (ds, hr) as select * from srcpart"
    intercept[ParseException] {
      parser.parsePlan(v1)
    }
  }

  test("create view - duplicate clauses") {
    def createViewStatement(duplicateClause: String): String = {
      s"""
         |CREATE OR REPLACE VIEW view1
         |(col1, col3 COMMENT 'hello')
         |$duplicateClause
         |$duplicateClause
         |AS SELECT * FROM tab1
      """.stripMargin
    }
    val sql1 = createViewStatement("COMMENT 'BLABLA'")
    val sql2 = createViewStatement("TBLPROPERTIES('prop1Key'=\"prop1Val\")")
    intercept(sql1, "Found duplicate clauses: COMMENT")
    intercept(sql2, "Found duplicate clauses: TBLPROPERTIES")
  }

  test("CREATE FUNCTION") {
    comparePlans(parser.parsePlan("CREATE FUNCTION a as 'fun'"),
      CreateFunction(UnresolvedDBObjectName(Seq("a"), false), "fun", Seq(), false, false))

    comparePlans(parser.parsePlan("CREATE FUNCTION a.b.c as 'fun'"),
      CreateFunction(UnresolvedDBObjectName(Seq("a", "b", "c"), false), "fun", Seq(), false, false))

    comparePlans(parser.parsePlan("CREATE OR REPLACE FUNCTION a.b.c as 'fun'"),
      CreateFunction(UnresolvedDBObjectName(Seq("a", "b", "c"), false), "fun", Seq(), false, true))

    comparePlans(parser.parsePlan("CREATE TEMPORARY FUNCTION a as 'fun'"),
      CreateFunctionCommand(None, "a", "fun", Seq(), true, false, false))

    comparePlans(parser.parsePlan("CREATE FUNCTION IF NOT EXISTS a.b.c as 'fun'"),
      CreateFunction(UnresolvedDBObjectName(Seq("a", "b", "c"), false), "fun", Seq(), true, false))

    comparePlans(parser.parsePlan("CREATE FUNCTION a as 'fun' USING JAR 'j'"),
      CreateFunction(UnresolvedDBObjectName(Seq("a"), false), "fun",
        Seq(FunctionResource(JarResource, "j")), false, false))

    comparePlans(parser.parsePlan("CREATE FUNCTION a as 'fun' USING ARCHIVE 'a'"),
      CreateFunction(UnresolvedDBObjectName(Seq("a"), false), "fun",
        Seq(FunctionResource(ArchiveResource, "a")), false, false))

    comparePlans(parser.parsePlan("CREATE FUNCTION a as 'fun' USING FILE 'f'"),
      CreateFunction(UnresolvedDBObjectName(Seq("a"), false), "fun",
        Seq(FunctionResource(FileResource, "f")), false, false))

    comparePlans(
      parser.parsePlan("CREATE FUNCTION a as 'fun' USING JAR 'j', ARCHIVE 'a', FILE 'f'"),
      CreateFunction(UnresolvedDBObjectName(Seq("a"), false), "fun",
        Seq(FunctionResource(JarResource, "j"),
          FunctionResource(ArchiveResource, "a"), FunctionResource(FileResource, "f")),
        false, false))

    intercept("CREATE FUNCTION a as 'fun' USING OTHER 'o'",
      "Operation not allowed: CREATE FUNCTION with resource type 'other'")
  }

  test("SPARK-32374: create temporary view with properties not allowed") {
    assertUnsupported(
      sql = """
              |CREATE OR REPLACE TEMPORARY VIEW a.b.c
              |(col1, col3 COMMENT 'hello')
              |TBLPROPERTIES('prop1Key'="prop1Val")
              |AS SELECT * FROM tab1
      """.stripMargin,
      containsThesePhrases = Seq("TBLPROPERTIES can't coexist with CREATE TEMPORARY VIEW"))
  }

  test("create table like") {
    val v1 = "CREATE TABLE table1 LIKE table2"
    val (target, source, fileFormat, provider, properties, exists) =
      parser.parsePlan(v1).collect {
        case CreateTableLikeCommand(t, s, f, p, pr, e) => (t, s, f, p, pr, e)
      }.head
    assert(exists == false)
    assert(target.database.isEmpty)
    assert(target.table == "table1")
    assert(source.database.isEmpty)
    assert(source.table == "table2")
    assert(fileFormat.locationUri.isEmpty)
    assert(provider.isEmpty)

    val v2 = "CREATE TABLE IF NOT EXISTS table1 LIKE table2"
    val (target2, source2, fileFormat2, provider2, properties2, exists2) =
      parser.parsePlan(v2).collect {
        case CreateTableLikeCommand(t, s, f, p, pr, e) => (t, s, f, p, pr, e)
      }.head
    assert(exists2)
    assert(target2.database.isEmpty)
    assert(target2.table == "table1")
    assert(source2.database.isEmpty)
    assert(source2.table == "table2")
    assert(fileFormat2.locationUri.isEmpty)
    assert(provider2.isEmpty)

    val v3 = "CREATE TABLE table1 LIKE table2 LOCATION '/spark/warehouse'"
    val (target3, source3, fileFormat3, provider3, properties3, exists3) =
      parser.parsePlan(v3).collect {
        case CreateTableLikeCommand(t, s, f, p, pr, e) => (t, s, f, p, pr, e)
      }.head
    assert(!exists3)
    assert(target3.database.isEmpty)
    assert(target3.table == "table1")
    assert(source3.database.isEmpty)
    assert(source3.table == "table2")
    assert(fileFormat3.locationUri.map(_.toString) == Some("/spark/warehouse"))
    assert(provider3.isEmpty)

    val v4 = "CREATE TABLE IF NOT EXISTS table1 LIKE table2 LOCATION '/spark/warehouse'"
    val (target4, source4, fileFormat4, provider4, properties4, exists4) =
      parser.parsePlan(v4).collect {
        case CreateTableLikeCommand(t, s, f, p, pr, e) => (t, s, f, p, pr, e)
      }.head
    assert(exists4)
    assert(target4.database.isEmpty)
    assert(target4.table == "table1")
    assert(source4.database.isEmpty)
    assert(source4.table == "table2")
    assert(fileFormat4.locationUri.map(_.toString) == Some("/spark/warehouse"))
    assert(provider4.isEmpty)

    val v5 = "CREATE TABLE IF NOT EXISTS table1 LIKE table2 USING parquet"
    val (target5, source5, fileFormat5, provider5, properties5, exists5) =
      parser.parsePlan(v5).collect {
        case CreateTableLikeCommand(t, s, f, p, pr, e) => (t, s, f, p, pr, e)
      }.head
    assert(exists5)
    assert(target5.database.isEmpty)
    assert(target5.table == "table1")
    assert(source5.database.isEmpty)
    assert(source5.table == "table2")
    assert(fileFormat5.locationUri.isEmpty)
    assert(provider5 == Some("parquet"))

    val v6 = "CREATE TABLE IF NOT EXISTS table1 LIKE table2 USING ORC"
    val (target6, source6, fileFormat6, provider6, properties6, exists6) =
      parser.parsePlan(v6).collect {
        case CreateTableLikeCommand(t, s, f, p, pr, e) => (t, s, f, p, pr, e)
      }.head
    assert(exists6)
    assert(target6.database.isEmpty)
    assert(target6.table == "table1")
    assert(source6.database.isEmpty)
    assert(source6.table == "table2")
    assert(fileFormat6.locationUri.isEmpty)
    assert(provider6 == Some("ORC"))
  }

  test("SET CATALOG") {
    comparePlans(
      parser.parsePlan("SET CATALOG abc"),
      SetCatalogCommand("abc"))
    comparePlans(
      parser.parsePlan("SET CATALOG 'a b c'"),
      SetCatalogCommand("a b c"))
    comparePlans(
      parser.parsePlan("SET CATALOG `a b c`"),
      SetCatalogCommand("a b c"))
  }
}
