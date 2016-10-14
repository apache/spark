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

package org.apache.spark.sql.hive

import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans
import org.apache.spark.sql.catalyst.dsl.plans.DslLogicalPlan
import org.apache.spark.sql.catalyst.expressions.JsonTuple
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Generate, ScriptTransformation}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.types.StructType

class HiveDDLCommandSuite extends PlanTest {
  val parser = TestHive.sessionState.sqlParser

  private def extractTableDesc(sql: String): (CatalogTable, Boolean) = {
    parser.parsePlan(sql).collect {
      case CreateTable(tableDesc, mode, _) => (tableDesc, mode == SaveMode.Ignore)
    }.head
  }

  private def assertUnsupported(sql: String): Unit = {
    val e = intercept[ParseException] {
      parser.parsePlan(sql)
    }
    assert(e.getMessage.toLowerCase.contains("operation not allowed"))
  }

  test("Test CTAS #1") {
    val s1 =
      """CREATE EXTERNAL TABLE IF NOT EXISTS mydb.page_view
        |COMMENT 'This is the staging page view table'
        |STORED AS RCFILE
        |LOCATION '/user/external/page_view'
        |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
        |AS SELECT * FROM src""".stripMargin

    val (desc, exists) = extractTableDesc(s1)
    assert(exists)
    assert(desc.identifier.database == Some("mydb"))
    assert(desc.identifier.table == "page_view")
    assert(desc.tableType == CatalogTableType.EXTERNAL)
    assert(desc.storage.locationUri == Some("/user/external/page_view"))
    assert(desc.schema.isEmpty) // will be populated later when the table is actually created
    assert(desc.comment == Some("This is the staging page view table"))
    // TODO will be SQLText
    assert(desc.viewText.isEmpty)
    assert(desc.viewOriginalText.isEmpty)
    assert(desc.partitionColumnNames.isEmpty)
    assert(desc.storage.inputFormat == Some("org.apache.hadoop.hive.ql.io.RCFileInputFormat"))
    assert(desc.storage.outputFormat == Some("org.apache.hadoop.hive.ql.io.RCFileOutputFormat"))
    assert(desc.storage.serde ==
      Some("org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe"))
    assert(desc.properties == Map(("p1", "v1"), ("p2", "v2")))
  }

  test("Test CTAS #2") {
    val s2 =
      """CREATE EXTERNAL TABLE IF NOT EXISTS mydb.page_view
        |COMMENT 'This is the staging page view table'
        |ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
        | STORED AS
        | INPUTFORMAT 'parquet.hive.DeprecatedParquetInputFormat'
        | OUTPUTFORMAT 'parquet.hive.DeprecatedParquetOutputFormat'
        |LOCATION '/user/external/page_view'
        |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
        |AS SELECT * FROM src""".stripMargin

    val (desc, exists) = extractTableDesc(s2)
    assert(exists)
    assert(desc.identifier.database == Some("mydb"))
    assert(desc.identifier.table == "page_view")
    assert(desc.tableType == CatalogTableType.EXTERNAL)
    assert(desc.storage.locationUri == Some("/user/external/page_view"))
    assert(desc.schema.isEmpty) // will be populated later when the table is actually created
    // TODO will be SQLText
    assert(desc.comment == Some("This is the staging page view table"))
    assert(desc.viewText.isEmpty)
    assert(desc.viewOriginalText.isEmpty)
    assert(desc.partitionColumnNames.isEmpty)
    assert(desc.storage.properties == Map())
    assert(desc.storage.inputFormat == Some("parquet.hive.DeprecatedParquetInputFormat"))
    assert(desc.storage.outputFormat == Some("parquet.hive.DeprecatedParquetOutputFormat"))
    assert(desc.storage.serde == Some("parquet.hive.serde.ParquetHiveSerDe"))
    assert(desc.properties == Map(("p1", "v1"), ("p2", "v2")))
  }

  test("Test CTAS #3") {
    val s3 = """CREATE TABLE page_view AS SELECT * FROM src"""
    val (desc, exists) = extractTableDesc(s3)
    assert(exists == false)
    assert(desc.identifier.database == None)
    assert(desc.identifier.table == "page_view")
    assert(desc.tableType == CatalogTableType.MANAGED)
    assert(desc.storage.locationUri == None)
    assert(desc.schema.isEmpty)
    assert(desc.viewText == None) // TODO will be SQLText
    assert(desc.viewOriginalText.isEmpty)
    assert(desc.storage.properties == Map())
    assert(desc.storage.inputFormat == Some("org.apache.hadoop.mapred.TextInputFormat"))
    assert(desc.storage.outputFormat ==
      Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"))
    assert(desc.storage.serde.isEmpty)
    assert(desc.properties == Map())
  }

  test("Test CTAS #4") {
    val s4 =
      """CREATE TABLE page_view
        |STORED BY 'storage.handler.class.name' AS SELECT * FROM src""".stripMargin
    intercept[AnalysisException] {
      extractTableDesc(s4)
    }
  }

  test("Test CTAS #5") {
    val s5 = """CREATE TABLE ctas2
               | ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe"
               | WITH SERDEPROPERTIES("serde_p1"="p1","serde_p2"="p2")
               | STORED AS RCFile
               | TBLPROPERTIES("tbl_p1"="p11", "tbl_p2"="p22")
               | AS
               |   SELECT key, value
               |   FROM src
               |   ORDER BY key, value""".stripMargin
    val (desc, exists) = extractTableDesc(s5)
    assert(exists == false)
    assert(desc.identifier.database == None)
    assert(desc.identifier.table == "ctas2")
    assert(desc.tableType == CatalogTableType.MANAGED)
    assert(desc.storage.locationUri == None)
    assert(desc.schema.isEmpty)
    assert(desc.viewText == None) // TODO will be SQLText
    assert(desc.viewOriginalText.isEmpty)
    assert(desc.storage.properties == Map(("serde_p1" -> "p1"), ("serde_p2" -> "p2")))
    assert(desc.storage.inputFormat == Some("org.apache.hadoop.hive.ql.io.RCFileInputFormat"))
    assert(desc.storage.outputFormat == Some("org.apache.hadoop.hive.ql.io.RCFileOutputFormat"))
    assert(desc.storage.serde == Some("org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe"))
    assert(desc.properties == Map(("tbl_p1" -> "p11"), ("tbl_p2" -> "p22")))
  }

  test("CTAS statement with a PARTITIONED BY clause is not allowed") {
    assertUnsupported(s"CREATE TABLE ctas1 PARTITIONED BY (k int)" +
      " AS SELECT key, value FROM (SELECT 1 as key, 2 as value) tmp")
  }

  test("CTAS statement with schema") {
    assertUnsupported(s"CREATE TABLE ctas1 (age INT, name STRING) AS SELECT * FROM src")
    assertUnsupported(s"CREATE TABLE ctas1 (age INT, name STRING) AS SELECT 1, 'hello'")
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
    assertError("select interval '.1111111111' second",
      "nanosecond 1111111111 outside range")
  }

  test("use native json_tuple instead of hive's UDTF in LATERAL VIEW") {
    val analyzer = TestHive.sparkSession.sessionState.analyzer
    val plan = analyzer.execute(parser.parsePlan(
      """
        |SELECT *
        |FROM (SELECT '{"f1": "value1", "f2": 12}' json) test
        |LATERAL VIEW json_tuple(json, 'f1', 'f2') jt AS a, b
      """.stripMargin))

    assert(plan.children.head.asInstanceOf[Generate].generator.isInstanceOf[JsonTuple])
  }

  test("transform query spec") {
    val plan1 = parser.parsePlan("select transform(a, b) using 'func' from e where f < 10")
      .asInstanceOf[ScriptTransformation].copy(ioschema = null)
    val plan2 = parser.parsePlan("map a, b using 'func' as c, d from e")
      .asInstanceOf[ScriptTransformation].copy(ioschema = null)
    val plan3 = parser.parsePlan("reduce a, b using 'func' as (c int, d decimal(10, 0)) from e")
      .asInstanceOf[ScriptTransformation].copy(ioschema = null)

    val p = ScriptTransformation(
      Seq(UnresolvedAttribute("a"), UnresolvedAttribute("b")),
      "func", Seq.empty, plans.table("e"), null)

    comparePlans(plan1,
      p.copy(child = p.child.where('f < 10), output = Seq('key.string, 'value.string)))
    comparePlans(plan2,
      p.copy(output = Seq('c.string, 'd.string)))
    comparePlans(plan3,
      p.copy(output = Seq('c.int, 'd.decimal(10, 0))))
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

  test("create table - basic") {
    val query = "CREATE TABLE my_table (id int, name string)"
    val (desc, allowExisting) = extractTableDesc(query)
    assert(!allowExisting)
    assert(desc.identifier.database.isEmpty)
    assert(desc.identifier.table == "my_table")
    assert(desc.tableType == CatalogTableType.MANAGED)
    assert(desc.schema == new StructType().add("id", "int").add("name", "string"))
    assert(desc.partitionColumnNames.isEmpty)
    assert(desc.bucketSpec.isEmpty)
    assert(desc.viewText.isEmpty)
    assert(desc.viewOriginalText.isEmpty)
    assert(desc.storage.locationUri.isEmpty)
    assert(desc.storage.inputFormat ==
      Some("org.apache.hadoop.mapred.TextInputFormat"))
    assert(desc.storage.outputFormat ==
      Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"))
    assert(desc.storage.serde.isEmpty)
    assert(desc.storage.properties.isEmpty)
    assert(desc.properties.isEmpty)
    assert(desc.comment.isEmpty)
  }

  test("create table - with database name") {
    val query = "CREATE TABLE dbx.my_table (id int, name string)"
    val (desc, _) = extractTableDesc(query)
    assert(desc.identifier.database == Some("dbx"))
    assert(desc.identifier.table == "my_table")
  }

  test("create table - temporary") {
    val query = "CREATE TEMPORARY TABLE tab1 (id int, name string)"
    val e = intercept[ParseException] { parser.parsePlan(query) }
    assert(e.message.contains("CREATE TEMPORARY TABLE is not supported yet"))
  }

  test("create table - external") {
    val query = "CREATE EXTERNAL TABLE tab1 (id int, name string) LOCATION '/path/to/nowhere'"
    val (desc, _) = extractTableDesc(query)
    assert(desc.tableType == CatalogTableType.EXTERNAL)
    assert(desc.storage.locationUri == Some("/path/to/nowhere"))
  }

  test("create table - if not exists") {
    val query = "CREATE TABLE IF NOT EXISTS tab1 (id int, name string)"
    val (_, allowExisting) = extractTableDesc(query)
    assert(allowExisting)
  }

  test("create table - comment") {
    val query = "CREATE TABLE my_table (id int, name string) COMMENT 'its hot as hell below'"
    val (desc, _) = extractTableDesc(query)
    assert(desc.comment == Some("its hot as hell below"))
  }

  test("create table - partitioned columns") {
    val query = "CREATE TABLE my_table (id int, name string) PARTITIONED BY (month int)"
    val (desc, _) = extractTableDesc(query)
    assert(desc.schema == new StructType()
      .add("id", "int")
      .add("name", "string")
      .add("month", "int"))
    assert(desc.partitionColumnNames == Seq("month"))
  }

  test("create table - clustered by") {
    val baseQuery = "CREATE TABLE my_table (id int, name string) CLUSTERED BY(id)"
    val query1 = s"$baseQuery INTO 10 BUCKETS"
    val query2 = s"$baseQuery SORTED BY(id) INTO 10 BUCKETS"
    val e1 = intercept[ParseException] { parser.parsePlan(query1) }
    val e2 = intercept[ParseException] { parser.parsePlan(query2) }
    assert(e1.getMessage.contains("Operation not allowed"))
    assert(e2.getMessage.contains("Operation not allowed"))
  }

  test("create table - skewed by") {
    val baseQuery = "CREATE TABLE my_table (id int, name string) SKEWED BY"
    val query1 = s"$baseQuery(id) ON (1, 10, 100)"
    val query2 = s"$baseQuery(id, name) ON ((1, 'x'), (2, 'y'), (3, 'z'))"
    val query3 = s"$baseQuery(id, name) ON ((1, 'x'), (2, 'y'), (3, 'z')) STORED AS DIRECTORIES"
    val e1 = intercept[ParseException] { parser.parsePlan(query1) }
    val e2 = intercept[ParseException] { parser.parsePlan(query2) }
    val e3 = intercept[ParseException] { parser.parsePlan(query3) }
    assert(e1.getMessage.contains("Operation not allowed"))
    assert(e2.getMessage.contains("Operation not allowed"))
    assert(e3.getMessage.contains("Operation not allowed"))
  }

  test("create table - row format") {
    val baseQuery = "CREATE TABLE my_table (id int, name string) ROW FORMAT"
    val query1 = s"$baseQuery SERDE 'org.apache.poof.serde.Baff'"
    val query2 = s"$baseQuery SERDE 'org.apache.poof.serde.Baff' WITH SERDEPROPERTIES ('k1'='v1')"
    val query3 =
      s"""
        |$baseQuery DELIMITED FIELDS TERMINATED BY 'x' ESCAPED BY 'y'
        |COLLECTION ITEMS TERMINATED BY 'a'
        |MAP KEYS TERMINATED BY 'b'
        |LINES TERMINATED BY '\n'
        |NULL DEFINED AS 'c'
      """.stripMargin
    val (desc1, _) = extractTableDesc(query1)
    val (desc2, _) = extractTableDesc(query2)
    val (desc3, _) = extractTableDesc(query3)
    assert(desc1.storage.serde == Some("org.apache.poof.serde.Baff"))
    assert(desc1.storage.properties.isEmpty)
    assert(desc2.storage.serde == Some("org.apache.poof.serde.Baff"))
    assert(desc2.storage.properties == Map("k1" -> "v1"))
    assert(desc3.storage.properties == Map(
      "field.delim" -> "x",
      "escape.delim" -> "y",
      "serialization.format" -> "x",
      "line.delim" -> "\n",
      "colelction.delim" -> "a", // yes, it's a typo from Hive :)
      "mapkey.delim" -> "b"))
  }

  test("create table - file format") {
    val baseQuery = "CREATE TABLE my_table (id int, name string) STORED AS"
    val query1 = s"$baseQuery INPUTFORMAT 'winput' OUTPUTFORMAT 'wowput'"
    val query2 = s"$baseQuery ORC"
    val (desc1, _) = extractTableDesc(query1)
    val (desc2, _) = extractTableDesc(query2)
    assert(desc1.storage.inputFormat == Some("winput"))
    assert(desc1.storage.outputFormat == Some("wowput"))
    assert(desc1.storage.serde.isEmpty)
    assert(desc2.storage.inputFormat == Some("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"))
    assert(desc2.storage.outputFormat == Some("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"))
    assert(desc2.storage.serde == Some("org.apache.hadoop.hive.ql.io.orc.OrcSerde"))
  }

  test("create table - storage handler") {
    val baseQuery = "CREATE TABLE my_table (id int, name string) STORED BY"
    val query1 = s"$baseQuery 'org.papachi.StorageHandler'"
    val query2 = s"$baseQuery 'org.mamachi.StorageHandler' WITH SERDEPROPERTIES ('k1'='v1')"
    val e1 = intercept[ParseException] { parser.parsePlan(query1) }
    val e2 = intercept[ParseException] { parser.parsePlan(query2) }
    assert(e1.getMessage.contains("Operation not allowed"))
    assert(e2.getMessage.contains("Operation not allowed"))
  }

  test("create table - properties") {
    val query = "CREATE TABLE my_table (id int, name string) TBLPROPERTIES ('k1'='v1', 'k2'='v2')"
    val (desc, _) = extractTableDesc(query)
    assert(desc.properties == Map("k1" -> "v1", "k2" -> "v2"))
  }

  test("create table - everything!") {
    val query =
      """
        |CREATE EXTERNAL TABLE IF NOT EXISTS dbx.my_table (id int, name string)
        |COMMENT 'no comment'
        |PARTITIONED BY (month int)
        |ROW FORMAT SERDE 'org.apache.poof.serde.Baff' WITH SERDEPROPERTIES ('k1'='v1')
        |STORED AS INPUTFORMAT 'winput' OUTPUTFORMAT 'wowput'
        |LOCATION '/path/to/mercury'
        |TBLPROPERTIES ('k1'='v1', 'k2'='v2')
      """.stripMargin
    val (desc, allowExisting) = extractTableDesc(query)
    assert(allowExisting)
    assert(desc.identifier.database == Some("dbx"))
    assert(desc.identifier.table == "my_table")
    assert(desc.tableType == CatalogTableType.EXTERNAL)
    assert(desc.schema == new StructType()
      .add("id", "int")
      .add("name", "string")
      .add("month", "int"))
    assert(desc.partitionColumnNames == Seq("month"))
    assert(desc.bucketSpec.isEmpty)
    assert(desc.viewText.isEmpty)
    assert(desc.viewOriginalText.isEmpty)
    assert(desc.storage.locationUri == Some("/path/to/mercury"))
    assert(desc.storage.inputFormat == Some("winput"))
    assert(desc.storage.outputFormat == Some("wowput"))
    assert(desc.storage.serde == Some("org.apache.poof.serde.Baff"))
    assert(desc.storage.properties == Map("k1" -> "v1"))
    assert(desc.properties == Map("k1" -> "v1", "k2" -> "v2"))
    assert(desc.comment == Some("no comment"))
  }

  test("create view -- basic") {
    val v1 = "CREATE VIEW view1 AS SELECT * FROM tab1"
    val command = parser.parsePlan(v1).asInstanceOf[CreateViewCommand]
    assert(!command.allowExisting)
    assert(command.name.database.isEmpty)
    assert(command.name.table == "view1")
    assert(command.originalText == Some("SELECT * FROM tab1"))
    assert(command.userSpecifiedColumns.isEmpty)
  }

  test("create view - full") {
    val v1 =
      """
        |CREATE OR REPLACE VIEW view1
        |(col1, col3 COMMENT 'hello')
        |COMMENT 'BLABLA'
        |TBLPROPERTIES('prop1Key'="prop1Val")
        |AS SELECT * FROM tab1
      """.stripMargin
    val command = parser.parsePlan(v1).asInstanceOf[CreateViewCommand]
    assert(command.name.database.isEmpty)
    assert(command.name.table == "view1")
    assert(command.userSpecifiedColumns == Seq("col1" -> None, "col3" -> Some("hello")))
    assert(command.originalText == Some("SELECT * FROM tab1"))
    assert(command.properties == Map("prop1Key" -> "prop1Val"))
    assert(command.comment == Some("BLABLA"))
  }

  test("create view -- partitioned view") {
    val v1 = "CREATE VIEW view1 partitioned on (ds, hr) as select * from srcpart"
    intercept[ParseException] {
      parser.parsePlan(v1)
    }
  }

  test("MSCK REPAIR table") {
    val sql = "MSCK REPAIR TABLE tab1"
    val parsed = parser.parsePlan(sql)
    val expected = AlterTableRecoverPartitionsCommand(
      TableIdentifier("tab1", None),
      "MSCK REPAIR TABLE")
    comparePlans(parsed, expected)
  }

  test("create table like") {
    val v1 = "CREATE TABLE table1 LIKE table2"
    val (target, source, exists) = parser.parsePlan(v1).collect {
      case CreateTableLikeCommand(t, s, allowExisting) => (t, s, allowExisting)
    }.head
    assert(exists == false)
    assert(target.database.isEmpty)
    assert(target.table == "table1")
    assert(source.database.isEmpty)
    assert(source.table == "table2")

    val v2 = "CREATE TABLE IF NOT EXISTS table1 LIKE table2"
    val (target2, source2, exists2) = parser.parsePlan(v2).collect {
      case CreateTableLikeCommand(t, s, allowExisting) => (t, s, allowExisting)
    }.head
    assert(exists2)
    assert(target2.database.isEmpty)
    assert(target2.table == "table1")
    assert(source2.database.isEmpty)
    assert(source2.table == "table2")
  }

  test("load data") {
    val v1 = "LOAD DATA INPATH 'path' INTO TABLE table1"
    val (table, path, isLocal, isOverwrite, partition) = parser.parsePlan(v1).collect {
      case LoadDataCommand(t, path, l, o, partition) => (t, path, l, o, partition)
    }.head
    assert(table.database.isEmpty)
    assert(table.table == "table1")
    assert(path == "path")
    assert(!isLocal)
    assert(!isOverwrite)
    assert(partition.isEmpty)

    val v2 = "LOAD DATA LOCAL INPATH 'path' OVERWRITE INTO TABLE table1 PARTITION(c='1', d='2')"
    val (table2, path2, isLocal2, isOverwrite2, partition2) = parser.parsePlan(v2).collect {
      case LoadDataCommand(t, path, l, o, partition) => (t, path, l, o, partition)
    }.head
    assert(table2.database.isEmpty)
    assert(table2.table == "table1")
    assert(path2 == "path")
    assert(isLocal2)
    assert(isOverwrite2)
    assert(partition2.nonEmpty)
    assert(partition2.get.apply("c") == "1" && partition2.get.apply("d") == "2")
  }

}
