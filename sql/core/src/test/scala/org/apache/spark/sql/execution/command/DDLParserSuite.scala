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

import java.net.URI
import java.util.Locale

import scala.reflect.{classTag, ClassTag}

import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans
import org.apache.spark.sql.catalyst.dsl.plans.DslLogicalPlan
import org.apache.spark.sql.catalyst.expressions.JsonTuple
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class DDLParserSuite extends AnalysisTest with SharedSparkSession {
  private lazy val parser = new SparkSqlParser(new SQLConf)

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

  private def parseAs[T: ClassTag](query: String): T = {
    parser.parsePlan(query) match {
      case t: T => t
      case other =>
        fail(s"Expected to parse ${classTag[T].runtimeClass} from query," +
          s"got ${other.getClass.getName}: $query")
    }
  }

  private def compareTransformQuery(sql: String, expected: LogicalPlan): Unit = {
    val plan = parser.parsePlan(sql).asInstanceOf[ScriptTransformation].copy(ioschema = null)
    comparePlans(plan, expected, checkAnalysis = false)
  }

  private def extractTableDesc(sql: String): (CatalogTable, Boolean) = {
    parser.parsePlan(sql).collect {
      case CreateTable(tableDesc, mode, _) => (tableDesc, mode == SaveMode.Ignore)
    }.head
  }

  test("alter database - property values must be set") {
    assertUnsupported(
      sql = "ALTER DATABASE my_db SET DBPROPERTIES('key_without_value', 'key_with_value'='x')",
      containsThesePhrases = Seq("key_without_value"))
  }

  test("create hive table - table file format") {
    val allSources = Seq("parquet", "parquetfile", "orc", "orcfile", "avro", "avrofile",
      "sequencefile", "rcfile", "textfile")

    allSources.foreach { s =>
      val query = s"CREATE TABLE my_tab STORED AS $s"
      val ct = parseAs[CreateTable](query)
      val hiveSerde = HiveSerDe.sourceToSerDe(s)
      assert(hiveSerde.isDefined)
      assert(ct.tableDesc.storage.serde ==
        hiveSerde.get.serde.orElse(Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")))
      assert(ct.tableDesc.storage.inputFormat == hiveSerde.get.inputFormat)
      assert(ct.tableDesc.storage.outputFormat == hiveSerde.get.outputFormat)
    }
  }

  test("create hive table - row format and table file format") {
    val createTableStart = "CREATE TABLE my_tab ROW FORMAT"
    val fileFormat = s"STORED AS INPUTFORMAT 'inputfmt' OUTPUTFORMAT 'outputfmt'"
    val query1 = s"$createTableStart SERDE 'anything' $fileFormat"
    val query2 = s"$createTableStart DELIMITED FIELDS TERMINATED BY ' ' $fileFormat"

    // No conflicting serdes here, OK
    val parsed1 = parseAs[CreateTable](query1)
    assert(parsed1.tableDesc.storage.serde == Some("anything"))
    assert(parsed1.tableDesc.storage.inputFormat == Some("inputfmt"))
    assert(parsed1.tableDesc.storage.outputFormat == Some("outputfmt"))

    val parsed2 = parseAs[CreateTable](query2)
    assert(parsed2.tableDesc.storage.serde ==
      Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
    assert(parsed2.tableDesc.storage.inputFormat == Some("inputfmt"))
    assert(parsed2.tableDesc.storage.outputFormat == Some("outputfmt"))
  }

  test("create hive table - row format serde and generic file format") {
    val allSources = Seq("parquet", "orc", "avro", "sequencefile", "rcfile", "textfile")
    val supportedSources = Set("sequencefile", "rcfile", "textfile")

    allSources.foreach { s =>
      val query = s"CREATE TABLE my_tab ROW FORMAT SERDE 'anything' STORED AS $s"
      if (supportedSources.contains(s)) {
        val ct = parseAs[CreateTable](query)
        val hiveSerde = HiveSerDe.sourceToSerDe(s)
        assert(hiveSerde.isDefined)
        assert(ct.tableDesc.storage.serde == Some("anything"))
        assert(ct.tableDesc.storage.inputFormat == hiveSerde.get.inputFormat)
        assert(ct.tableDesc.storage.outputFormat == hiveSerde.get.outputFormat)
      } else {
        assertUnsupported(query, Seq("row format serde", "incompatible", s))
      }
    }
  }

  test("create hive table - row format delimited and generic file format") {
    val allSources = Seq("parquet", "orc", "avro", "sequencefile", "rcfile", "textfile")
    val supportedSources = Set("textfile")

    allSources.foreach { s =>
      val query = s"CREATE TABLE my_tab ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' STORED AS $s"
      if (supportedSources.contains(s)) {
        val ct = parseAs[CreateTable](query)
        val hiveSerde = HiveSerDe.sourceToSerDe(s)
        assert(hiveSerde.isDefined)
        assert(ct.tableDesc.storage.serde ==
          hiveSerde.get.serde.orElse(Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")))
        assert(ct.tableDesc.storage.inputFormat == hiveSerde.get.inputFormat)
        assert(ct.tableDesc.storage.outputFormat == hiveSerde.get.outputFormat)
      } else {
        assertUnsupported(query, Seq("row format delimited", "only compatible with 'textfile'", s))
      }
    }
  }

  test("create hive external table - location must be specified") {
    assertUnsupported(
      sql = "CREATE EXTERNAL TABLE my_tab STORED AS parquet",
      containsThesePhrases = Seq("create external table", "location"))
    val query = "CREATE EXTERNAL TABLE my_tab STORED AS parquet LOCATION '/something/anything'"
    val ct = parseAs[CreateTable](query)
    assert(ct.tableDesc.tableType == CatalogTableType.EXTERNAL)
    assert(ct.tableDesc.storage.locationUri == Some(new URI("/something/anything")))
  }

  test("create hive table - property values must be set") {
    assertUnsupported(
      sql = "CREATE TABLE my_tab STORED AS parquet " +
        "TBLPROPERTIES('key_without_value', 'key_with_value'='x')",
      containsThesePhrases = Seq("key_without_value"))
    assertUnsupported(
      sql = "CREATE TABLE my_tab ROW FORMAT SERDE 'serde' " +
        "WITH SERDEPROPERTIES('key_without_value', 'key_with_value'='x')",
      containsThesePhrases = Seq("key_without_value"))
  }

  test("create hive table - location implies external") {
    val query = "CREATE TABLE my_tab STORED AS parquet LOCATION '/something/anything'"
    val ct = parseAs[CreateTable](query)
    assert(ct.tableDesc.tableType == CatalogTableType.EXTERNAL)
    assert(ct.tableDesc.storage.locationUri == Some(new URI("/something/anything")))
  }

  test("Duplicate clauses - create hive table") {
    def createTableHeader(duplicateClause: String): String = {
      s"CREATE TABLE my_tab(a INT, b STRING) STORED AS parquet $duplicateClause $duplicateClause"
    }

    intercept(createTableHeader("TBLPROPERTIES('test' = 'test2')"),
      "Found duplicate clauses: TBLPROPERTIES")
    intercept(createTableHeader("LOCATION '/tmp/file'"),
      "Found duplicate clauses: LOCATION")
    intercept(createTableHeader("COMMENT 'a table'"),
      "Found duplicate clauses: COMMENT")
    intercept(createTableHeader("CLUSTERED BY(b) INTO 256 BUCKETS"),
      "Found duplicate clauses: CLUSTERED BY")
    intercept(createTableHeader("PARTITIONED BY (k int)"),
      "Found duplicate clauses: PARTITIONED BY")
    intercept(createTableHeader("STORED AS parquet"),
      "Found duplicate clauses: STORED AS/BY")
    intercept(
      createTableHeader("ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'"),
      "Found duplicate clauses: ROW FORMAT")
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

  test("SPARK-34556: duplicate keys in partition spec") {
    val e = intercept[ParseException] {
      parser.parsePlan("INSERT OVERWRITE t PARTITION (c='2', C='3') VALUES (1)")
    }.getMessage
    assert(e.contains("Found duplicate keys 'c'"))
    val conf = new SQLConf()
    conf.setConf(SQLConf.CASE_SENSITIVE, true)
    val caseSensitiveParser = new SparkSqlParser(conf)
    caseSensitiveParser.parsePlan("INSERT OVERWRITE t PARTITION (c='2', C='3') VALUES (1)")
  }

  test("duplicate columns in partition specs") {
    val e = intercept[ParseException] {
      parser.parsePlan(
        "ALTER TABLE dbx.tab1 PARTITION (a='1', a='2') RENAME TO PARTITION (a='100', a='200')")
    }.getMessage
    assert(e.contains("Found duplicate keys 'a'"))
  }

  test("empty values in non-optional partition specs") {
    val e = intercept[ParseException] {
      parser.parsePlan(
        "SHOW PARTITIONS dbx.tab1 PARTITION (a='1', b)")
    }.getMessage
    assert(e.contains("Found an empty partition key 'b'"))
  }

  test("Test CTAS #1") {
    val s1 =
      """
        |CREATE EXTERNAL TABLE IF NOT EXISTS mydb.page_view
        |COMMENT 'This is the staging page view table'
        |STORED AS RCFILE
        |LOCATION '/user/external/page_view'
        |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
        |AS SELECT * FROM src
      """.stripMargin

    val s2 =
      """
        |CREATE EXTERNAL TABLE IF NOT EXISTS mydb.page_view
        |STORED AS RCFILE
        |COMMENT 'This is the staging page view table'
        |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
        |LOCATION '/user/external/page_view'
        |AS SELECT * FROM src
      """.stripMargin

    val s3 =
      """
        |CREATE EXTERNAL TABLE IF NOT EXISTS mydb.page_view
        |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
        |LOCATION '/user/external/page_view'
        |STORED AS RCFILE
        |COMMENT 'This is the staging page view table'
        |AS SELECT * FROM src
      """.stripMargin

    checkParsing(s1)
    checkParsing(s2)
    checkParsing(s3)

    def checkParsing(sql: String): Unit = {
      val (desc, exists) = extractTableDesc(sql)
      assert(exists)
      assert(desc.identifier.database == Some("mydb"))
      assert(desc.identifier.table == "page_view")
      assert(desc.tableType == CatalogTableType.EXTERNAL)
      assert(desc.storage.locationUri == Some(new URI("/user/external/page_view")))
      assert(desc.schema.isEmpty) // will be populated later when the table is actually created
      assert(desc.comment == Some("This is the staging page view table"))
      // TODO will be SQLText
      assert(desc.viewText.isEmpty)
      assert(desc.viewCatalogAndNamespace.isEmpty)
      assert(desc.viewQueryColumnNames.isEmpty)
      assert(desc.partitionColumnNames.isEmpty)
      assert(desc.storage.inputFormat == Some("org.apache.hadoop.hive.ql.io.RCFileInputFormat"))
      assert(desc.storage.outputFormat == Some("org.apache.hadoop.hive.ql.io.RCFileOutputFormat"))
      assert(desc.storage.serde ==
        Some("org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe"))
      assert(desc.properties == Map("p1" -> "v1", "p2" -> "v2"))
    }
  }

  test("Test CTAS #2") {
    val s1 =
      """
        |CREATE EXTERNAL TABLE IF NOT EXISTS mydb.page_view
        |COMMENT 'This is the staging page view table'
        |ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
        | STORED AS
        | INPUTFORMAT 'parquet.hive.DeprecatedParquetInputFormat'
        | OUTPUTFORMAT 'parquet.hive.DeprecatedParquetOutputFormat'
        |LOCATION '/user/external/page_view'
        |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
        |AS SELECT * FROM src
      """.stripMargin

    val s2 =
      """
        |CREATE EXTERNAL TABLE IF NOT EXISTS mydb.page_view
        |LOCATION '/user/external/page_view'
        |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
        |ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
        | STORED AS
        | INPUTFORMAT 'parquet.hive.DeprecatedParquetInputFormat'
        | OUTPUTFORMAT 'parquet.hive.DeprecatedParquetOutputFormat'
        |COMMENT 'This is the staging page view table'
        |AS SELECT * FROM src
      """.stripMargin

    checkParsing(s1)
    checkParsing(s2)

    def checkParsing(sql: String): Unit = {
      val (desc, exists) = extractTableDesc(sql)
      assert(exists)
      assert(desc.identifier.database == Some("mydb"))
      assert(desc.identifier.table == "page_view")
      assert(desc.tableType == CatalogTableType.EXTERNAL)
      assert(desc.storage.locationUri == Some(new URI("/user/external/page_view")))
      assert(desc.schema.isEmpty) // will be populated later when the table is actually created
      // TODO will be SQLText
      assert(desc.comment == Some("This is the staging page view table"))
      assert(desc.viewText.isEmpty)
      assert(desc.viewCatalogAndNamespace.isEmpty)
      assert(desc.viewQueryColumnNames.isEmpty)
      assert(desc.partitionColumnNames.isEmpty)
      assert(desc.storage.properties == Map())
      assert(desc.storage.inputFormat == Some("parquet.hive.DeprecatedParquetInputFormat"))
      assert(desc.storage.outputFormat == Some("parquet.hive.DeprecatedParquetOutputFormat"))
      assert(desc.storage.serde == Some("parquet.hive.serde.ParquetHiveSerDe"))
      assert(desc.properties == Map("p1" -> "v1", "p2" -> "v2"))
    }
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
    assert(desc.viewQueryColumnNames.isEmpty)
    assert(desc.storage.properties == Map())
    assert(desc.storage.inputFormat == Some("org.apache.hadoop.mapred.TextInputFormat"))
    assert(desc.storage.outputFormat ==
      Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"))
    assert(desc.storage.serde == Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
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
    assert(desc.viewCatalogAndNamespace.isEmpty)
    assert(desc.viewQueryColumnNames.isEmpty)
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
    val p = ScriptTransformation(
      Seq(UnresolvedAttribute("a"), UnresolvedAttribute("b")),
      "func", Seq.empty, plans.table("e"), null)

    compareTransformQuery("select transform(a, b) using 'func' from e where f < 10",
      p.copy(child = p.child.where('f < 10), output = Seq('key.string, 'value.string)))
    compareTransformQuery("map a, b using 'func' as c, d from e",
      p.copy(output = Seq('c.string, 'd.string)))
    compareTransformQuery("reduce a, b using 'func' as (c int, d decimal(10, 0)) from e",
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
    assert(desc.viewQueryColumnNames.isEmpty)
    assert(desc.storage.locationUri.isEmpty)
    assert(desc.storage.inputFormat ==
      Some("org.apache.hadoop.mapred.TextInputFormat"))
    assert(desc.storage.outputFormat ==
      Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"))
    assert(desc.storage.serde == Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
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
    assert(desc.storage.locationUri == Some(new URI("/path/to/nowhere")))
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
    val numBuckets = 10
    val bucketedColumn = "id"
    val sortColumn = "id"
    val baseQuery =
      s"""
         CREATE TABLE my_table (
           $bucketedColumn int,
           name string)
         CLUSTERED BY($bucketedColumn)
       """

    val query1 = s"$baseQuery INTO $numBuckets BUCKETS"
    val (desc1, _) = extractTableDesc(query1)
    assert(desc1.bucketSpec.isDefined)
    val bucketSpec1 = desc1.bucketSpec.get
    assert(bucketSpec1.numBuckets == numBuckets)
    assert(bucketSpec1.bucketColumnNames.head.equals(bucketedColumn))
    assert(bucketSpec1.sortColumnNames.isEmpty)

    val query2 = s"$baseQuery SORTED BY($sortColumn) INTO $numBuckets BUCKETS"
    val (desc2, _) = extractTableDesc(query2)
    assert(desc2.bucketSpec.isDefined)
    val bucketSpec2 = desc2.bucketSpec.get
    assert(bucketSpec2.numBuckets == numBuckets)
    assert(bucketSpec2.bucketColumnNames.head.equals(bucketedColumn))
    assert(bucketSpec2.sortColumnNames.head.equals(sortColumn))
  }

  test("create table(hive) - skewed by") {
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

  test("create table(hive) - row format") {
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

  test("create table(hive) - file format") {
    val baseQuery = "CREATE TABLE my_table (id int, name string) STORED AS"
    val query1 = s"$baseQuery INPUTFORMAT 'winput' OUTPUTFORMAT 'wowput'"
    val query2 = s"$baseQuery ORC"
    val (desc1, _) = extractTableDesc(query1)
    val (desc2, _) = extractTableDesc(query2)
    assert(desc1.storage.inputFormat == Some("winput"))
    assert(desc1.storage.outputFormat == Some("wowput"))
    assert(desc1.storage.serde == Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
    assert(desc2.storage.inputFormat == Some("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"))
    assert(desc2.storage.outputFormat == Some("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"))
    assert(desc2.storage.serde == Some("org.apache.hadoop.hive.ql.io.orc.OrcSerde"))
  }

  test("create table(hive) - storage handler") {
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

  test("create table(hive) - everything!") {
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
    assert(desc.viewCatalogAndNamespace.isEmpty)
    assert(desc.viewQueryColumnNames.isEmpty)
    assert(desc.storage.locationUri == Some(new URI("/path/to/mercury")))
    assert(desc.storage.inputFormat == Some("winput"))
    assert(desc.storage.outputFormat == Some("wowput"))
    assert(desc.storage.serde == Some("org.apache.poof.serde.Baff"))
    assert(desc.storage.properties == Map("k1" -> "v1"))
    assert(desc.properties == Map("k1" -> "v1", "k2" -> "v2"))
    assert(desc.comment == Some("no comment"))
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
}
