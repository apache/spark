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

package org.apache.spark.sql

import scala.beans.BeanProperty
import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkConf, SparkRuntimeException, SparkUnsupportedOperationException}
import org.apache.spark.sql.connector.catalog.InMemoryCatalog
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

case class FileRecord(id: Int, file: FileRef, files: Seq[FileRef])

class FileBean extends Serializable {
  @BeanProperty var id: Int = _
  @BeanProperty var file: FileRef = _
}

/**
 * Tests for FILE values in Datasets and DataFrames: the FileRef encoder, Row.getFile and the
 * paths a FILE value takes through the engine.
 */
class FileTypeDatasetSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  override protected def sparkConf: SparkConf =
    super.sparkConf.set("spark.sql.catalog.testcat", classOf[InMemoryCatalog].getName)

  private val f1 = new FileRef("s3://bucket/dir/doc.pdf", 10L, 2048L, "application/pdf", "MD5:abc")
  private val f2 = new FileRef("/data/b.txt", null, null, null, null)

  test("a Dataset of FileRef") {
    val ds = Seq(f1, f2, null).toDS()
    assert(ds.schema === new StructType().add("value", FileType(), nullable = true))
    assert(ds.collect().toSeq === Seq(f1, f2, null))
    assert(ds.map(f => if (f == null) null else f.uri).collect().toSeq ===
      Seq(f1.uri, f2.uri, null))
  }

  test("a Dataset of a case class with FileRef fields") {
    val records = Seq(FileRecord(1, f1, Seq(f2)), FileRecord(2, null, Seq(f1, null)))
    val ds = records.toDS()
    assert(ds.schema("file").dataType === FileType())
    assert(ds.collect().toSeq === records)
    assert(ds.filter($"id" === 2).collect().toSeq === records.tail)
    val rows = ds.select($"file", $"files").collect()
    assert(rows.map(_.getFile(0)).toSeq === Seq(f1, null))
    assert(rows.map(_.getSeq[FileRef](1)).toSeq === Seq(Seq(f2), Seq(f1, null)))
  }

  test("a DataFrame with a FILE EXTERNAL column, read back as FileRef") {
    val schema = new StructType().add("id", IntegerType).add("f", FileType.external)
    val df = spark.createDataFrame(Seq(Row(1, f1), Row(2, f2), Row(3, null)).asJava, schema)
    assert(df.schema === schema)
    checkAnswer(df, Seq(Row(1, f1), Row(2, f2), Row(3, null)))
    assert(df.collect().map(_.getFile(1)).toSeq === Seq(f1, f2, null))
    assert(df.select($"f").as[FileRef].collect().toSeq === Seq(f1, f2, null))
  }

  test("show renders a FILE value as its canonical JSON") {
    val df = Seq(FileRecord(1, f2, Seq(f2))).toDF()
    // Test sessions generate code for every expression, so the last setting is the only one
    // that evaluates the display expression interpretively.
    Seq(
      Seq(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true"),
      Seq(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false"),
      Seq(
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
        SQLConf.CODEGEN_FACTORY_MODE.key -> "NO_CODEGEN")).foreach { confs =>
      withSQLConf(confs: _*) {
        val shown = df.showString(1, truncate = 0)
        assert(shown.contains("""|1  |{"uri":"/data/b.txt"}|[{"uri":"/data/b.txt"}]|"""), shown)
      }
    }
  }

  test("a FILE literal is named and explained by its JSON form") {
    val df = spark.range(1).select(functions.typedLit(f2))
    assert(df.columns.toSeq === Seq("""FILE {"uri":"/data/b.txt"}"""))
    assert(df.collect().map(_.getFile(0)).toSeq === Seq(f2))
  }

  test("aggregates that hash a FILE value fail with an error that names FILE") {
    val df = Seq(FileRecord(1, f1, Nil), FileRecord(2, f2, Nil)).toDF().repartition(2)
    Seq("approx_count_distinct(file)", "count(DISTINCT file)").foreach { agg =>
      val e = intercept[Exception](df.selectExpr(agg).collect())
      val causes = Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null).toSeq
      assert(causes.exists {
        case u: SparkUnsupportedOperationException => u.getCondition == "UNSUPPORTED_DATATYPE"
        case _ => false
      }, s"$agg: $e")
      // Null FILE values hash to the seed, so these aggregates still run over them.
      val nulls = Seq(FileRecord(1, null, Nil), FileRecord(2, null, Nil)).toDF().repartition(2)
      checkAnswer(nulls.selectExpr(agg), Row(0L))
    }
  }

  test("a FILE does not cast to STRING") {
    val df = Seq(FileRecord(1, f2, Seq(f2))).toDF()
    Seq("true", "false").foreach { ansi =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
        Seq(
          ("file", "file", "\"FILE\""),
          ("files", "files", "\"ARRAY<FILE>\""),
          ("named_struct('f', file)", "named_struct(f, file)", "\"STRUCT<f: FILE>\"")
        ).foreach { case (expr, exprSql, srcType) =>
          val cast = s"CAST($expr AS STRING)"
          checkError(
            exception = intercept[AnalysisException](df.selectExpr(cast).collect()),
            condition = "DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION",
            parameters = Map(
              "sqlExpr" -> s"\"CAST($exprSql AS STRING)\"",
              "srcType" -> srcType,
              "targetType" -> "\"STRING\""),
            context = ExpectedContext(fragment = cast, start = 0, stop = cast.length - 1))
        }
      }
    }
  }

  test("a FILE value of any mode is written to a FILE EXTERNAL column") {
    val records = Seq(FileRecord(1, f1, Seq(f2)), FileRecord(2, null, Seq(null)))
    val expected = Seq(Row(1, f1, Seq(f2), Row(f1)), Row(2, null, Seq(null), Row(null)))
    val query = records.toDS()
      .selectExpr("id", "file AS f", "files AS fs", "named_struct('g', file) AS s")
    // Data source V2 tables do not allow the LEGACY policy, and FileTypeOpsSuite covers its cast.
    Seq("ANSI", "STRICT").foreach { policy =>
      withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> policy) {
        withTable("testcat.t") {
          sql("CREATE TABLE testcat.t " +
            "(id INT, f FILE EXTERNAL, fs ARRAY<FILE EXTERNAL>, s STRUCT<g: FILE EXTERNAL>)")
          val schema = spark.table("testcat.t").schema
          query.writeTo("testcat.t").append()
          withTempView("v") {
            query.createOrReplaceTempView("v")
            sql("INSERT INTO testcat.t SELECT * FROM v")
          }
          assert(spark.table("testcat.t").schema === schema, policy)
          checkAnswer(spark.table("testcat.t"), expected ++ expected)
        }
      }
    }
    // An explicit cast does not turn a FILE of unknown mode into a FILE EXTERNAL.
    val e = intercept[AnalysisException](query.selectExpr("CAST(f AS FILE EXTERNAL)"))
    assert(e.getCondition.startsWith("DATATYPE_MISMATCH.CAST_"), e)
  }

  test("the fields of a FILE are accessed by name") {
    val records = Seq(FileRecord(1, f1, Seq(f2)), FileRecord(2, f2, Nil), FileRecord(3, null, Nil))
    withTempView("v") {
      records.toDS()
        .selectExpr("id", "file", "files", "named_struct('g', file) AS s",
          "map('k', file) AS m")
        .createOrReplaceTempView("v")
      Seq("true", "false").foreach { pruning =>
        withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> pruning) {
          val fields = sql(
            "SELECT id, file.uri, file.offset, file.size, file.content_type, file.checksum " +
              "FROM v")
          assert(fields.schema === new StructType()
            .add("id", IntegerType, nullable = false)
            .add("uri", StringType)
            .add("offset", LongType)
            .add("size", LongType)
            .add("content_type", StringType)
            .add("checksum", StringType))
          // An absent field, such as the offset of f2, is NULL.
          checkAnswer(fields, Seq(
            Row(1, f1.uri, 10L, 2048L, "application/pdf", "MD5:abc"),
            Row(2, f2.uri, null, null, null, null),
            Row(3, null, null, null, null, null)))
          checkAnswer(
            sql("SELECT id, file.URI, s.g.uri, m['k'].size, get(files, 0).uri FROM v"),
            Seq(Row(1, f1.uri, f1.uri, 2048L, f2.uri), Row(2, f2.uri, f2.uri, null, null),
              Row(3, null, null, null, null)))
          checkAnswer(
            sql("SELECT id FROM v WHERE file.uri = 's3://bucket/dir/doc.pdf'"), Row(1))
          checkAnswer(
            sql("SELECT file.uri FROM v WHERE id > 1 ORDER BY id LIMIT 1"), Row(f2.uri))
          checkAnswer(records.toDF().select($"file.uri", $"file".getField("size")),
            Seq(Row(f1.uri, 2048L), Row(f2.uri, null), Row(null, null)))
        }
      }
      // The inline field is internal.
      checkError(
        exception = intercept[AnalysisException](sql("SELECT file.inline FROM v")),
        condition = "FIELD_NOT_FOUND",
        parameters = Map(
          "fieldName" -> "`inline`",
          "fields" -> "`uri`, `offset`, `size`, `content_type`, `checksum`"),
        context = ExpectedContext(fragment = "file.inline", start = 7, stop = 17))
    }
  }

  test("the fields of a FILE column of a table are accessed by name") {
    withTable("testcat.t") {
      sql("CREATE TABLE testcat.t (id INT, f FILE EXTERNAL)")
      Seq(FileRecord(1, f1, Nil), FileRecord(2, f2, Nil)).toDS()
        .selectExpr("id", "file AS f").writeTo("testcat.t").append()
      checkAnswer(sql("SELECT id, f.uri, f.size FROM testcat.t WHERE f.offset IS NULL"),
        Row(2, f2.uri, null))
    }
  }

  test("a struct of the FILE fields casts to a FILE") {
    val struct = "named_struct('uri', 's3://bucket/dir/doc.pdf', 'offset', 10L, " +
      "'size', 2048, 'content_type', 'application/pdf', 'checksum', 'MD5:abc')"
    Seq("true", "false").foreach { ansi =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
        val df = sql(s"SELECT CAST($struct AS FILE) AS f")
        assert(df.schema("f").dataType === FileType())
        checkAnswer(df, Row(f1))
        checkAnswer(sql(s"SELECT CAST($struct AS FILE).uri"), Row(f1.uri))
        // The fields other than uri may be NULL, and the names are matched case-insensitively.
        checkAnswer(
          sql("SELECT CAST(named_struct('URI', '/data/b.txt', 'offset', NULL, 'size', NULL, " +
            "'content_type', NULL, 'checksum', NULL) AS FILE)"),
          Row(f2))
        checkAnswer(sql(s"SELECT CAST(array($struct, NULL) AS ARRAY<FILE>)"),
          Row(Seq(f1, null)))
        // A NULL uri is an error, and TRY_CAST returns NULL for it.
        val nullUri = "named_struct('uri', CAST(NULL AS STRING), 'offset', 1L, 'size', 2L, " +
          "'content_type', 'a', 'checksum', 'b')"
        checkAnswer(sql(s"SELECT TRY_CAST($nullUri AS FILE)"), Row(null))
        checkError(
          exception = intercept[SparkRuntimeException](
            sql(s"SELECT CAST($nullUri AS FILE)").collect()),
          condition = "CAST_TO_FILE_TYPE.NULL_URI",
          sqlState = "42846",
          parameters = Map("sourceType" ->
            ("\"STRUCT<uri: STRING, offset: BIGINT NOT NULL, size: BIGINT NOT NULL, " +
              "content_type: STRING NOT NULL, checksum: STRING NOT NULL>\"")))
        // A struct without exactly the public fields, in their order, does not cast.
        Seq(
          "named_struct('uri', 'a')",
          "named_struct('size', 1L, 'uri', 'a', 'offset', 1L, 'content_type', 'c', " +
            "'checksum', 'd')",
          s"named_struct('uri', 'a', 'offset', 1L, 'size', 1L, 'content_type', 'c', " +
            "'checksum', 'd', 'inline', X'01')").foreach { source =>
          val e = intercept[AnalysisException](sql(s"SELECT CAST($source AS FILE)"))
          assert(e.getCondition.startsWith("DATATYPE_MISMATCH.CAST_"), e)
        }
        // An explicit cast does not produce a FILE EXTERNAL.
        val e = intercept[AnalysisException](sql(s"SELECT CAST($struct AS FILE EXTERNAL)"))
        assert(e.getCondition.startsWith("DATATYPE_MISMATCH.CAST_"), e)
      }
    }
  }

  test("a variant object casts to a FILE") {
    val json = """{"uri":"s3://bucket/dir/doc.pdf","offset":10,"size":2048,""" +
      """"content_type":"application/pdf","checksum":"MD5:abc","inline":"AQI=","x":1}"""
    checkAnswer(sql(s"SELECT CAST(parse_json('$json') AS FILE)"), Row(f1))
    checkAnswer(sql(s"SELECT variant_get(parse_json('$json'), '$$', 'file')"), Row(f1))
    checkAnswer(sql(s"SELECT variant_get(parse_json('$json'), '$$', 'file').size"), Row(2048L))
    checkAnswer(
      sql("""SELECT variant_get(parse_json('[{"uri":"/data/b.txt"},null]'), """ +
        "'$', 'array<file>') AS value").selectExpr("value[0]"),
      Row(f2))
    checkAnswer(sql("""SELECT try_variant_get(parse_json('{"size":1}'), '$', 'file')"""),
      Row(null))
    checkAnswer(sql("""SELECT TRY_CAST(parse_json('{"uri":"a","URI":"b"}') AS FILE)"""),
      Row(null))
    checkError(
      exception = intercept[SparkRuntimeException](
        sql("""SELECT CAST(parse_json('{"uri":"a","URI":"b"}') AS FILE)""").collect()),
      condition = "CAST_TO_FILE_TYPE.DUPLICATE_FIELD",
      sqlState = "42846",
      parameters = Map("sourceType" -> "\"VARIANT\"", "fieldName" -> "`uri`"))
  }

  test("a struct cast to a FILE is written to a FILE EXTERNAL column") {
    withTable("testcat.t") {
      sql("CREATE TABLE testcat.t (id INT, f FILE EXTERNAL)")
      sql("INSERT INTO testcat.t SELECT 1, CAST(named_struct('uri', '/data/b.txt', " +
        "'offset', NULL, 'size', NULL, 'content_type', NULL, 'checksum', NULL) AS FILE)")
      sql("""INSERT INTO testcat.t SELECT 2, CAST(parse_json('{"uri":"/data/b.txt"}') AS FILE)""")
      checkAnswer(spark.table("testcat.t"), Seq(Row(1, f2), Row(2, f2)))
    }
  }

  test("a bean with a FileRef property") {
    val bean = new FileBean
    bean.setId(1)
    bean.setFile(f1)
    val ds = spark.createDataset(Seq(bean))(Encoders.bean(classOf[FileBean]))
    assert(ds.schema("file").dataType === FileType())
    val back = ds.collect().head
    assert(back.getId() === 1)
    assert(back.getFile() === f1)
  }

  test("a cached DataFrame with FILE columns") {
    val df = Seq(FileRecord(1, f1, Seq(f2)), FileRecord(2, null, Nil)).toDF()
    val cached = df.cache()
    try {
      checkAnswer(cached, Seq(Row(1, f1, Seq(f2)), Row(2, null, Nil)))
    } finally {
      cached.unpersist()
    }
  }

  test("a FileRef encoder needs the FILE type to be enabled") {
    withSQLConf(SQLConf.FILE_TYPE_ENABLED.key -> "false") {
      checkError(
        exception = intercept[AnalysisException](Seq(f1).toDS()),
        condition = "UNSUPPORTED_FILE_TYPE",
        parameters = Map.empty)
    }
  }
}
