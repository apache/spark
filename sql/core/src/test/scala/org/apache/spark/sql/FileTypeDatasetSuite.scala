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

import org.apache.spark.{SparkConf, SparkUnsupportedOperationException}
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
