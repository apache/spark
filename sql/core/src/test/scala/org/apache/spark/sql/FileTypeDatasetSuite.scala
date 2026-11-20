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
    Seq(true, false).foreach { codegen =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen.toString) {
        val shown = df.showString(1, truncate = 0)
        assert(shown.contains("""|1  |{"uri":"/data/b.txt"}|[{"uri":"/data/b.txt"}]|"""), shown)
      }
    }
  }

  test("a FILE does not cast to STRING") {
    val df = Seq(FileRecord(1, f2, Seq(f2))).toDF()
    Seq("file", "files", "named_struct('f', file)").foreach { expr =>
      val e = intercept[AnalysisException](df.selectExpr(s"CAST($expr AS STRING)").collect())
      assert(e.getCondition.startsWith("DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION"), expr)
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
