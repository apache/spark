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
package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, DataType, FileRef, FileType, IntegerType, MapType, StringType, StructType}

/**
 * Tests that the file formats other than Parquet reject FILE columns, on write, on read with a
 * user-specified schema, and in table DDL, through the V1 and V2 data sources. Parquet stores
 * FILE, see `ParquetFileTypeSuite`.
 */
class FileTypeFormatsSuite extends QueryTest with SharedSparkSession {

  private val formats = Seq("json", "csv", "orc", "xml", "text")

  private val file = new FileRef("s3://bucket/a.pdf", null, 10L, "application/pdf", null)

  // Each column type that holds a FILE, with the FILE value it holds.
  private val fileColumns: Seq[(DataType, Any)] = Seq(
    FileType.external -> file,
    FileType() -> file,
    new StructType().add("f", FileType.external) -> Row(file),
    ArrayType(FileType.external) -> Seq(file),
    MapType(StringType, FileType.external) -> Map("k" -> file))

  private def withV1AndV2(f: => Unit): Unit = {
    Seq(formats.mkString(","), "").foreach { v1List =>
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> v1List)(f)
    }
  }

  private def checkRejected(format: String, dataType: DataType)(f: => Unit): Unit = {
    val e = intercept[AnalysisException](f)
    assert(e.getCondition === "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE", e.getMessage)
    assert(e.getMessageParameters.get("columnName") === "`c`")
    assert(e.getMessageParameters.get("columnType") === s""""${dataType.sql}"""")
    assert(e.getMessageParameters.get("format").equalsIgnoreCase(format))
  }

  test("a DataFrame write rejects a FILE column") {
    withV1AndV2 {
      for (format <- formats; (dataType, value) <- fileColumns) {
        val schema = new StructType().add("c", dataType)
        val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(value)), 1), schema)
        withTempPath { dir =>
          withClue(s"$format, $dataType: ") {
            checkRejected(format, dataType) {
              df.write.format(format).save(dir.getCanonicalPath)
            }
          }
        }
      }
    }
  }

  test("a read with a user-specified schema rejects a FILE column") {
    withV1AndV2 {
      for (format <- formats; (dataType, _) <- fileColumns) {
        withTempPath { dir =>
          spark.range(1).selectExpr("CAST(id AS STRING) AS c")
            .write.format(format).option("rowTag", "row").save(dir.getCanonicalPath)
          withClue(s"$format, $dataType: ") {
            checkRejected(format, dataType) {
              spark.read.schema(new StructType().add("c", dataType)).option("rowTag", "row")
                .format(format).load(dir.getCanonicalPath).collect()
            }
          }
        }
      }
    }
  }

  test("a table of another format rejects a FILE EXTERNAL column") {
    for (format <- formats) {
      Seq("FILE EXTERNAL", "STRUCT<f: FILE EXTERNAL>", "ARRAY<FILE EXTERNAL>").foreach { ddl =>
        withTable("t") {
          withClue(s"$format, $ddl: ") {
            checkRejected(format, DataType.fromDDL(ddl)) {
              sql(s"CREATE TABLE t (c $ddl) USING $format")
            }
          }
        }
      }
    }
  }

  test("a table of another format that gained a FILE column fails to write and read it") {
    // ALTER TABLE ADD COLUMNS does not check the column types against the format, for any type,
    // so a FILE column can be added to a table of the formats whose tables can add columns. A
    // write or read of that column then fails.
    Seq("json", "csv", "orc").foreach { format =>
      withTable("t") {
        sql(s"CREATE TABLE t (id INT) USING $format")
        sql("ALTER TABLE t ADD COLUMNS (c FILE EXTERNAL)")
        val schema = new StructType().add("id", IntegerType).add("c", FileType.external)
        val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(1, file)), 1), schema)
        withClue(s"$format: ") {
          checkRejected(format, FileType.external) {
            df.write.insertInto("t")
          }
          checkRejected(format, FileType.external) {
            spark.table("t").collect()
          }
        }
      }
    }
  }

  test("Parquet accepts what the other formats reject") {
    withV1AndV2 {
      for ((dataType, value) <- fileColumns) {
        val schema = new StructType().add("id", IntegerType).add("c", dataType)
        val df =
          spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(1, value)), 1), schema)
        withTempPath { dir =>
          df.write.parquet(dir.getCanonicalPath)
          checkAnswer(spark.read.schema(schema).parquet(dir.getCanonicalPath), Row(1, value))
        }
      }
    }
    withTable("t") {
      sql("CREATE TABLE t (id INT) USING parquet")
      sql("ALTER TABLE t ADD COLUMNS (c FILE EXTERNAL)")
      assert(spark.table("t").schema("c").dataType === FileType.external)
    }
  }
}
