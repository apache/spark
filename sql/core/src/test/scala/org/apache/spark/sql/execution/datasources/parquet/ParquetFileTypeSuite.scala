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
package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * End-to-end tests for storing FILE in Parquet: a FILE is written as a group of the FILE fields
 * and read back with every Parquet reader, through the V1 and V2 data sources.
 */
class ParquetFileTypeSuite extends QueryTest with SharedSparkSession {

  private val f1 = new FileRef("s3://bucket/a.pdf", 10L, 2048L, "application/pdf", "MD5:abc")
  private val withInline =
    FileRefInternals.create("s3://bucket/b.txt", null, 5L, "text/plain", null, Array[Byte](1, 2, 3))
  private val inlineOnly = FileRefInternals.create(null, null, null, null, null, Array[Byte](9))
  private val uriOnly = new FileRef("/tmp/c.json", null, null, null, null)
  private val allNullFields = new FileRef(null, null, null, null, null)

  // One value per field that is the only one set, and one per field that is the only one unset.
  private val valuesPerField: Seq[FileRef] = {
    val full = Array[Any]("s3://bucket/d.bin", 1L, 2L, "application/octet-stream", "ETAG:e",
      Array[Byte](4, 5))
    def make(values: Array[Any]): FileRef = FileRefInternals.create(
      values(0).asInstanceOf[String],
      values(1).asInstanceOf[java.lang.Long],
      values(2).asInstanceOf[java.lang.Long],
      values(3).asInstanceOf[String],
      values(4).asInstanceOf[String],
      values(5).asInstanceOf[Array[Byte]])
    full.indices.flatMap { i =>
      Seq(
        make(full.indices.map(j => if (j == i) full(j) else null).toArray),
        make(full.indices.map(j => if (j == i) null else full(j)).toArray))
    }
  }

  private val values: Seq[FileRef] =
    Seq(f1, withInline, inlineOnly, uriOnly, allNullFields, null) ++ valuesPerField

  // The Parquet readers: the vectorized reader, the row-based reader that the vectorized reader
  // falls back to for nested columns, and the row-based reader alone.
  private val readers: Seq[(String, Seq[(String, String)])] = Seq(
    "vectorized" -> Seq(
      SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true",
      SQLConf.PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key -> "true"),
    "vectorized, off-heap" -> Seq(
      SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true",
      SQLConf.PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key -> "true",
      SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED.key -> "true"),
    "row-based, nested vectorization off" -> Seq(
      SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true",
      SQLConf.PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key -> "false"),
    "row-based" -> Seq(
      SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false"))

  // V1 and V2 Parquet data sources.
  private val sources: Seq[(String, String)] = Seq("V1" -> "parquet", "V2" -> "")

  private def withSourceAndReader(f: (String, String) => Unit): Unit = {
    for ((source, v1List) <- sources; (reader, confs) <- readers) {
      withSQLConf(confs :+ (SQLConf.USE_V1_SOURCE_LIST.key -> v1List): _*) {
        f(source, reader)
      }
    }
  }

  private def roundTrip(schema: StructType, rows: Seq[Row]): Unit = {
    withSourceAndReader { (source, reader) =>
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        spark.createDataFrame(spark.sparkContext.parallelize(rows, 2), schema)
          .write.parquet(path)
        val df = spark.read.schema(schema).parquet(path)
        withClue(s"$source, $reader: ") {
          // A file source reads every column as nullable.
          assert(df.schema === schema.asNullable)
          checkAnswer(df, rows)
          // A read without a schema takes the FILE type, with its mode, from the file footer.
          val inferred = spark.read.parquet(path)
          assert(inferred.schema === schema.asNullable)
          checkAnswer(inferred, rows)
        }
      }
    }
  }

  private def isVectorized(df: org.apache.spark.sql.DataFrame): Boolean =
    df.queryExecution.executedPlan.collectFirst {
      case s: FileSourceScanExec => s.supportsColumnar
      case s: BatchScanExec => s.supportsColumnar
    }.get

  test("FILE column round trip") {
    val schema = new StructType().add("id", IntegerType).add("f", FileType.external)
    roundTrip(schema, values.zipWithIndex.map { case (v, i) => Row(i, v) })
  }

  test("FILE column of unknown mode round trip") {
    val schema = new StructType().add("id", IntegerType).add("f", FileType())
    roundTrip(schema, Seq(Row(1, f1), Row(2, null), Row(3, withInline)))
  }

  test("FILE nested in a struct, an array and a map round trip") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("s", new StructType().add("name", StringType).add("f", FileType.external))
      .add("a", ArrayType(FileType.external))
      .add("m", MapType(StringType, FileType.external))
      .add("as", ArrayType(new StructType().add("f", FileType.external)))
    val rows = Seq(
      Row(1, Row("x", f1), Seq(f1, null, withInline), Map("k1" -> f1, "k2" -> null),
        Seq(Row(withInline), Row(null), null)),
      Row(2, Row("y", null), Seq.empty, Map.empty, Seq.empty),
      Row(3, null, null, null, null),
      Row(4, Row(null, inlineOnly), valuesPerField, Map("k" -> allNullFields),
        valuesPerField.map(Row(_))))
    roundTrip(schema, rows)
  }

  test("a non-nullable FILE array round trip with the legacy format") {
    val schema = new StructType()
      .add("a", ArrayType(FileType.external, containsNull = false))
      .add("m", MapType(StringType, FileType.external, valueContainsNull = false))
    val rows = Seq(Row(Seq(f1, withInline), Map("k" -> uriOnly)), Row(null, null))
    Seq("true", "false").foreach { legacy =>
      withSQLConf(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key -> legacy) {
        roundTrip(schema, rows)
      }
    }
  }

  test("the vectorized reader reads FILE columns when nested vectorization is on") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val schema = new StructType().add("f", FileType.external)
      spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(f1)), 1), schema)
        .write.parquet(path)
      for ((_, v1List) <- sources) {
        withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> v1List) {
          withSQLConf(readers.head._2: _*) {
            assert(isVectorized(spark.read.parquet(path)))
          }
          withSQLConf(readers(2)._2: _*) {
            assert(!isVectorized(spark.read.parquet(path)))
          }
        }
      }
    }
  }

  test("a FILE is stored as a plain group of its fields") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val schema = new StructType().add("f", FileType.external)
      spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(f1)), 1), schema)
        .write.parquet(path)
      val file = dir.listFiles().filter(_.getName.endsWith(".parquet")).head
      val reader = ParquetFileReader.open(
        HadoopInputFile.fromPath(new Path(file.getCanonicalPath), new org.apache.hadoop.conf
          .Configuration()))
      val group = try reader.getFileMetaData.getSchema.getType(0).asGroupType() finally {
        reader.close()
      }
      assert(group.getLogicalTypeAnnotation === null)
      assert(group === new SparkToParquetSchemaConverter(conf)
        .convert(new StructType().add("f", FileType.asStruct)).getType(0))
    }
  }

  test("FILE values read as a struct of the FILE fields, and a struct reads as a FILE") {
    val fileSchema = new StructType().add("id", IntegerType).add("f", FileType.external)
    val structSchema = new StructType().add("id", IntegerType).add("f", FileType.asStruct)
    val rows = Seq(Row(1, f1), Row(2, withInline), Row(3, null))
    val structRows = Seq(
      Row(1, Row(f1.uri, f1.offset, f1.size, f1.contentType, f1.checksum, null)),
      Row(2, Row(withInline.uri, null, withInline.size, withInline.contentType, null,
        Array[Byte](1, 2, 3))),
      Row(3, null))
    withSourceAndReader { (source, reader) =>
      withClue(s"$source, $reader: ") {
        withTempPath { dir =>
          val path = dir.getCanonicalPath
          spark.createDataFrame(spark.sparkContext.parallelize(rows, 1), fileSchema)
            .write.parquet(path)
          checkAnswer(spark.read.schema(structSchema).parquet(path), structRows)
        }
        withTempPath { dir =>
          val path = dir.getCanonicalPath
          spark.createDataFrame(spark.sparkContext.parallelize(structRows, 1), structSchema)
            .write.parquet(path)
          checkAnswer(spark.read.schema(fileSchema).parquet(path), rows)
        }
      }
    }
  }

  test("a group with some of the FILE fields, or other fields, reads as a FILE") {
    // A group written by another writer may hold only some of the FILE fields, in another order,
    // or fields that FILE does not have: the missing fields read as null and the others are
    // ignored.
    val written = new StructType().add("f", new StructType()
      .add("extra", IntegerType)
      .add("size", LongType)
      .add("uri", StringType))
    val fileSchema = new StructType().add("f", FileType.external)
    withSourceAndReader { (source, reader) =>
      withClue(s"$source, $reader: ") {
        withTempPath { dir =>
          val path = dir.getCanonicalPath
          spark.createDataFrame(spark.sparkContext.parallelize(
            Seq(Row(Row(7, 3L, "s3://bucket/e")), Row(Row(8, null, null)), Row(null)), 1), written)
            .write.parquet(path)
          checkAnswer(spark.read.schema(fileSchema).parquet(path), Seq(
            Row(new FileRef("s3://bucket/e", null, 3L, null, null)),
            Row(allNullFields),
            Row(null)))
        }
      }
    }
  }

  test("Parquet tables with a FILE EXTERNAL column") {
    val rows = Seq(Row(1, f1, Row(withInline)), Row(2, null, null), Row(3, inlineOnly, Row(null)))
    val schema = new StructType()
      .add("id", IntegerType)
      .add("f", FileType.external)
      .add("s", new StructType().add("g", FileType.external))
    withTable("t", "t2") {
      sql("CREATE TABLE t (id INT, f FILE EXTERNAL, s STRUCT<g: FILE EXTERNAL>) USING parquet")
      val df = spark.createDataFrame(spark.sparkContext.parallelize(rows, 2), schema)
      df.write.insertInto("t")
      readers.foreach { case (reader, confs) =>
        withSQLConf(confs: _*) {
          withClue(s"$reader: ") {
            checkAnswer(spark.table("t"), rows)
            checkAnswer(sql("SELECT f FROM t WHERE id = 1"), Row(f1))
          }
        }
      }
      df.write.format("parquet").saveAsTable("t2")
      assert(spark.table("t2").schema === schema)
      checkAnswer(spark.table("t2"), rows)
      sql("INSERT INTO t2 SELECT * FROM t")
      checkAnswer(spark.table("t2"), rows ++ rows)
    }
  }

  test("nested schema pruning keeps a FILE column whole, with its mode") {
    withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "true") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        val schema = new StructType()
          .add("id", IntegerType)
          .add("f", FileType.external)
          .add("other", new StructType().add("a", IntegerType).add("b", IntegerType))
        spark.createDataFrame(
          spark.sparkContext.parallelize(Seq(Row(1, f1, Row(10, 20))), 1), schema)
          .write.parquet(path)
        withSourceAndReader { (source, reader) =>
          withClue(s"$source, $reader: ") {
            // Pruning fires for the sibling struct column and leaves the FILE column whole.
            val df = spark.read.schema(schema).parquet(path).selectExpr("other.a", "f")
            val readSchema = df.queryExecution.executedPlan.collectFirst {
              case s: FileSourceScanExec => s.requiredSchema
              case s: BatchScanExec => s.scan.readSchema()
            }.get
            assert(readSchema === new StructType()
              .add("f", FileType.external)
              .add("other", new StructType().add("a", IntegerType)))
            checkAnswer(df, Row(10, f1))
            // A FILE column that is not read is pruned.
            val ids = spark.read.schema(schema).parquet(path).select("id")
            val idSchema = ids.queryExecution.executedPlan.collectFirst {
              case s: FileSourceScanExec => s.requiredSchema
              case s: BatchScanExec => s.scan.readSchema()
            }.get
            assert(idSchema === new StructType().add("id", IntegerType))
          }
        }
      }
    }
  }

  test("a cached Parquet scan of FILE values") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val schema = new StructType().add("id", IntegerType).add("f", FileType.external)
      val rows = values.zipWithIndex.map { case (v, i) => Row(i, v) }
      spark.createDataFrame(spark.sparkContext.parallelize(rows, 2), schema).write.parquet(path)
      readers.foreach { case (reader, confs) =>
        withSQLConf(confs: _*) {
          withClue(s"$reader: ") {
            val df = spark.read.parquet(path).cache()
            try {
              checkAnswer(df, rows)
              checkAnswer(df.filter("id < 2"), rows.take(2))
            } finally {
              df.unpersist()
            }
          }
        }
      }
    }
  }

  test("Parquet does not support FILE when the FILE type is disabled") {
    val format = new ParquetFileFormat()
    assert(format.supportDataType(FileType.external))
    assert(format.supportDataType(ArrayType(new StructType().add("f", FileType.external))))
    withSQLConf(SQLConf.FILE_TYPE_ENABLED.key -> "false") {
      assert(!format.supportDataType(FileType.external))
      assert(!format.supportDataType(ArrayType(new StructType().add("f", FileType.external))))
    }
  }
}
