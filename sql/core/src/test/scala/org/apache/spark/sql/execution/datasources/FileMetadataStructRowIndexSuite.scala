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

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StructField, StructType}

class FileMetadataStructRowIndexSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  val EXPECTED_ROW_ID_COL = "expected_row_idx"
  val EXPECTED_EXTRA_COL = "expected_extra_col"
  val EXPECTED_PARTITION_COL = "experted_pb_col"
  val NUM_ROWS = 100

  def withReadDataFrame(
         format: String,
         partitionCol: String = null,
         extraCol: String = "ec",
         extraSchemaFields: Seq[StructField] = Seq.empty)
      (f: DataFrame => Unit): Unit = {
    withTempPath { path =>
      val baseDf = spark.range(0, NUM_ROWS, 1, 1).toDF("id")
        .withColumn(extraCol, $"id" + lit(1000 * 1000))
        .withColumn(EXPECTED_EXTRA_COL, col(extraCol))
      val writeSchema: StructType = if (partitionCol != null) {
        val writeDf = baseDf
          .withColumn(partitionCol, ($"id" / 10).cast("int") + lit(1000))
          .withColumn(EXPECTED_PARTITION_COL, col(partitionCol))
          .withColumn(EXPECTED_ROW_ID_COL, $"id" % 10)
        writeDf.write.format(format).partitionBy(partitionCol).save(path.getAbsolutePath)
        writeDf.schema
      } else {
        val writeDf = baseDf
          .withColumn(EXPECTED_ROW_ID_COL, $"id")
        writeDf.write.format(format).save(path.getAbsolutePath)
        writeDf.schema
      }
      val readSchema: StructType = new StructType(writeSchema.fields ++ extraSchemaFields)
      val readDf = spark.read.format(format).schema(readSchema).load(path.getAbsolutePath)
      f(readDf)
    }
  }

  private val allMetadataCols = Seq(
    FileFormat.FILE_PATH,
    FileFormat.FILE_SIZE,
    FileFormat.FILE_MODIFICATION_TIME,
    FileFormat.ROW_INDEX
  )

  /** Identifies the names of all the metadata columns present in the schema. */
  private def collectMetadataCols(struct: StructType): Seq[String] = {
    struct.fields.flatMap { field => field.dataType match {
      case s: StructType => collectMetadataCols(s)
      case _ if allMetadataCols.contains(field.name) => Some(field.name)
      case _ => None
    }}
  }

  for (useVectorizedReader <- Seq(false, true))
  for (useOffHeapMemory <- Seq(useVectorizedReader, false).distinct)
  for (partitioned <- Seq(false, true)) {
    val label = Seq(
        { if (useVectorizedReader) "vectorized" else "parquet-mr"},
        { if (useOffHeapMemory) "off-heap" else "" },
        { if (partitioned) "partitioned" else "" }
      ).filter(_.nonEmpty).mkString(", ")
    test(s"parquet ($label) - read _metadata.row_index") {
      withSQLConf(
          SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> useVectorizedReader.toString,
          SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED.key -> useOffHeapMemory.toString) {
        withReadDataFrame("parquet", partitionCol = "pb") { df =>
          val res = df.select("*", s"${FileFormat.METADATA_NAME}.${FileFormat.ROW_INDEX}")
            .where(s"$EXPECTED_ROW_ID_COL != ${FileFormat.ROW_INDEX}")
          assert(res.count() == 0)
        }
      }
    }
  }

  test("supported file format - read _metadata struct") {
    withReadDataFrame("parquet") { df =>
      val withMetadataStruct = df.select("*", FileFormat.METADATA_NAME)

      // `_metadata.row_index` column is present when selecting `_metadata` as a whole.
      val metadataCols = collectMetadataCols(withMetadataStruct.schema)
      assert(metadataCols.contains(FileFormat.ROW_INDEX))
    }
  }

  test("unsupported file format - read _metadata struct") {
    withReadDataFrame("orc") { df =>
      val withMetadataStruct = df.select("*", FileFormat.METADATA_NAME)

      // Metadata struct can be read without an error.
      withMetadataStruct.collect()

      // Schema does not contain row index column, but contains all the remaining metadata columns.
      val metadataCols = collectMetadataCols(withMetadataStruct.schema)
      assert(!metadataCols.contains(FileFormat.ROW_INDEX))
      assert(allMetadataCols.intersect(metadataCols).size == allMetadataCols.size - 1)
    }
  }

  test("unsupported file format - read _metadata.row_index") {
    withReadDataFrame("orc") { df =>
      val ex = intercept[AnalysisException] {
        df.select("*", s"${FileFormat.METADATA_NAME}.${FileFormat.ROW_INDEX}")
      }
      assert(ex.getMessage.contains("No such struct field row_index"))
    }
  }

  for (useVectorizedReader <- Seq(true, false)) {
    val label = if (useVectorizedReader) "vectorized" else "parquet-mr"
    test(s"parquet ($label) - use mixed case for column name") {
      withSQLConf(
          SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> useVectorizedReader.toString) {
        withReadDataFrame("parquet") { df =>
          val mixedCaseRowIndex = "RoW_InDeX"
          assert(mixedCaseRowIndex.toLowerCase() == FileFormat.ROW_INDEX)

          assert(df.select("*", s"${FileFormat.METADATA_NAME}.$mixedCaseRowIndex")
            .where(s"$EXPECTED_ROW_ID_COL != $mixedCaseRowIndex")
            .count == 0)
        }
      }
    }
  }

  test(s"reading ${FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME} - not present in a table") {
    // File format supporting row index generation populates the column with row indexes.
    withReadDataFrame("parquet", extraSchemaFields =
        Seq(StructField(FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME, LongType))) { df =>
      assert(df
          .where(col(EXPECTED_ROW_ID_COL) === col(FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME))
          .count == NUM_ROWS)
    }

    // File format not supporting row index generation populates missing column with nulls.
    withReadDataFrame("json", extraSchemaFields =
        Seq(StructField(FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME, LongType)))  { df =>
      assert(df
          .where(col(FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME).isNull)
          .count == NUM_ROWS)
    }
  }

  test(s"reading ${FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME} - present in a table") {
    withReadDataFrame("parquet", extraCol = FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME) { df =>
      // Values of FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME column are always populated with
      // generated row indexes, rather than read from the file.
      // TODO(SPARK-40059): Allow users to include columns named
      //                    FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME in their schemas.
      assert(df
        .where(col(EXPECTED_ROW_ID_COL) === col(FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME))
        .count == NUM_ROWS)

      // Column cannot be read in combination with _metadata.row_index.
      intercept[AnalysisException](df.select("*", FileFormat.METADATA_NAME).collect())
      intercept[AnalysisException](df
        .select("*", s"${FileFormat.METADATA_NAME}.${FileFormat.ROW_INDEX}").collect())
    }
  }

  test(s"reading ${FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME} - as partition col") {
    withReadDataFrame("parquet", partitionCol = FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME) { df =>
      // Column values are set for each partition, rather than populated with generated row indexes.
      assert(df
        .where(col(EXPECTED_PARTITION_COL) === col(FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME))
        .count == NUM_ROWS)

      // Column cannot be read in combination with _metadata.row_index.
      intercept[AnalysisException](df.select("*", FileFormat.METADATA_NAME).collect())
      intercept[AnalysisException](df
        .select("*", s"${FileFormat.METADATA_NAME}.${FileFormat.ROW_INDEX}").collect())
    }
  }

  test(s"cannot make ${FileFormat.METADATA_NAME}.${FileFormat.ROW_INDEX} a partition column") {
    withTempPath { srcPath =>
      spark.range(0, 10, 1, 1).toDF("id").write.parquet(srcPath.getAbsolutePath)

      withTempPath { dstPath =>
        intercept[AnalysisException] {
          spark.read.parquet(srcPath.getAbsolutePath)
            .select("*", FileFormat.METADATA_NAME)
            .write
            .partitionBy(s"${FileFormat.METADATA_NAME}.${FileFormat.ROW_INDEX}")
            .save(dstPath.getAbsolutePath)
        }
      }
    }
  }

  test(s"read user created ${FileFormat.METADATA_NAME}.${FileFormat.ROW_INDEX} column") {
    withReadDataFrame("parquet", partitionCol = "pb") { df =>
      withTempPath { dir =>
        // The `df` has 10 input files with 10 rows each. Therefore the `_metadata.row_index` values
        // will be { 10 x 0, 10 x 1, ..., 10 x 9 }. We store all these values in a single file.
        df.select("id", s"${FileFormat.METADATA_NAME}")
          .coalesce(1)
          .write.parquet(dir.getAbsolutePath)

        assert(spark
          .read.parquet(dir.getAbsolutePath)
          .count == NUM_ROWS)

        // The _metadata.row_index is returning data from the file, not generated metadata.
        assert(spark
          .read.parquet(dir.getAbsolutePath)
          .select(s"${FileFormat.METADATA_NAME}.${FileFormat.ROW_INDEX}")
          .distinct.count == NUM_ROWS / 10)
      }
    }
  }
}
