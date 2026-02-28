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

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.AccumulatorContext

/**
 * Tests for row-group skipping on shredded Variant columns in Parquet (DSv1).
 *
 * When a Variant column is written with shredding enabled, each extracted field is stored as a
 * separate typed Parquet column (e.g. `v.typed_value.a.typed_value` for `$.a`) with standard
 * row-group min/max statistics. The PUSH_VARIANT_INTO_SCAN rule rewrites
 * `variant_get(v, '$.a', 'bigint') > 100` into a struct field access `v.`0` > 100`.
 * ParquetFilters then maps the logical path `v.`0`` to the physical shredded column
 * `v.typed_value.a.typed_value` via VariantMetadata, enabling Parquet FilterPredicate to
 * evaluate row-group min/max statistics and skip row groups that cannot contain matching rows.
 */
class VariantShreddingFilterPushdownSuite extends QueryTest with ParquetTest
    with SharedSparkSession {

  private val shreddingWriteConf = Seq(
    SQLConf.VARIANT_WRITE_SHREDDING_ENABLED.key -> "true",
    SQLConf.VARIANT_INFER_SHREDDING_SCHEMA.key -> "true",
    SQLConf.PARQUET_ANNOTATE_VARIANT_LOGICAL_TYPE.key -> "false",
    SQLConf.VARIANT_ALLOW_READING_SHREDDED.key -> "true"
  )

  /**
   * Writes a shredded Variant Parquet file coalesced to 1 partition with a tiny block size so
   * the writer produces at least 2 non-overlapping row groups:
   *   - Row group 0: ids 0..999,     a in [0, 999],    b in ["str0".."str999"]
   *   - Row group 1: ids 1000..1999, a in [1000, 1999], b in ["str1000".."str1999"]
   */
  private def writeTwoRowGroups(dir: java.io.File): Unit = {
    withSQLConf(shreddingWriteConf: _*) {
      spark.sql(
        """SELECT parse_json('{"a":' || id || ', "b":"str' || id || '"}') AS v
          |FROM range(0, 2000, 1, 1)""".stripMargin
      ).coalesce(1)
        .write
        .option("parquet.block.size", 512)
        .mode("overwrite")
        .parquet(dir.getAbsolutePath)
    }
  }

  /**
   * Counts how many Parquet row groups are actually read by running the DataFrame with an
   * accumulator-based counter. Uses the same technique from ParquetFilterSuite.
   */
  private def countRowGroupsRead(df: org.apache.spark.sql.DataFrame): Int = {
    val accu = new NumRowGroupsAcc
    sparkContext.register(accu)
    try {
      df.foreachPartition((it: Iterator[Row]) => it.foreach(_ => accu.add(0)))
      accu.value
    } finally {
      AccumulatorContext.remove(accu.id)
    }
  }

  test("variant_get integer predicate skips row groups and returns correct rows") {
    // Set PARQUET_VECTORIZED_READER_ENABLED tot rue to count read of row groups.
    // ParquetFilters maps "v.`0`" to "v.typed_value.a.typed_value" via VariantMetadata.
    // Row group 0: a in [0, 999]    -- dropped by "a > 999" (max=999 is not > 999)
    // Row group 1: a in [1000, 1999] -- kept
    withTempDir { dir =>
      writeTwoRowGroups(dir)
      withSQLConf(
        SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true",
        SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true",
        SQLConf.VARIANT_ALLOW_READING_SHREDDED.key -> "true"
      ) {
        val dfFiltered = spark.read.parquet(dir.getAbsolutePath)
          .selectExpr("variant_get(v, '$.a', 'bigint') AS a")
          .where("a > 999")
        val dfAll = spark.read.parquet(dir.getAbsolutePath)
          .selectExpr("variant_get(v, '$.a', 'bigint') AS a")

        assert(countRowGroupsRead(dfFiltered) < countRowGroupsRead(dfAll),
          "Expected at least one row group to be skipped by the shredded column statistics")
        checkAnswer(dfFiltered.orderBy("a"), (1000L to 1999L).map(Row(_)))
      }
    }
  }

  test("variant_get string predicate skips row groups and returns correct rows") {
    // Set PARQUET_VECTORIZED_READER_ENABLED to true to count read of row groups.
    // Row group 0: b in ["str0".."str999"]; row group 1: b in ["str1000".."str1999"].
    // "b = 'str1500'" skips row group 0 (str1500 > str999 lexicographically).
    withTempDir { dir =>
      writeTwoRowGroups(dir)
      withSQLConf(
        SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true",
        SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true",
        SQLConf.VARIANT_ALLOW_READING_SHREDDED.key -> "true"
      ) {
        val dfFiltered = spark.read.parquet(dir.getAbsolutePath)
          .selectExpr("variant_get(v, '$.b', 'string') AS b")
          .where("b = 'str1500'")
        val dfAll = spark.read.parquet(dir.getAbsolutePath)
          .selectExpr("variant_get(v, '$.b', 'string') AS b")

        assert(countRowGroupsRead(dfFiltered) < countRowGroupsRead(dfAll),
          "Expected at least one row group to be skipped by the shredded string column statistics")
        checkAnswer(dfFiltered, Seq(Row("str1500")))
      }
    }
  }
}
