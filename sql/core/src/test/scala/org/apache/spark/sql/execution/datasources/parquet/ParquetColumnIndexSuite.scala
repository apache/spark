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

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class ParquetColumnIndexSuite extends ParquetTest with SharedSparkSession {
  import testImplicits._

  private val actions: Seq[DataFrame => DataFrame] = Seq(
    "_1 = 500",
    "_1 = 500 or _1 = 1500",
    "_1 = 500 or _1 = 501 or _1 = 1500",
    "_1 = 500 or _1 = 501 or _1 = 1000 or _1 = 1500",
    "_1 >= 500 and _1 < 1000",
    "(_1 >= 500 and _1 < 1000) or (_1 >= 1500 and _1 < 1600)"
  ).map(f => (df: DataFrame) => df.filter(f))

  private val structActions: Seq[DataFrame => DataFrame] = Seq(
    "s._1 = 500",
    "s._1 = 500 or s._1 = 1500",
    "s._1 = 500 or s._1 = 501 or s._1 = 1500",
    "s._1 = 500 or s._1 = 501 or s._1 = 1000 or s._1 = 1500",
    "s._1 >= 500 and s._1 < 1000",
    "(s._1 >= 500 and s._1 < 1000) or (s._1 >= 1500 and s._1 < 1600)"
  ).map(f => (df: DataFrame) => df.filter(f))

  /**
   * create parquet file with two columns and unaligned pages
   * pages will be of the following layout
   * col_1     500       500       500       500
   *  |---------|---------|---------|---------|
   *  |-------|-----|-----|---|---|---|---|---|
   * col_2   400   300   300 200 200 200 200 200
   * See `parquetV2Options` for the layout with the Parquet v2 writer.
   */
  def checkUnalignedPages(df: DataFrame, parquetV2: Boolean = false, filterColumn: String = "_1")(
      actions: (DataFrame => DataFrame)*): Unit = {
    val extraOptions = if (parquetV2) parquetV2Options(filterColumn) else Map.empty[String, String]
    Seq(true, false).foreach { enableDictionary =>
      withTempPath(file => {
        df.coalesce(1)
            .write
            .option("parquet.page.size", "4096")
            .option("parquet.enable.dictionary", enableDictionary.toString)
            // Applied last to override the options above, e.g. the page size.
            .options(extraOptions)
            .parquet(file.getCanonicalPath)

        // Column index filtering skips nothing unless the filter column has multiple pages.
        val parquetFile = file.listFiles().filter(_.getName.startsWith("part")).head
        val in = HadoopInputFile.fromPath(
          new Path(parquetFile.getCanonicalPath), spark.sessionState.newHadoopConf())
        Utils.tryWithResource(ParquetFileReader.open(in)) { reader =>
          val column = reader.getFooter.getBlocks.get(0).getColumns.asScala
            .find(_.getPath.toDotString == filterColumn).get
          assert(reader.readOffsetIndex(column).getPageCount > 1)
        }

        val parquetDf = spark.read.parquet(file.getCanonicalPath)

        actions.foreach { action =>
          checkAnswer(action(parquetDf), action(df))
        }
      })
    }
  }

  /**
   * Options to write unaligned pages with the Parquet v2 writer, which encodes INT32/INT64 with
   * DELTA_BINARY_PACKED, BINARY/FIXED_LEN_BYTE_ARRAY with DELTA_BYTE_ARRAY and BOOLEAN with RLE.
   * DELTA_BINARY_PACKED makes the sequential filter column too small to be split into pages, so
   * dictionary encoding is always enabled for it. Then its pages are split by the plain encoded
   * size (about 400 rows per page) even after falling back to DELTA_BINARY_PACKED, and the
   * filters start inside a page of the columns written with the v2 encodings.
   */
  private def parquetV2Options(filterColumn: String): Map[String, String] = Map(
    "parquet.writer.version" -> "PARQUET_2_0",
    "parquet.page.size" -> "3072",
    s"parquet.enable.dictionary#$filterColumn" -> "true")

  test("reading from unaligned pages - test filters") {
    val df = spark.range(0, 2000).map(i => (i, s"$i:${"o".repeat((i / 100).toInt)}")).toDF()
    checkUnalignedPages(df)(actions: _*)
  }

  private def allTypesDf: DataFrame = spark.range(0, 2000).selectExpr(
    "id as _1",
    "cast(id as short) as _3",
    "cast(id as int) as _4",
    "cast(id as float) as _5",
    "cast(id as double) as _6",
    "cast(id as decimal(20,0)) as _7",
    "cast(cast(1618161925000 + id * 1000 * 60 * 60 * 24 as timestamp) as date) as _9",
    "cast(1618161925000 + id as timestamp) as _10"
  )

  test("test reading unaligned pages - test all types") {
    checkUnalignedPages(allTypesDf)(actions: _*)
  }

  test("test reading unaligned pages - test all types (Parquet v2 writer)") {
    checkUnalignedPages(allTypesDf, parquetV2 = true)(actions: _*)
  }

  private def allTypesDictEncodeDf: DataFrame = spark.range(0, 2000).selectExpr(
    "id as _1",
    "cast(id % 10 as byte) as _2",
    "cast(id % 10 as short) as _3",
    "cast(id % 10 as int) as _4",
    "cast(id % 10 as float) as _5",
    "cast(id % 10 as double) as _6",
    "cast(id % 10 as decimal(20,0)) as _7",
    "cast(id % 2 as boolean) as _8",
    "cast(cast(1618161925000 + (id % 10) * 1000 * 60 * 60 * 24 as timestamp) as date) as _9",
    "cast(1618161925000 + (id % 10) as timestamp) as _10"
  )

  test("test reading unaligned pages - test all types (dict encode)") {
    checkUnalignedPages(allTypesDictEncodeDf)(actions: _*)
  }

  test("test reading unaligned pages - test all types (dict encode, Parquet v2 writer)") {
    checkUnalignedPages(allTypesDictEncodeDf, parquetV2 = true)(actions: _*)
  }

  private def nullsDf: DataFrame = spark.range(0, 2000).map { i =>
    val strVal = if (i >= 400 && i < 450) null else s"$i:${"o".repeat((i / 100).toInt)}"
    (i, strVal)
  }.toDF()

  test("SPARK-36123: reading from unaligned pages - test filters with nulls") {
    // insert 50 null values in [400, 450) to verify that they are skipped during processing row
    // range [500, 1000) against the second page of col_2 [400, 800)
    checkUnalignedPages(nullsDf)(actions: _*)
  }

  test("reading from unaligned pages - test filters with nulls (Parquet v2 writer)") {
    checkUnalignedPages(nullsDf, parquetV2 = true)(actions: _*)
  }

  private def structDf: DataFrame =
    (0 until 2000).map(i => Tuple1((i.toLong, s"$i:${"o".repeat(i / 100)}"))).toDF("s")

  test("reading unaligned pages - struct type") {
    checkUnalignedPages(structDf, filterColumn = "s._1")(structActions: _*)
  }

  test("reading unaligned pages - struct type (Parquet v2 writer)") {
    checkUnalignedPages(structDf, parquetV2 = true, filterColumn = "s._1")(structActions: _*)
  }

  test("SPARK-59828: reading unaligned pages - FIXED_LEN_BYTE_ARRAY with DELTA_BYTE_ARRAY") {
    // Without a dictionary, the v2 writer encodes FIXED_LEN_BYTE_ARRAY as DELTA_BYTE_ARRAY.
    // The pages of _2 are split by size, and those of _1 by row count because its
    // DELTA_BINARY_PACKED values are tiny, so that the pages of the two columns are unaligned.
    val df = spark.range(0, 2000).selectExpr("id as _1", "cast(id as decimal(38, 18)) as _2")
    withTempPath { file =>
      df.coalesce(1)
        .write
        .option("parquet.writer.version", "PARQUET_2_0")
        .option("parquet.enable.dictionary", "false")
        .option("parquet.page.size", "2048")
        .option("parquet.page.row.count.limit", "500")
        .parquet(file.getCanonicalPath)

      val parquetDf = spark.read.parquet(file.getCanonicalPath)
      actions.foreach { action =>
        checkAnswer(action(parquetDf), action(df))
      }
    }
  }

  test("SPARK-59831: reading unaligned pages - BYTE_STREAM_SPLIT") {
    // BYTE_STREAM_SPLIT is enabled for the floating point columns, whose pages are split by
    // size. The pages of _1 are split by size with v1 pages, where it is PLAIN encoded, and by
    // row count with v2 pages, where its DELTA_BINARY_PACKED values are tiny. Either way they
    // are unaligned with the BYTE_STREAM_SPLIT pages, so the rows outside the row ranges are
    // skipped within those pages. _2 and _3 are required, and _4 and _5 have nulls.
    val df = spark.range(0, 2000).selectExpr(
      "id as _1",
      "cast(id as float) as _2",
      "cast(id as double) as _3",
      "if(id >= 400 and id < 450, null, cast(id as float)) as _4",
      "if(id % 3 = 0, null, cast(id as double)) as _5")
    Seq("PARQUET_1_0", "PARQUET_2_0").foreach { version =>
      withTempPath { file =>
        df.coalesce(1)
          .write
          .option("parquet.writer.version", version)
          .option("parquet.enable.dictionary", "false")
          .option("parquet.enable.bytestreamsplit", "true")
          .option("parquet.page.size", "1024")
          .option("parquet.page.row.count.limit", "500")
          .parquet(file.getCanonicalPath)

        val parquetDf = spark.read.parquet(file.getCanonicalPath)
        actions.foreach { action =>
          checkAnswer(action(parquetDf), action(df))
        }
      }
    }
  }
}
