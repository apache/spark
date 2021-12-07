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

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.test.SharedSparkSession

class ParquetColumnIndexSuite extends QueryTest with ParquetTest with SharedSparkSession {
  import testImplicits._

  private val actions: Seq[DataFrame => DataFrame] = Seq(
    "_1 = 500",
    "_1 = 500 or _1 = 1500",
    "_1 = 500 or _1 = 501 or _1 = 1500",
    "_1 = 500 or _1 = 501 or _1 = 1000 or _1 = 1500",
    "_1 >= 500 and _1 < 1000",
    "(_1 >= 500 and _1 < 1000) or (_1 >= 1500 and _1 < 1600)"
  ).map(f => (df: DataFrame) => df.filter(f))

  /**
   * create parquet file with two columns and unaligned pages
   * pages will be of the following layout
   * col_1     500       500       500       500
   *  |---------|---------|---------|---------|
   *  |-------|-----|-----|---|---|---|---|---|
   * col_2   400   300   300 200 200 200 200 200
   */
  def checkUnalignedPages(df: DataFrame)(actions: (DataFrame => DataFrame)*): Unit = {
    Seq(true, false).foreach { enableDictionary =>
      withTempPath(file => {
        df.coalesce(1)
            .write
            .option("parquet.page.size", "4096")
            .option("parquet.enable.dictionary", enableDictionary.toString)
            .parquet(file.getCanonicalPath)

        val parquetDf = spark.read.parquet(file.getCanonicalPath)

        actions.foreach { action =>
          checkAnswer(action(parquetDf), action(df))
        }
      })
    }
  }

  test("reading from unaligned pages - test filters") {
    val df = spark.range(0, 2000).map(i => (i, i + ":" + "o" * (i / 100).toInt)).toDF()
    checkUnalignedPages(df)(actions: _*)
  }

  test("test reading unaligned pages - test all types") {
    val df = spark.range(0, 2000).selectExpr(
      "id as _1",
      "cast(id as short) as _3",
      "cast(id as int) as _4",
      "cast(id as float) as _5",
      "cast(id as double) as _6",
      "cast(id as decimal(20,0)) as _7",
      "cast(cast(1618161925000 + id * 1000 * 60 * 60 * 24 as timestamp) as date) as _9",
      "cast(1618161925000 + id as timestamp) as _10"
    )
    checkUnalignedPages(df)(actions: _*)
  }

  test("test reading unaligned pages - test all types (dict encode)") {
    val df = spark.range(0, 2000).selectExpr(
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
    checkUnalignedPages(df)(actions: _*)
  }

  test("SPARK-36123: reading from unaligned pages - test filters with nulls") {
    // insert 50 null values in [400, 450) to verify that they are skipped during processing row
    // range [500, 1000) against the second page of col_2 [400, 800)
    val df = spark.range(0, 2000).map { i =>
      val strVal = if (i >= 400 && i < 450) null else i + ":" + "o" * (i / 100).toInt
      (i, strVal)
    }.toDF()
    checkUnalignedPages(df)(actions: _*)
  }
}
