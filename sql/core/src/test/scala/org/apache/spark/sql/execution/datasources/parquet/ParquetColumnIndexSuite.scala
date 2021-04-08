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

import org.apache.spark.sql._
import org.apache.spark.sql.test.SharedSparkSession


class ParquetColumnIndexSuite extends QueryTest with ParquetTest with SharedSparkSession {

  import testImplicits._

  /**
    * create parquet file with two columns and unaligned pages
    * pages will be of the following layout
    * col_1     500       500       500       500
    *  |---------|---------|---------|---------|
    *  |-------|-----|-----|---|---|---|---|---|
    * col_2   400   300   200 200 200 200 200 200
    */
  def checkUnalignedPages(action: DataFrame => DataFrame): Unit = {
    withTempPath(file => {
      val ds = spark.range(0, 2000).map(i => (i, i + ":" + "o" * (i / 100).toInt))
      ds.coalesce(1)
        .write
        .option("parquet.page.size", "4096")
        .parquet(file.getCanonicalPath)

      val parquetDf = spark.read.parquet(file.getCanonicalPath)

      checkAnswer(action(parquetDf), action(ds.toDF()))
    })
  }

  test("read from unaligned pages - single value filters") {
    checkUnalignedPages(df => df.filter("_1 = 500"))
    checkUnalignedPages(df => df.filter("_1 = 500 or _1 = 1500"))
    checkUnalignedPages(df => df.filter("_1 = 500 or _1 = 501 or _1 = 1500"))
    checkUnalignedPages(df => df.filter("_1 = 500 or _1 = 501 or _1 = 1000 or _1 = 1500"))
  }

  test("read from unaligned pages - range filter") {
    checkUnalignedPages(df => df.filter("_1 >= 500 and _1 < 1000"))
    checkUnalignedPages(df => df.filter("(_1 >= 500 and _1 < 1000) or (_1 >= 1500 and _1 < 1600)"))
  }
}
