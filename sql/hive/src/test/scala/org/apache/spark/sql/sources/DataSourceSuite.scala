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

package org.apache.spark.sql.sources

import java.sql.{Date, Timestamp}

import org.apache.orc.OrcConf

import org.apache.spark.sql.{Dataset, QueryTest, Row}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

/**
 * Data Source qualification as Apache Spark Data Sources.
 * - Apache Spark Data Type Value Limits: CSV, JSON, ORC, Parquet
 * - Predicate Push Down: ORC
 */
class DataSourceSuite
  extends QueryTest
  with SQLTestUtils
  with TestHiveSingleton {

  import testImplicits._

  var df: Dataset[Row] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set("spark.sql.session.timeZone", "GMT")

    df = ((
      false,
      true,
      Byte.MinValue,
      Byte.MaxValue,
      Short.MinValue,
      Short.MaxValue,
      Int.MinValue,
      Int.MaxValue,
      Long.MinValue,
      Long.MaxValue,
      Float.MinValue,
      Float.MaxValue,
      Double.MinValue,
      Double.MaxValue,
      Date.valueOf("0001-01-01"),
      Date.valueOf("9999-12-31"),
      new Timestamp(-62135769600000L), // 0001-01-01 00:00:00.000
      new Timestamp(253402300799999L)  // 9999-12-31 23:59:59.999
    ) :: Nil).toDF()
  }

  override def afterAll(): Unit = {
    try {
      spark.conf.unset("spark.sql.session.timeZone")
    } finally {
      super.afterAll()
    }
  }

  Seq("parquet", "orc", "json", "csv").foreach { dataSource =>
    test(s"$dataSource - data type value limit") {
      withTempPath { dir =>
        df.write.format(dataSource).save(dir.getCanonicalPath)

        // Use the same schema for saving/loading
        checkAnswer(
          spark.read.format(dataSource).schema(df.schema).load(dir.getCanonicalPath),
          df)

        // Use schema inference, but skip text-based format due to its limitation
        if (Seq("parquet", "orc").contains(dataSource)) {
          withTable("tab1") {
            sql(s"CREATE TABLE tab1 USING $dataSource LOCATION '${dir.toURI}'")
            checkAnswer(sql(s"SELECT ${df.schema.fieldNames.mkString(",")} FROM tab1"), df)
          }
        }
      }
    }
  }

  Seq("orc").foreach { dataSource =>
    test(s"$dataSource - predicate push down") {
      withSQLConf(
        SQLConf.ORC_FILTER_PUSHDOWN_ENABLED.key -> "true",
        SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true") {
        withTempPath { dir =>
          // write 4000 rows with the integer and the string in a single orc file with stride 1000
          spark
            .range(4000)
            .map(i => (i, s"$i"))
            .toDF("i", "s")
            .repartition(1)
            .write
            .option(OrcConf.ROW_INDEX_STRIDE.getAttribute, 1000)
            // TODO: Add Parquet option, too.
            .format(dataSource)
            .save(dir.getCanonicalPath)

          val df = spark.read.format(dataSource).load(dir.getCanonicalPath)
            .where(s"i BETWEEN 1500 AND 1999")
          // skip over the rows except 1000 to 2000
          val answer = spark.range(1000, 2000).map(i => (i, s"$i")).toDF("i", "s")
          checkAnswer(stripSparkFilter(df), answer)
        }
      }
    }
  }
}
