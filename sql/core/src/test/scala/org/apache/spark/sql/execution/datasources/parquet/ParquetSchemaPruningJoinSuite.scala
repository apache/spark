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
import org.apache.spark.sql.execution.FileSchemaPruningTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

class ParquetSchemaPruningJoinSuite
    extends QueryTest
    with ParquetTest
    with FileSchemaPruningTest
    with SharedSQLContext {
  setupTestData()

  private lazy val upperCaseStructData: DataFrame = {
    val df = sql("select named_struct(\"N\", N, \"L\", L) as S from uppercasedata")
    df.createOrReplaceTempView("upperCaseStruct")
    df
  }

  private lazy val lowerCaseStructData: DataFrame = {
    val df = sql("select named_struct(\"n\", n, \"l\", l) as s from lowercasedata")
    df.createOrReplaceTempView("lowerCaseStruct")
    df
  }

  override def loadTestData(): Unit = {
    super.loadTestData
    upperCaseStructData
    lowerCaseStructData
  }

  testStandardAndLegacyModes("schema pruning join 1") {
    asParquetTable(upperCaseStructData, "r1") {
      asParquetTable(lowerCaseStructData, "r2") {
        withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
          def join(joinType: String): DataFrame =
            sql(s"select s.n from r1 $joinType join r2 on r1.S.N = r2.s.n")
          val scanSchema1 = "struct<S:struct<N:int>>"
          val scanSchema2 = "struct<s:struct<n:int>>"
          checkScanSchemata(
            join("inner"),
            scanSchema1,
            scanSchema2)
          checkScanSchemata(
            join("left outer"),
            scanSchema1,
            scanSchema2)
          checkScanSchemata(
            join("right outer"),
            scanSchema1,
            scanSchema2)
          checkScanSchemata(
            join("full outer"),
            scanSchema1,
            scanSchema2)
        }
      }
    }
  }

  testStandardAndLegacyModes("schema pruning join 2") {
    asParquetTable(upperCaseStructData, "r1") {
      asParquetTable(lowerCaseStructData, "r2") {
        withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
          def join(joinType: String): DataFrame =
            sql(s"select s.l from r1 $joinType join r2 on r1.S.N = r2.s.n")
          val scanSchema1 = "struct<S:struct<N:int>>"
          val scanSchema2 = "struct<s:struct<l:string,n:int>>"
          checkScanSchemata(
            join("inner"),
            scanSchema1,
            scanSchema2)
          checkScanSchemata(
            join("left outer"),
            scanSchema1,
            scanSchema2)
          checkScanSchemata(
            join("right outer"),
            scanSchema1,
            scanSchema2)
          checkScanSchemata(
            join("full outer"),
            scanSchema1,
            scanSchema2)
        }
      }
    }
  }

  testStandardAndLegacyModes("schema pruning join 3") {
    asParquetTable(upperCaseStructData, "r1") {
      asParquetTable(lowerCaseStructData, "r2") {
        withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
          def join(joinType: String): DataFrame =
            sql(s"select S.L from r1 $joinType join r2 on r1.S.N = r2.s.n")
          val scanSchema1 = "struct<S:struct<L:string,N:int>>"
          val scanSchema2 = "struct<s:struct<n:int>>"
          checkScanSchemata(
            join("inner"),
            scanSchema1,
            scanSchema2)
          checkScanSchemata(
            join("left outer"),
            scanSchema1,
            scanSchema2)
          checkScanSchemata(
            join("right outer"),
            scanSchema1,
            scanSchema2)
          checkScanSchemata(
            join("full outer"),
            scanSchema1,
            scanSchema2)
        }
      }
    }
  }

  testStandardAndLegacyModes("schema pruning join 4") {
    asParquetTable(upperCaseStructData, "r1") {
      asParquetTable(lowerCaseStructData, "r2") {
        withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
          def join(joinType: String): DataFrame =
            sql(s"select count(s.n) from r1 $joinType join r2 on r1.S.N = r2.s.n")
          val scanSchema1 = "struct<S:struct<N:int>>"
          val scanSchema2 = "struct<s:struct<n:int>>"
          checkScanSchemata(
            join("inner"),
            scanSchema1,
            scanSchema2)
          checkScanSchemata(
            join("left outer"),
            scanSchema1,
            scanSchema2)
          checkScanSchemata(
            join("right outer"),
            scanSchema1,
            scanSchema2)
          checkScanSchemata(
            join("full outer"),
            scanSchema1,
            scanSchema2)
        }
      }
    }
  }

  testStandardAndLegacyModes("schema pruning join 5") {
    asParquetTable(upperCaseStructData, "r1") {
      asParquetTable(lowerCaseStructData, "r2") {
        withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
          def join(joinType: String): DataFrame =
            sql(s"select count(1), s.n from r1 $joinType join r2 on r1.S.N = r2.s.n group by s.n")
          val scanSchema1 = "struct<S:struct<N:int>>"
          val scanSchema2 = "struct<s:struct<n:int>>"
          checkScanSchemata(
            join("inner"),
            scanSchema1,
            scanSchema2)
          checkScanSchemata(
            join("left outer"),
            scanSchema1,
            scanSchema2)
          checkScanSchemata(
            join("right outer"),
            scanSchema1,
            scanSchema2)
          checkScanSchemata(
            join("full outer"),
            scanSchema1,
            scanSchema2)
        }
      }
    }
  }
}
