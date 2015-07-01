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

package org.apache.spark.sql.parquet

import org.apache.spark.sql.{SQLConf, QueryTest}
import org.apache.spark.sql.test.TestSQLContext

case class ParquetData2(int2Field: Int, string2Field: String)


class ParquetSchemaMergeConfigSuite  extends QueryTest with ParquetTest {
  val sqlContext = TestSQLContext
  val defaultPartitionName = "__HIVE_DEFAULT_PARTITION__"
  case class ParquetData2(int2Field: Int, string2Field: String)

  test("Disable SchemaMerge") {
    sqlContext.conf.setConf(SQLConf.PARQUET_MERGE_SCHEMA_ENABLED, false)
    withTempDir { base =>
      val parquetFile1 = makePartitionDir(base, defaultPartitionName , "lala" -> 1)
      makeParquetFile( (1 to 10).map(i => ParquetData(i,i.toString)),parquetFile1)
      val parquetFile2 = makePartitionDir(base, defaultPartitionName ,  "lala" -> 2)
      makeParquetFile( (1 to 10).map(i => ParquetData2(i,i.toString)),parquetFile2)
      assert( sqlContext.read.parquet(parquetFile1.toString , parquetFile2.toString)
                .columns.size == 3)
    }
  }

  test("Enable SchemaMerge") {
    sqlContext.conf.setConf(SQLConf.PARQUET_MERGE_SCHEMA_ENABLED, true)
    withTempDir { base =>
      val parquetFile1 = makePartitionDir(base, defaultPartitionName , "lala" -> 1)
      makeParquetFile( (1 to 10).map(i => ParquetData(i,i.toString)),parquetFile1)
      val parquetFile2 = makePartitionDir(base, defaultPartitionName ,  "lala" -> 2)
      makeParquetFile( (1 to 10).map(i => ParquetData2(i,i.toString)),parquetFile2)
      assert( sqlContext.read.parquet(parquetFile1.toString , parquetFile2.toString)
        .columns.size == 5)
    }
  }

}
