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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ParquetPartitionFilterSuite extends QueryTest with ParquetTest with SharedSparkSession {
  private val defaultPartitionName = ExternalCatalogUtils.DEFAULT_PARTITION_NAME

  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")

  test("filter partition key included in Parquet file") {
    withTempDir { base =>
      for {
        pi <- Seq(1, 2)
        ps <- Seq("foo", "bar")
      } {
        makeParquetFile(
          (1 to 10).map(i => ParquetDataWithKey(i, pi, i.toString, ps)),
          makePartitionDir(base, defaultPartitionName, "pi" -> pi, "ps" -> ps))
      }

      withTempView("t") {
        spark.read.parquet(base.getCanonicalPath).createOrReplaceTempView("t")
        Seq(true, false).foreach { enableVectorizedReader =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> s"$enableVectorizedReader") {
            checkAnswer(
              sql("SELECT intField, stringField, pi, ps FROM t WHERE pi = 1"),
              for {
                i <- 1 to 10
                ps <- Seq("foo", "bar")
              } yield Row(i, i.toString, 1, ps))
          }
        }
      }
    }
  }
}
