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

import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class LogicalRelationSuite extends SharedSparkSession {

  test("LogicalRelation statistics's sizeInBytes will be file size " +
    "if runtime filter enable.") {
    withTable("test") {
      spark.conf.set(SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key, "true")
      spark.conf.set(SQLConf.RUNTIME_FILTER_SEMI_JOIN_REDUCTION_ENABLED.key, "true")
      spark.range(10).selectExpr("id", "id % 3 as p").write.partitionBy("p")
        .saveAsTable("test")
      val tableMeta = spark.sharedState.externalCatalog.getTable("default", "test")
      val catalogFileIndex = new CatalogFileIndex(
        spark, tableMeta, spark.sessionState.conf.getConf(SQLConf.DEFAULT_SIZE_IN_BYTES))

      val dataSchema = StructType(tableMeta.schema.filterNot { f =>
        tableMeta.partitionColumnNames.contains(f.name)
      })
      val relation = HadoopFsRelation(
        location = catalogFileIndex,
        partitionSchema = tableMeta.partitionSchema,
        dataSchema = dataSchema,
        bucketSpec = None,
        fileFormat = new ParquetFileFormat(),
        options = Map.empty)(sparkSession = spark)

      val logicalRelation = LogicalRelation(relation, tableMeta)
      val statistics = logicalRelation.computeStats()
      assert(statistics.sizeInBytes == 2863)
    }
  }

  test("LogicalRelation statistics's sizeInBytes will be defaultSizeInBytes " +
    "if runtime filter not enable.") {
    withTable("test") {
      spark.conf.set(SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key, "false")
      spark.conf.set(SQLConf.RUNTIME_FILTER_SEMI_JOIN_REDUCTION_ENABLED.key, "false")
      spark.range(10).selectExpr("id", "id % 3 as p").write.partitionBy("p")
        .saveAsTable("test")
      val tableMeta = spark.sharedState.externalCatalog.getTable("default", "test")
      val catalogFileIndex = new CatalogFileIndex(
        spark, tableMeta, spark.sessionState.conf.getConf(SQLConf.DEFAULT_SIZE_IN_BYTES))

      val dataSchema = StructType(tableMeta.schema.filterNot { f =>
        tableMeta.partitionColumnNames.contains(f.name)
      })
      val relation = HadoopFsRelation(
        location = catalogFileIndex,
        partitionSchema = tableMeta.partitionSchema,
        dataSchema = dataSchema,
        bucketSpec = None,
        fileFormat = new ParquetFileFormat(),
        options = Map.empty)(sparkSession = spark)

      val logicalRelation = LogicalRelation(relation, tableMeta)
      val statistics = logicalRelation.computeStats()
      val defaultSizeInBytes = conf.getConf(SQLConf.DEFAULT_SIZE_IN_BYTES)
      assert(statistics.sizeInBytes == defaultSizeInBytes)
    }
  }

}

