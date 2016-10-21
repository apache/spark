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

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation, PruneFileSourcePartitions, TableFileCatalog}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.StructType

class PruneFileSourcePartitionsSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("PruneFileSourcePartitions", Once, PruneFileSourcePartitions) :: Nil
  }

  test("PruneFileSourcePartitions should not change the output of LogicalRelation") {
    withTable("test") {
      withTempDir { dir =>
        sql(
          s"""
            |CREATE EXTERNAL TABLE test(i int)
            |PARTITIONED BY (p int)
            |STORED AS parquet
            |LOCATION '${dir.getAbsolutePath}'""".stripMargin)

        val tableMeta = spark.sharedState.externalCatalog.getTable("default", "test")
        val tableFileCatalog = new TableFileCatalog(
          spark,
          tableMeta.database,
          tableMeta.identifier.table,
          Some(tableMeta.partitionSchema),
          0)

        val dataSchema = StructType(tableMeta.schema.filterNot { f =>
          tableMeta.partitionColumnNames.contains(f.name)
        })
        val relation = HadoopFsRelation(
          location = tableFileCatalog,
          partitionSchema = tableMeta.partitionSchema,
          dataSchema = dataSchema,
          bucketSpec = None,
          fileFormat = new ParquetFileFormat(),
          options = Map.empty)(sparkSession = spark)

        val logicalRelation = LogicalRelation(relation, catalogTable = Some(tableMeta))
        val query = Project(Seq('i, 'p), Filter('p === 1, logicalRelation)).analyze

        val optimized = Optimize.execute(query)
        assert(optimized.missingInput.isEmpty)
      }
    }
  }
}
