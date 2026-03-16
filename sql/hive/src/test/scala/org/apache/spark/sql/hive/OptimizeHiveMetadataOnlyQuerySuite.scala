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

package org.apache.spark.sql.hive

import org.scalatest.BeforeAndAfter

import org.apache.spark.metrics.source.HiveCatalogMetrics
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.{Distinct, Filter, Project, SubqueryAlias}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf.OPTIMIZER_METADATA_ONLY
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class OptimizeHiveMetadataOnlyQuerySuite extends QueryTest with TestHiveSingleton
    with BeforeAndAfter with SQLTestUtils {

  import spark.implicits._
  import spark.toRichColumn

  override def beforeAll(): Unit = {
    super.beforeAll()
    sql("CREATE TABLE metadata_only (id bigint, data string) PARTITIONED BY (part int)")
    (0 to 10).foreach(p => sql(s"ALTER TABLE metadata_only ADD PARTITION (part=$p)"))
  }

  override protected def afterAll(): Unit = {
    try {
      sql("DROP TABLE IF EXISTS metadata_only")
    } finally {
      super.afterAll()
    }
  }

  test("SPARK-23877: validate metadata-only query pushes filters to metastore") {
    withSQLConf(OPTIMIZER_METADATA_ONLY.key -> "true") {
      val startCount = HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount

      // verify the number of matching partitions
      assert(sql("SELECT DISTINCT part FROM metadata_only WHERE part < 5").collect().length === 5)

      // verify that the partition predicate was pushed down to the metastore
      assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount - startCount === 5)
    }
  }

  test("SPARK-23877: filter on projected expression") {
    withSQLConf(OPTIMIZER_METADATA_ONLY.key -> "true") {
      val startCount = HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount

      // verify the matching partitions
      val partitions = spark.internalCreateDataFrame(Distinct(Filter(($"x" < 5).expr,
        Project(Seq(($"part" + 1).as("x").expr.asInstanceOf[NamedExpression]),
          spark.table("metadata_only").logicalPlan.asInstanceOf[SubqueryAlias].child)))
          .queryExecution.toRdd, StructType(Seq(StructField("x", IntegerType))))

      checkAnswer(partitions, Seq(1, 2, 3, 4).toDF("x"))

      // verify that the partition predicate was not pushed down to the metastore
      assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount - startCount == 11)
    }
  }
}
