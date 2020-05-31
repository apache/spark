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

import com.google.common.cache.{Cache, CacheStats}

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.{QualifiedTableName, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.execution.datasources.FindDataSourceTable
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

class DetermineTableStatsSuite extends QueryTest
  with TestHiveSingleton
  with SQLTestUtils {

  val tName = "t1"

  override def beforeAll(): Unit = {
    super.beforeAll()
    sql(s"CREATE TABLE $tName(id int, name STRING) STORED AS PARQUET".stripMargin)
  }

  override def afterAll(): Unit = {
    spark.sql(s"drop table $tName")
    super.afterAll()
  }

  test("SPARK-31850: Test if table size retrieved from cache") {


    spark.sql(s"set ${SQLConf.ENABLE_FALL_BACK_TO_HDFS_FOR_STATS.key}=true")
    spark.sql(s"set ${HiveUtils.CONVERT_METASTORE_PARQUET.key}=false")
    val catalog = spark.sessionState.catalog
    val cacheMethod = classOf[SessionCatalog].getDeclaredMethod("tableRelationCache")
    cacheMethod.setAccessible(true)
    val cache = cacheMethod.invoke(catalog)
      .asInstanceOf[Cache[QualifiedTableName, LogicalPlan]]

    def getStats: CacheStats = {
      cache.stats()
    }
    val df = catalog.lookupRelation(TableIdentifier(tName))
    catalog.invalidateAllCachedTables()
    val baseStats: CacheStats = getStats

    Analyzer.execute(df.queryExecution.logical)
    var stats = getStats
    assert(stats.missCount() == baseStats.missCount() + 1)

    Analyzer.execute(df.queryExecution.logical)
    stats = getStats
    assert(stats.missCount() == baseStats.missCount() + 1)
  }

  object Analyzer extends RuleExecutor[LogicalPlan] {
    val batches: List[Batch] = Batch("DetermineTableStatsTest", Once,
      new FindDataSourceTable(spark),
      new DetermineTableStats(spark)) :: Nil
  }
}
