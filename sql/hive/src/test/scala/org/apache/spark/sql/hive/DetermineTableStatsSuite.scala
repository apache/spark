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

import scala.collection.mutable

import org.mockito.Mockito._

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

class DetermineTableStatsSuite extends QueryTest
  with TestHiveSingleton
  with SQLTestUtils {

  val relationToSizeMap: mutable.Map[TableIdentifier, Long] = mutable.Map.empty
  val tName = "t1"

  override def beforeAll(): Unit = {
    super.beforeAll()
    sql(s"CREATE TABLE $tName(id int, name STRING) STORED AS PARQUET".stripMargin)
  }

  test("SPARK-31850: Test if table size retrieved from cache") {
    val flags = Seq(true, false)
    flags.foreach {
      hdfsFallbackEnabled =>
        withSQLConf((SQLConf.ENABLE_FALL_BACK_TO_HDFS_FOR_STATS.key,
          hdfsFallbackEnabled.toString)) {

          val hiveRelation = DDLUtils.readHiveTable(spark.sharedState
            .externalCatalog.getTable(SessionCatalog.DEFAULT_DATABASE, tName))
          val ident = TableIdentifier(tName)
          val rule: DetermineTableStats = mock(classOf[DetermineTableStats])

          // define the mock methods behaviour
          when(rule.session).thenReturn(spark)
          when(rule.getRelationToSizeMap).thenReturn(relationToSizeMap)
          when(rule.hiveTableWithStats(hiveRelation)).thenCallRealMethod()
          when(rule.apply(hiveRelation)).thenCallRealMethod()

          rule.apply(hiveRelation)
          assert(relationToSizeMap.nonEmpty == hdfsFallbackEnabled)
          assert(relationToSizeMap.contains(ident) == hdfsFallbackEnabled)

          // clear the map for next iteration
          relationToSizeMap.clear()
        }
    }
  }

  override def afterAll(): Unit = {
    spark.sql(s"drop table $tName")
    super.afterAll()
  }
}
