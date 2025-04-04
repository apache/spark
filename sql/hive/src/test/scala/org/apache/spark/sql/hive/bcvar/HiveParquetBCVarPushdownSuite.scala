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
package org.apache.spark.sql.hive.bcvar

import java.util

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.BufferedRows
import org.apache.spark.sql.execution.datasources.parquet.ParquetTest
import org.apache.spark.sql.execution.joins.{BaseBroadcastVarPushDownTests, BroadcastVarPushdownUtils}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.types.StructType

trait HiveRelationCreator {
  this : BroadcastVarPushdownUtils =>

  import scala.jdk.CollectionConverters._

  override def createNonPartitionedRelation(
      tablename: String,
      schema: StructType,
      data: BufferedRows): LogicalPlan = {
    val rows = data.rows.map[Row](i => new GenericRow(i.toSeq(schema).toArray))
    val df = spark.createDataFrame(new util.LinkedList[Row](rows.asJavaCollection), schema)
    df.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable(tablename)
    spark.table(tablename).queryExecution.analyzed
  }

  override def createPartitionedRelation(
      tablename: String,
      schema: StructType,
      datas: IndexedSeq[BufferedRows],
      partitionColName: String): LogicalPlan = {
    throw new UnsupportedOperationException("not supported")
  }
}

class HiveParquetBCVarPushdownSuite
  extends BaseBroadcastVarPushDownTests
  with ParquetTest
  with TestHiveSingleton
  with HiveRelationCreator

