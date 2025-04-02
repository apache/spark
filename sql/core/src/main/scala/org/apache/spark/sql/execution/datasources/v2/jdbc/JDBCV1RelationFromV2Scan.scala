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
package org.apache.spark.sql.execution.datasources.v2.jdbc

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation
import org.apache.spark.sql.execution.datasources.v2.TableSampleInfo
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType

/**
 * Relation that is compatible with V1 TableScan,
 * but it uses JDBCRelation's buildScan which accepts all v2 pushdowns
 */
case class JDBCV1RelationFromV2Scan(
    context: SQLContext,
    prunedSchema: StructType,
    relation: JDBCRelation,
    pushedPredicates: Array[Predicate],
    pushedAggregateColumn: Array[String] = Array(),
    groupByColumns: Option[Array[String]],
    tableSample: Option[TableSampleInfo],
    pushedLimit: Int,
    sortOrders: Array[String],
    pushedOffset: Int) extends BaseRelation with TableScan {
  override def sqlContext: SQLContext = context
  override def schema: StructType = prunedSchema
  override def needConversion: Boolean = relation.needConversion
  override def buildScan(): RDD[Row] = {
    val columnList = if (groupByColumns.isEmpty) {
      prunedSchema.map(_.name).toArray
    } else {
      pushedAggregateColumn
    }

    relation.buildScan(columnList, prunedSchema, pushedPredicates, groupByColumns, tableSample,
      pushedLimit, sortOrders, pushedOffset)
  }

  override def toString: String = "JDBC v1 Relation from v2 scan"
}
