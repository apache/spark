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
import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.connector.read.V1Scan
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation
import org.apache.spark.sql.sources.{BaseRelation, Filter, TableScan}
import org.apache.spark.sql.types.StructType

case class JDBCScan(
    relation: JDBCRelation,
    prunedSchema: StructType,
    pushedFilters: Array[Filter],
    pushedAggregateColumn: Array[String] = Array(),
    groupByColumns: Option[Array[FieldReference]]) extends V1Scan {

  override def readSchema(): StructType = prunedSchema

  override def toV1TableScan[T <: BaseRelation with TableScan](context: SQLContext): T = {
    new BaseRelation with TableScan {
      override def sqlContext: SQLContext = context
      override def schema: StructType = prunedSchema
      override def needConversion: Boolean = relation.needConversion
      override def buildScan(): RDD[Row] = {
        if (groupByColumns.isEmpty) {
          relation.buildScan(
            prunedSchema.map(_.name).toArray, Some(prunedSchema), pushedFilters, groupByColumns)
        } else {
          relation.buildScan(
            pushedAggregateColumn, Some(prunedSchema), pushedFilters, groupByColumns)
        }
      }
    }.asInstanceOf[T]
  }

  override def description(): String = {
    val (aggString, groupByString) = if (groupByColumns.nonEmpty) {
      val groupByColumnsLength = groupByColumns.get.length
      (seqToString(pushedAggregateColumn.drop(groupByColumnsLength)),
        seqToString(pushedAggregateColumn.take(groupByColumnsLength)))
    } else {
      ("[]", "[]")
    }
    super.description()  + ", prunedSchema: " + seqToString(prunedSchema) +
      ", PushedFilters: " + seqToString(pushedFilters) +
      ", PushedAggregates: " + aggString + ", PushedGroupBy: " + groupByString
  }

  private def seqToString(seq: Seq[Any]): String = seq.mkString("[", ", ", "]")
}
