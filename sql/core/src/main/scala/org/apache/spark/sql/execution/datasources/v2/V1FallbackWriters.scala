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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import org.apache.spark.sql.sources.{Filter, InsertableRelation}

/**
 * Physical plan node for append into a v2 table using V1 write interfaces.
 *
 * Rows in the output data set are appended.
 */
case class AppendDataExecV1(
    v1Relation: InsertableRelation,
    plan: LogicalPlan) extends LeafExecNode with SupportsV1Write {

  override def output: Seq[Attribute] = Nil

  override protected def doExecute(): RDD[InternalRow] = {
    writeWithV1(v1Relation)
  }
}

/**
 * Physical plan node for overwrite into a v2 table with V1 write interfaces. Note that when this
 * interface is used, the atomicity of the operation depends solely on the target data source.
 *
 * Overwrites data in a table matched by a set of filters. Rows matching all of the filters will be
 * deleted and rows in the output data set are appended.
 *
 * This plan is used to implement SaveMode.Overwrite. The behavior of SaveMode.Overwrite is to
 * truncate the table -- delete all rows -- and append the output data set. This uses the filter
 * AlwaysTrue to delete all rows.
 */
case class OverwriteByExpressionExecV1(
    v1Relation: InsertableRelation,
    deleteWhere: Seq[Filter],
    plan: LogicalPlan) extends LeafExecNode with SupportsV1Write {

  override def output: Seq[Attribute] = Nil

  override protected def doExecute(): RDD[InternalRow] = {
    writeWithV1(v1Relation)
  }
}

/**
 * A trait that allows Tables that use V1 Writer interfaces to append data.
 */
trait SupportsV1Write extends SparkPlan {
  // TODO: We should be able to work on SparkPlans at this point.
  def plan: LogicalPlan

  protected def writeWithV1(relation: InsertableRelation): RDD[InternalRow] = {
    relation.insert(Dataset.ofRows(sqlContext.sparkSession, plan), overwrite = false)
    sparkContext.emptyRDD
  }
}
