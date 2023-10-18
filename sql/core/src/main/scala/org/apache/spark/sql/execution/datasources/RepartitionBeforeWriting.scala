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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, RepartitionByExpression, RepartitionOperation}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

/**
 * This rule add a shuffle before writing data into partitioned table or bucket table when
 * AUTO_REPARTITION_FOR_WRITING_ENABLED is true, which can avoid many small files.
 * 1. For bucket tables, repartition by bucket columns.
 * 2. For partition tables, repartition by partition columns.
 */
object RepartitionBeforeWriting extends Rule[LogicalPlan] {

  def buildRepartition(
      attributes: Seq[Attribute],
      query: LogicalPlan,
      numPartitions: Option[Int] = None): LogicalPlan = {
    query.collectFirst { case r: RepartitionOperation => r } match {
        case Some(RepartitionByExpression(attrs, _, _, _))
          if AttributeSet(attrs).subsetOf(AttributeSet(attributes)) => query

        case _ => new RepartitionByExpression(attributes, query, numPartitions, None)
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.AUTO_REPARTITION_BEFORE_WRITING_ENABLED)) {
      return plan
    }

    plan match {
      case i @ InsertIntoHadoopFsRelationCommand(_, _, _, _, Some(bucket), _, _, query, _, _, _, _)
        if query.resolved =>
        val resolver = conf.resolver
        val bucketColumns =
          bucket.bucketColumnNames.map { col => query.resolve(Seq(col), resolver).get.toAttribute }
        i.copy(query = buildRepartition(bucketColumns, query, Some(bucket.numBuckets)))

      case i @ InsertIntoHadoopFsRelationCommand(_, sp, _, pc, None, _, _, query, _, _, _, _)
        if query.resolved =>
        val dynamicPartitionColumns = pc.filterNot(attr => sp.contains(attr.name))
        i.copy(query = buildRepartition(dynamicPartitionColumns, query))

      case _ => plan
    }
  }
}
