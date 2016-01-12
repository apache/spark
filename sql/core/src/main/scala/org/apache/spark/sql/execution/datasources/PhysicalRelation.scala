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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.sources.{BaseRelation, HadoopFsRelation}

/**
 * A placeholder for [[PhysicalRDD]].  It will carry the relation information and we will decide
 * to use bucketing or not based on the information later.
 */
private[sql] case class PhysicalRelation(
    output: Seq[Attribute],
    scanBuilder: Boolean => RDD[InternalRow],
    relation: BaseRelation,
    meta: Map[String, String],
    useBucketInfo: Boolean = true) extends LeafNode {

  def toPhysicalRDD: PhysicalRDD = {
    val rdd = scanBuilder(useBucketInfo)
    if (useBucketInfo) {
      PhysicalRDD.createFromDataSource(output, rdd, relation, meta, outputPartitioning)
    } else {
      PhysicalRDD.createFromDataSource(output, rdd, relation, meta)
    }
  }

  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalArgumentException("We should never execute PhysicalRelation.")

  override val outputPartitioning: Partitioning = {
    val bucketSpec = relation match {
      case r: HadoopFsRelation if r.sqlContext.conf.bucketingEnabled() && useBucketInfo =>
        r.bucketSpec
      case _ => None
    }

    def toAttribute(colName: String): Attribute = output.find(_.name == colName).get

    val bucketedOutputPartition = for {
      spec <- bucketSpec
      numBuckets = spec.numBuckets
      bucketColumns = spec.bucketColumnNames.map(toAttribute)
    } yield {
      HashPartitioning(bucketColumns, numBuckets)
    }

    bucketedOutputPartition.getOrElse(super.outputPartitioning)
  }
}

/**
 * Replaces [[PhysicalRelation]] with [[PhysicalRDD]] after decided to use bucketing information or
 * not.
 *
 * Note that this rule only replaces one `PhysicalRelation` at a time, so should be put in a batch
 * with fixed point.
 */
private[sql] case class ReplacePhysicalRelation(sqlContext: SQLContext) extends Rule[SparkPlan] {

  /**
   * The strategy is: picks up one `PhysicalRelation` and disable its bucketing. If we need extra
   * shuffle because of this, it means this bucketing information is useful and we should keep it.
   * Else, we can safely ignore the bucketing information.
   */
  def apply(plan: SparkPlan): SparkPlan = {
    var stop = false
    val bucketDisabled = plan transform {
      case r: PhysicalRelation if !stop =>
        stop = true
        r.copy(useBucketInfo = false)
    }

    if (!stop) {
      plan
    } else if (numShuffle(addShuffle(bucketDisabled)) > numShuffle(plan)) {
      bucketDisabled transform {
        case r: PhysicalRelation if !r.useBucketInfo => r.copy(useBucketInfo = true).toPhysicalRDD
      }
    } else {
      bucketDisabled transform {
        case r: PhysicalRelation if !r.useBucketInfo => r.toPhysicalRDD
      }
    }
  }

  private def numShuffle(plan: SparkPlan): Int = {
    plan.collect { case _: Exchange => 0 }.length
  }

  private val addShuffle = EnsureRequirements(sqlContext)
}
