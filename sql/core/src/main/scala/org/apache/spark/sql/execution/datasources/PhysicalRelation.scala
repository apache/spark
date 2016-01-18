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
import org.apache.spark.sql.{AnalysisException, SQLContext}
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
    bucketingRequired: Boolean = true) extends LeafNode {

  def toPhysicalRDD: PhysicalRDD = {
    val rdd = scanBuilder(bucketingRequired)
    if (bucketingRequired) {
      PhysicalRDD.createFromDataSource(output, rdd, relation, meta, outputPartitioning)
    } else {
      PhysicalRDD.createFromDataSource(output, rdd, relation, meta)
    }
  }

  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalArgumentException("We should never execute PhysicalRelation.")

  override def outputPartitioning: Partitioning = {
    val bucketSpec = relation match {
      case r: HadoopFsRelation if bucketingRequired => r.getBucketSpec
      case _ => None
    }

    def toAttribute(colName: String): Attribute = output.find(_.name == colName).getOrElse {
      throw new AnalysisException(s"bucket column $colName not found in existing columns " +
        s"(${output.map(_.name).mkString(", ")})")
    }

    bucketSpec.map { spec =>
      val numBuckets = spec.numBuckets
      val bucketColumns = spec.bucketColumnNames.map(toAttribute)
      HashPartitioning(bucketColumns, numBuckets)
    }.getOrElse {
      super.outputPartitioning
    }
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
    val bucketingDisabled = plan transform {
      case r: PhysicalRelation if !stop =>
        stop = true
        r.copy(bucketingRequired = false)
    }

    if (!stop) {
      // There is no more PhysicalRelation to replace, return the original plan.
      plan
    } else {
      val bucketingRequired = numShuffle(addShuffle(bucketingDisabled)) > numShuffle(plan)
      bucketingDisabled transform {
        case r: PhysicalRelation if !r.bucketingRequired =>
          r.copy(bucketingRequired = bucketingRequired).toPhysicalRDD
      }
    }
  }

  private def numShuffle(plan: SparkPlan): Int = {
    plan.collect { case _: Exchange => 0 }.length
  }

  private val addShuffle = EnsureRequirements(sqlContext)
}
