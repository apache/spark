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

package org.apache.spark.sql.catalyst.plans.logical.statsEstimation

import org.apache.spark.SparkException
import org.apache.spark.annotation.Unstable
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * An interface for estimating statistics of logical plan nodes.
 *
 * Implementations may be shared across sessions and must be thread-safe. Session-specific
 * configuration is available through `plan.conf`.
 */
@Unstable
trait LogicalPlanStatsEstimator {
  def estimate(plan: LogicalPlan): Statistics
}

/** The built-in logical plan statistics estimator. */
class DefaultLogicalPlanStatsEstimator extends LogicalPlanStatsEstimator {
  override def estimate(plan: LogicalPlan): Statistics = {
    if (plan.conf.cboEnabled) BasicStatsPlanVisitor.visit(plan)
    else SizeInBytesOnlyStatsPlanVisitor.visit(plan)
  }
}

private[sql] object LogicalPlanStatsEstimator {
  private val instances = new ClassValue[LogicalPlanStatsEstimator] {
    override def computeValue(estimatorClass: Class[_]): LogicalPlanStatsEstimator = {
      if (!classOf[LogicalPlanStatsEstimator].isAssignableFrom(estimatorClass)) {
        throw new SparkException(
          s"Statistics estimator ${estimatorClass.getName} does not implement " +
            s"${classOf[LogicalPlanStatsEstimator].getName}.")
      }
      estimatorClass.getConstructor().newInstance().asInstanceOf[LogicalPlanStatsEstimator]
    }
  }

  def get(conf: SQLConf): LogicalPlanStatsEstimator = {
    instances.get(Utils.classForName(conf.statsEstimatorClass))
  }
}
