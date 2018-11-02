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

package org.apache.spark.sql.test

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.sql.{execution, Strategy}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.CastSupport
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.RowDataSourceScanExec
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.FakeRelation

/**
 * A Strategy for planning fake Relation.
 */
case class FakeDataSourceStrategy(conf: SQLConf) extends Strategy with Logging with CastSupport {

  def apply(plan: LogicalPlan): Seq[execution.SparkPlan] = plan match {
    case l @ LogicalRelation(relation: FakeRelation, _, _, _) =>
      RowDataSourceScanExec(
        l.output,
        l.output.indices,
        Set.empty,
        Set.empty,
        new EmptyRDD[InternalRow](relation.sqlContext.sparkContext),
        relation,
        None) :: Nil

    case _ => Nil
  }
}
