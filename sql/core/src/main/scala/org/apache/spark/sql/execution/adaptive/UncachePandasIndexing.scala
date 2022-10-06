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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.python.AttachDistributedSequenceExec


// scalastyle:off println
object UncachePandasIndexing extends Rule[SparkPlan] {

  private def clean(plan: SparkPlan, doClean: Boolean): Unit = plan foreach {
    case index: AttachDistributedSequenceExec if doClean =>
      logWarning(s"clean AttachDistributedSequenceExec(${index.id})")
      println(s"clean AttachDistributedSequenceExec(${index.id})")
      index.clean()

    case stage: QueryStageExec =>
      // QueryStageExec itself is a leaf, should go through it.
      clean(stage.plan, stage.isMaterialized)

    case _ =>
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    // TODO: add a config to enable it
    println(s"before clean")
    println(plan)
    clean(plan, false)
    println(s"after clean")
    println(plan)
    plan
  }
}
