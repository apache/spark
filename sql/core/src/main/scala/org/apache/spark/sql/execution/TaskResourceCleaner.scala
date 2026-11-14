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

package org.apache.spark.sql.execution

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.execution.exchange.Exchange

/**
 * Releases the resources that a physical operator holds in a task, without referencing the
 * operator. See [[SparkPlan.taskResourceCleaner]].
 *
 * The cleaner is per-task mutable state: it is created on the driver, before the task binary is
 * serialized, and every task deserializes its own copy. Everything in a task that creates or
 * releases the resources must reach them through that same copy, so it must capture the cleaner
 * itself rather than the operator: a deserialized operator does not carry its cleaner.
 */
private[sql] trait TaskResourceCleaner extends Serializable {
  def cleanupResources(): Unit
}

private[sql] object TaskResourceCleaner {

  /**
   * The cleaners of the operators that run in the same task as `plan`: those in its subtree, but
   * not beyond an [[Exchange]] (or a leaf, such as a query stage). An operator that releases the
   * resources of its children early in a task, such as a sort merge join, releases these.
   */
  def inStage(plan: SparkPlan): Array[TaskResourceCleaner] = {
    val cleaners = mutable.ArrayBuffer[TaskResourceCleaner]()
    def visit(p: SparkPlan): Unit = p match {
      case _: Exchange =>
      case other =>
        cleaners ++= other.taskResourceCleaner
        other.children.foreach(visit)
    }
    plan.children.foreach(visit)
    cleaners.toArray
  }

  /** Releases the resources of `cleaners`. */
  def cleanupAll(cleaners: Array[TaskResourceCleaner]): Unit = {
    cleaners.foreach(_.cleanupResources())
  }

  /**
   * Generates the code that releases the resources of the operators below `plan` in its stage
   * (see [[inStage]]), for an operator that releases them early. The generated code references
   * the cleaners, not the plans, so that the plans are not serialized into every task.
   */
  def genCleanupInStage(ctx: CodegenContext, plan: SparkPlan): String = {
    val cleaners = inStage(plan)
    if (cleaners.isEmpty) {
      ""
    } else {
      val ref = ctx.addReferenceObj("taskResourceCleaners", cleaners)
      val i = ctx.freshName("i")
      s"""
         |for (int $i = 0; $i < $ref.length; $i++) {
         |  $ref[$i].cleanupResources();
         |}
       """.stripMargin
    }
  }
}
