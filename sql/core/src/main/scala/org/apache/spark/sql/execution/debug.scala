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

private[sql] object DebugQuery {
  def apply(plan: SparkPlan): SparkPlan = {
    val visited = new collection.mutable.HashSet[Long]()
    plan transform {
      case s: SparkPlan if !visited.contains(s.id) =>
        visited += s.id
        DebugNode(s)
    }
  }
}

private[sql] case class DebugNode(child: SparkPlan) extends UnaryNode {
  def references = Set.empty
  def output = child.output
  def execute() = {
    val childRdd = child.execute()
    println(
      s"""
        |=========================
        |${child.simpleString}
        |=========================
      """.stripMargin)
    childRdd.foreach(println(_))
    childRdd
  }
}
