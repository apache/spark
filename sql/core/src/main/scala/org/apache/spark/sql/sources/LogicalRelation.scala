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
package org.apache.spark.sql.sources

import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.AttributeMap
import org.apache.spark.sql.catalyst.plans.logical.{Statistics, LeafNode, LogicalPlan}

/**
 * Used to link a [[BaseRelation]] in to a logical query plan.
 */
private[sql] case class LogicalRelation(relation: BaseRelation)
  extends LeafNode
  with MultiInstanceRelation {

  override val output = relation.schema.toAttributes

  // Logical Relations are distinct if they have different output for the sake of transformations.
  override def equals(other: Any) = other match {
    case l @ LogicalRelation(otherRelation) => relation == otherRelation && output == l.output
    case  _ => false
  }

  override def sameResult(otherPlan: LogicalPlan) = otherPlan match {
    case LogicalRelation(otherRelation) => relation == otherRelation
    case _ => false
  }

  @transient override lazy val statistics = Statistics(
    // TODO: Allow datasources to provide statistics as well.
    sizeInBytes = BigInt(relation.sqlContext.defaultSizeInBytes)
  )

  /** Used to lookup original attribute capitalization */
  val attributeMap = AttributeMap(output.map(o => (o, o)))

  def newInstance() = LogicalRelation(relation).asInstanceOf[this.type]

  override def simpleString = s"Relation[${output.mkString(",")}] $relation"
}
