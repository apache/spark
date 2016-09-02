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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, NonSQLPlan, Statistics}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.util.Utils

/**
 * Used to link a [[BaseRelation]] in to a logical query plan.
 *
 * Note that sometimes we need to use `LogicalRelation` to replace an existing leaf node without
 * changing the output attributes' IDs.  The `expectedOutputAttributes` parameter is used for
 * this purpose.  See https://issues.apache.org/jira/browse/SPARK-10741 for more details.
 */
case class LogicalRelation(
    relation: BaseRelation,
    expectedOutputAttributes: Option[Seq[Attribute]] = None,
    metastoreTableIdentifier: Option[TableIdentifier] = None)
  extends LeafNode with MultiInstanceRelation {

  override val output: Seq[AttributeReference] = {
    val attrs = relation.schema.toAttributes
    expectedOutputAttributes.map { expectedAttrs =>
      assert(expectedAttrs.length == attrs.length)
      attrs.zip(expectedAttrs).map {
        // We should respect the attribute names provided by base relation and only use the
        // exprId in `expectedOutputAttributes`.
        // The reason is that, some relations(like parquet) will reconcile attribute names to
        // workaround case insensitivity issue.
        case (attr, expected) => attr.withExprId(expected.exprId)
      }
    }.getOrElse(attrs)
  }

  // Logical Relations are distinct if they have different output for the sake of transformations.
  override def equals(other: Any): Boolean = other match {
    case l @ LogicalRelation(otherRelation, _, _) => relation == otherRelation && output == l.output
    case _ => false
  }

  override def hashCode: Int = {
    com.google.common.base.Objects.hashCode(relation, output)
  }

  override def sameResult(otherPlan: LogicalPlan): Boolean = {
    otherPlan.canonicalized match {
      case LogicalRelation(otherRelation, _, _) => relation == otherRelation
      case _ => false
    }
  }

  // When comparing two LogicalRelations from within LogicalPlan.sameResult, we only need
  // LogicalRelation.cleanArgs to return Seq(relation), since expectedOutputAttribute's
  // expId can be different but the relation is still the same.
  override lazy val cleanArgs: Seq[Any] = Seq(relation)

  @transient override lazy val statistics: Statistics = Statistics(
    sizeInBytes = BigInt(relation.sizeInBytes)
  )

  /** Used to lookup original attribute capitalization */
  val attributeMap: AttributeMap[AttributeReference] = AttributeMap(output.map(o => (o, o)))

  /**
   * Returns a new instance of this LogicalRelation. According to the semantics of
   * MultiInstanceRelation, this method returns a copy of this object with
   * unique expression ids. We respect the `expectedOutputAttributes` and create
   * new instances of attributes in it.
   */
  override def newInstance(): this.type = {
    LogicalRelation(
      relation,
      expectedOutputAttributes.map(_.map(_.newInstance())),
      metastoreTableIdentifier).asInstanceOf[this.type]
  }

  override def refresh(): Unit = relation match {
    case fs: HadoopFsRelation => fs.refresh()
    case _ =>  // Do nothing.
  }

  override def simpleString: String = s"Relation[${Utils.truncatedString(output, ",")}] $relation"

  override def sql: String = metastoreTableIdentifier.get.identifier
}
