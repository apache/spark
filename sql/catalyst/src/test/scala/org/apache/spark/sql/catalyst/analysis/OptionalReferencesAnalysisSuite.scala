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

package org.apache.spark.sql.catalyst.analysis

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  AttributeReference,
  InSubquery,
  IsNull,
  ListQuery,
  Literal
}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{
  Assignment,
  DeleteFromTable,
  Filter,
  LeafNode,
  LocalRelation,
  LogicalPlan,
  MergeIntoTable,
  UpdateTable
}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.types.IntegerType

class OptionalReferencesAnalysisSuite extends SparkFunSuite {

  /**
   * A relation that counts how often a search for optional references visits it: the search for
   * unresolved references through `innerChildren`, and the search of the DML check for the NULLs
   * of analyzed operations through the lookup of the mark of such an analysis.
   */
  private case class CountingRelation(output: Seq[Attribute]) extends LeafNode {
    var visits = 0
    var markLookups = 0
    override def innerChildren: Seq[QueryPlan[_]] = {
      visits += 1
      Nil
    }
    override def getTagValue[T](tag: TreeNodeTag[T]): Option[T] = {
      if (tag == OptionalReferences.RESOLVED_NULL_REFERENCE) {
        markLookups += 1
      }
      super.getTagValue(tag)
    }
  }

  /**
   * Returns `depth` levels of IN subqueries, each a Filter over a [[CountingRelation]] whose
   * condition is an IN subquery of the next level, and adds the relations to `relations`.
   */
  private def nestedSubqueries(
      depth: Int,
      relations: ArrayBuffer[CountingRelation]): LogicalPlan = {
    val relation = CountingRelation(Seq(AttributeReference("a", IntegerType)()))
    relations += relation
    if (depth == 0) {
      relation
    } else {
      val subquery = ListQuery(nestedSubqueries(depth - 1, relations))
      Filter(InSubquery(relation.output, subquery), relation)
    }
  }

  test("the search for optional references visits every nested subquery plan once") {
    // A plan that is searched twice per nesting level would be searched about a million times at
    // the deepest level, in every analysis of every DELETE, UPDATE, and MERGE.
    val relations = ArrayBuffer.empty[CountingRelation]
    val plan = nestedSubqueries(depth = 20, relations)
    assert(!OptionalReferences.hasOptionalReferences(plan))
    assert(relations.map(_.visits).toSet == Set(1))

    // Each search of the DML check, in a condition and in the source of a MERGE.
    relations.clear()
    val condition = InSubquery(Seq(AttributeReference("a", IntegerType)()),
      ListQuery(nestedSubqueries(depth = 20, relations)))
    CheckOptionalReferences(DeleteFromTable(LocalRelation(), condition))
    assert(relations.map(_.visits).toSet == Set(1))
    assert(relations.map(_.markLookups).toSet == Set(1))

    relations.clear()
    CheckOptionalReferences(MergeIntoTable(
      targetTable = LocalRelation(),
      sourceTable = nestedSubqueries(depth = 20, relations),
      mergeCondition = Literal(true),
      matchedActions = Nil,
      notMatchedActions = Nil,
      notMatchedBySourceActions = Nil,
      withSchemaEvolution = false))
    assert(relations.map(_.visits).toSet == Set(1))
    assert(relations.map(_.markLookups).toSet == Set(1))
  }

  test("an UPDATE that was built without the parser rejects optional references") {
    val a = AttributeReference("a", IntegerType)()
    def missing: UnresolvedAttribute = {
      val attribute = UnresolvedAttribute("missing")
      OptionalReferences.markNamePartsOptional(attribute, Set(0))
      attribute
    }
    Seq(
      UpdateTable(LocalRelation(a), Seq(Assignment(a, missing)), None),
      UpdateTable(LocalRelation(a), Seq(Assignment(a, Literal(1))), Some(IsNull(missing)))
    ).foreach { update =>
      checkError(
        exception = intercept[AnalysisException](CheckOptionalReferences(update)),
        condition = "OPTIONAL_REFERENCE_NOT_SUPPORTED.IN_DML",
        sqlState = "0A000",
        parameters = Map("context" -> "UPDATE"))
    }
  }
}
