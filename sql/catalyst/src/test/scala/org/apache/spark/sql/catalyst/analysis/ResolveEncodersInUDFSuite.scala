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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Exists, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, Filter, MergeIntoTable, ReplaceData, UpdateAction}
import org.apache.spark.sql.catalyst.trees.TreePattern
import org.apache.spark.sql.connector.catalog.InMemoryRowLevelOperationTable
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class ResolveEncodersInUDFSuite extends AnalysisTest {
  test("SPARK-48921: ScalaUDF encoders in subquery should be resolved for MergeInto") {
    val table = new InMemoryRowLevelOperationTable("table",
      StructType(StructField("a", IntegerType) ::
        StructField("b", DoubleType) ::
        StructField("c", StringType) :: Nil),
      Array.empty,
      new java.util.HashMap[String, String]()
    )
    val relation = DataSourceV2Relation(table,
      Seq(AttributeReference("a", IntegerType)(),
        AttributeReference("b", DoubleType)(),
        AttributeReference("c", StringType)()),
      None,
      None,
      CaseInsensitiveStringMap.empty()
    )


    val string = relation.output(2)
    val udf = ScalaUDF((_: String) => "x", StringType, string :: Nil,
      Option(ExpressionEncoder[String]()) :: Nil)

    val mergeIntoSource =
      relation
        .where($"c" === udf)
        .select($"a", $"b")
        .limit(1)
    val cond = mergeIntoSource.output(0) == relation.output(0) &&
      mergeIntoSource.output(1) == relation.output(1)

    val mergePlan = MergeIntoTable(
      relation,
      mergeIntoSource,
      cond,
      Seq(UpdateAction(None,
        Seq(Assignment(relation.output(0), relation.output(0)),
          Assignment(relation.output(1), relation.output(1)),
          Assignment(relation.output(2), relation.output(2))))),
      Seq.empty,
      Seq.empty)

    val replaceData = mergePlan.analyze.asInstanceOf[ReplaceData]

    val existsPlans = replaceData.groupFilterCondition.map(_.collect {
      case e: Exists =>
        e.plan.collect {
          case f: Filter if f.containsPattern(TreePattern.SCALA_UDF) => f
        }
    }.flatten)

    assert(existsPlans.isDefined)

    val udfs = existsPlans.get.map(_.expressions.flatMap(e => e.collect {
      case s: ScalaUDF =>
        assert(s.inputEncoders.nonEmpty)
        val encoder = s.inputEncoders.head
        assert(encoder.isDefined)
        assert(encoder.get.objDeserializer.resolved)

        s
    })).flatten
    assert(udfs.size == 1)
  }
}
