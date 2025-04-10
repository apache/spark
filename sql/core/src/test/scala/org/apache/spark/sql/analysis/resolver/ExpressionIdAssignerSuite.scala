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

package org.apache.spark.sql.analysis.resolver

import java.util.IdentityHashMap

import scala.collection.mutable.{ArrayBuffer, HashMap}

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.resolver.{ExpressionIdAssigner, Resolver}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class ExpressionIdAssignerSuite extends QueryTest with SharedSparkSession {
  private val col1Integer = AttributeReference(name = "col1", dataType = IntegerType)()
  private val col1IntegerAlias = Alias(col1Integer, "a")()
  private val col2Integer = AttributeReference(name = "col2", dataType = IntegerType)()
  private val col2IntegerAlias = Alias(col2Integer, "b")()
  private val col3Integer = AttributeReference(name = "col3", dataType = IntegerType)()
  private val col4Integer = AttributeReference(name = "col4", dataType = IntegerType)()
  private val col5Integer = AttributeReference(name = "col5", dataType = IntegerType)()

  private val CONSTRAINTS_VALIDATED = TreeNodeTag[Boolean]("constraints_validated")

  test("Mapping is not created") {
    val assigner = new ExpressionIdAssigner

    intercept[SparkException] {
      assigner.mapExpression(col1Integer)
    }

    assigner.withNewMapping() {
      assigner.withNewMapping() {
        intercept[SparkException] {
          assigner.mapExpression(col1Integer)
        }
      }
    }
  }

  test("Mapping is created twice") {
    val assigner = new ExpressionIdAssigner

    intercept[SparkException] {
      assigner.createMappingForLeafOperator(newOperator = LocalRelation())
      assigner.createMappingForLeafOperator(newOperator = LocalRelation())
    }

    assigner.withNewMapping() {
      assigner.withNewMapping() {
        assigner.createMappingForLeafOperator(newOperator = LocalRelation())

        intercept[SparkException] {
          assigner.createMappingForLeafOperator(newOperator = LocalRelation())
        }

        intercept[SparkException] {
          assigner.createMappingFromChildMappings()
        }
      }

      assigner.createMappingForLeafOperator(newOperator = LocalRelation())

      intercept[SparkException] {
        assigner.createMappingForLeafOperator(newOperator = LocalRelation())
      }

      intercept[SparkException] {
        assigner.createMappingFromChildMappings()
      }
    }
  }

  test("Create mapping with new output and old output with different length") {
    val assigner = new ExpressionIdAssigner

    intercept[SparkException] {
      val oldOperator = LocalRelation(output = Seq(col1Integer, col2Integer))

      assigner.createMappingForLeafOperator(
        newOperator = LocalRelation(output = Seq(col1Integer.newInstance())),
        oldOperator = Some(oldOperator)
      )
    }
  }

  test("Collect child mappings without creating a lower mapping first") {
    val assigner = new ExpressionIdAssigner

    assigner.withNewMapping() {}

    intercept[SparkException] {
      assigner.withNewMapping(collectChildMapping = true) {}
    }
  }

  test("Create mapping from absent chid mappings") {
    val assigner = new ExpressionIdAssigner

    assigner.withNewMapping() {
      val oldOperator = LocalRelation(output = Seq(col1Integer))

      assigner.createMappingForLeafOperator(
        newOperator = LocalRelation(output = Seq(col1Integer.newInstance())),
        oldOperator = Some(oldOperator)
      )
    }

    intercept[SparkException] {
      assigner.createMappingFromChildMappings()
    }
  }

  test("Dangling references") {
    val assigner = new ExpressionIdAssigner

    assigner.withNewMapping() {
      assigner.createMappingForLeafOperator(newOperator = LocalRelation())

      intercept[SparkException] {
        assigner.mapExpression(col1Integer)
      }
    }

    assigner.withNewMapping() {
      val oldOperator = LocalRelation(output = Seq(col1Integer))

      assigner.createMappingForLeafOperator(
        newOperator = LocalRelation(output = Seq(col1Integer.newInstance())),
        oldOperator = Some(oldOperator)
      )

      intercept[SparkException] {
        assigner.mapExpression(col2Integer)
      }
    }
  }

  test("Single AttributeReference") {
    val assigner = new ExpressionIdAssigner

    assigner.withNewMapping() {
      val oldOperator = LocalRelation(output = Seq(col1Integer))

      assert(assigner.shouldPreserveLeafOperatorIds(oldOperator))
      assigner.createMappingForLeafOperator(
        newOperator = oldOperator
      )

      val col1IntegerMapped = assigner.mapExpression(col1Integer)
      assert(col1IntegerMapped.exprId == col1Integer.exprId)

      val col1IntegerReferenced = assigner.mapExpression(col1Integer)
      assert(col1IntegerReferenced.exprId == col1IntegerMapped.exprId)

      val col1IntegerMappedReferenced = assigner.mapExpression(col1IntegerMapped)
      assert(col1IntegerMappedReferenced.exprId == col1IntegerMapped.exprId)
    }

    assigner.withNewMapping() {
      val oldOperator = LocalRelation(output = Seq(col1Integer))

      val col1IntegerNew = col1Integer.newInstance()

      assert(!assigner.shouldPreserveLeafOperatorIds(oldOperator))
      assigner.createMappingForLeafOperator(
        newOperator = LocalRelation(output = Seq(col1IntegerNew)),
        oldOperator = Some(oldOperator)
      )

      val col1IntegerMapped = assigner.mapExpression(col1Integer)
      assert(col1IntegerMapped.exprId != col1Integer.exprId)
      assert(col1IntegerMapped.exprId == col1IntegerNew.exprId)

      val col1IntegerReferenced = assigner.mapExpression(col1Integer)
      assert(col1IntegerReferenced.exprId == col1IntegerMapped.exprId)

      val col1IntegerMappedReferenced = assigner.mapExpression(col1IntegerMapped)
      assert(col1IntegerMappedReferenced.exprId == col1IntegerMapped.exprId)
    }
  }

  test("Single Alias") {
    val assigner = new ExpressionIdAssigner

    assigner.withNewMapping() {
      val oldOperator = LocalRelation(output = Seq(col1Integer))

      assert(assigner.shouldPreserveLeafOperatorIds(oldOperator))
      assigner.createMappingForLeafOperator(
        newOperator = oldOperator
      )

      val col1IntegerAliasMapped = assigner.mapExpression(col1IntegerAlias)
      assert(col1IntegerAliasMapped.exprId == col1IntegerAlias.exprId)

      val col1IntegerAliasReferenced = assigner.mapExpression(col1IntegerAlias.toAttribute)
      assert(col1IntegerAliasReferenced.exprId == col1IntegerAliasMapped.exprId)

      val col1IntegerAliasMappedReferenced =
        assigner.mapExpression(col1IntegerAliasMapped.toAttribute)
      assert(col1IntegerAliasMappedReferenced.exprId == col1IntegerAliasMapped.exprId)

      val col1IntegerAliasMappedAgain = assigner.mapExpression(col1IntegerAlias)
      assert(col1IntegerAliasMappedAgain.exprId != col1IntegerAlias.exprId)
      assert(col1IntegerAliasMappedAgain.exprId != col1IntegerAliasMapped.exprId)
    }

    assigner.withNewMapping() {
      val oldOperator = LocalRelation(output = Seq(col1Integer))

      assert(!assigner.shouldPreserveLeafOperatorIds(oldOperator))
      assigner.createMappingForLeafOperator(
        newOperator = LocalRelation(output = Seq(col1Integer.newInstance())),
        oldOperator = Some(oldOperator)
      )

      val col1IntegerAliasMapped = assigner.mapExpression(col1IntegerAlias)
      assert(col1IntegerAliasMapped.exprId != col1IntegerAlias.exprId)

      val col1IntegerAliasReferenced = assigner.mapExpression(col1IntegerAlias.toAttribute)
      assert(col1IntegerAliasReferenced.exprId == col1IntegerAliasMapped.exprId)

      val col1IntegerAliasMappedReferenced =
        assigner.mapExpression(col1IntegerAliasMapped.toAttribute)
      assert(col1IntegerAliasMappedReferenced.exprId == col1IntegerAliasMapped.exprId)

      val col1IntegerAliasMappedAgain = assigner.mapExpression(col1IntegerAlias)
      assert(col1IntegerAliasMappedAgain.exprId != col1IntegerAlias.exprId)
      assert(col1IntegerAliasMappedAgain.exprId != col1IntegerAliasMapped.exprId)
    }
  }

  test("Attributes and aliases") {
    val assigner = new ExpressionIdAssigner

    assigner.withNewMapping() {
      val oldOperator = LocalRelation(output = Seq(col1Integer, col2Integer))

      assert(assigner.shouldPreserveLeafOperatorIds(oldOperator))
      assigner.createMappingForLeafOperator(
        newOperator = oldOperator
      )
      val col1IntegerReferenced = assigner.mapExpression(col1Integer)
      assert(col1IntegerReferenced.exprId == col1Integer.exprId)

      val col2IntegerReferenced = assigner.mapExpression(col2Integer)
      assert(col2IntegerReferenced.exprId == col2Integer.exprId)

      val col2IntegerAliasMapped = assigner.mapExpression(col2IntegerAlias)
      assert(col2IntegerAliasMapped.exprId == col2IntegerAlias.exprId)
      assert(col2IntegerAliasMapped.exprId != col2Integer.exprId)

      intercept[SparkException] {
        assigner.mapExpression(col3Integer)
      }
    }

    assigner.withNewMapping() {
      val oldOperator = LocalRelation(output = Seq(col1Integer, col2Integer))

      val col1IntegerNew = col1Integer.newInstance()
      val col2IntegerNew = col2Integer.newInstance()

      assert(!assigner.shouldPreserveLeafOperatorIds(oldOperator))
      assigner.createMappingForLeafOperator(
        newOperator = LocalRelation(output = Seq(col1IntegerNew, col2IntegerNew)),
        oldOperator = Some(oldOperator)
      )

      val col1IntegerReferenced = assigner.mapExpression(col1Integer)
      assert(col1IntegerReferenced.exprId != col1Integer.exprId)
      assert(col1IntegerReferenced.exprId == col1IntegerNew.exprId)

      val col1IntegerNewReferenced = assigner.mapExpression(col1IntegerNew)
      assert(col1IntegerNewReferenced.exprId == col1IntegerNew.exprId)

      val col2IntegerReferenced = assigner.mapExpression(col2Integer)
      assert(col2IntegerReferenced.exprId != col2Integer.exprId)
      assert(col2IntegerReferenced.exprId == col2IntegerNew.exprId)

      val col2IntegerNewReferenced = assigner.mapExpression(col2IntegerNew)
      assert(col2IntegerNewReferenced.exprId == col2IntegerNew.exprId)

      val col2IntegerAliasMapped = assigner.mapExpression(col2IntegerAlias)
      assert(col2IntegerAliasMapped.exprId != col2IntegerAlias.exprId)
      assert(col2IntegerAliasMapped.exprId != col2Integer.exprId)

      intercept[SparkException] {
        assigner.mapExpression(col3Integer)
      }
    }
  }

  test("Create mapping from child mappings") {
    val assigner = new ExpressionIdAssigner

    val col1IntegerNew = col1Integer.newInstance()
    val col2IntegerNew = col2Integer.newInstance()
    val col2IntegerNew2 = col2Integer.newInstance()
    val col3IntegerNew = col3Integer.newInstance()
    val col3IntegerNew2 = col3Integer.newInstance()
    val col4IntegerNew = col4Integer.newInstance()
    val col4IntegerNew2 = col4Integer.newInstance()
    val col5IntegerNew = col5Integer.newInstance()

    assigner.withNewMapping(collectChildMapping = true) {
      val oldOperator = LocalRelation(output = Seq(col1Integer, col2Integer))

      assert(assigner.shouldPreserveLeafOperatorIds(oldOperator))
      assigner.createMappingForLeafOperator(
        newOperator = oldOperator
      )
    }

    assigner.withNewMapping(collectChildMapping = true) {
      val oldOperator = LocalRelation(output = Seq(col1Integer, col2Integer))

      assert(!assigner.shouldPreserveLeafOperatorIds(oldOperator))
      assigner.createMappingForLeafOperator(
        newOperator = LocalRelation(output = Seq(col1IntegerNew, col2IntegerNew)),
        oldOperator = Some(oldOperator)
      )
    }

    assigner.withNewMapping(collectChildMapping = true) {
      val oldOperator = LocalRelation(output = Seq(col3Integer, col4Integer))

      assert(assigner.shouldPreserveLeafOperatorIds(oldOperator))
      assigner.createMappingForLeafOperator(
        newOperator = oldOperator
      )
    }

    assigner.withNewMapping(collectChildMapping = true) {
      val oldOperator = LocalRelation(output = Seq(col3Integer, col4Integer))

      assert(!assigner.shouldPreserveLeafOperatorIds(oldOperator))
      assigner.createMappingForLeafOperator(
        newOperator = LocalRelation(output = Seq(col3IntegerNew, col4IntegerNew)),
        oldOperator = Some(oldOperator)
      )
    }

    assigner.withNewMapping(collectChildMapping = true) {
      val oldOperator = LocalRelation(output = Seq(col2Integer, col4Integer))

      assert(!assigner.shouldPreserveLeafOperatorIds(oldOperator))
      assigner.createMappingForLeafOperator(
        newOperator = LocalRelation(output = Seq(col2IntegerNew2, col4IntegerNew2)),
        oldOperator = Some(oldOperator)
      )
    }

    assigner.withNewMapping(collectChildMapping = true) {
      val oldOperator = LocalRelation(output = Seq(col3Integer, col5Integer))

      assert(!assigner.shouldPreserveLeafOperatorIds(oldOperator))
      assigner.createMappingForLeafOperator(
        newOperator = LocalRelation(output = Seq(col3IntegerNew2, col5IntegerNew)),
        oldOperator = Some(oldOperator)
      )
    }

    assigner.createMappingFromChildMappings()

    val col1IntegerMapped = assigner.mapExpression(col1Integer)
    assert(col1IntegerMapped.exprId == col1Integer.exprId)

    val col1IntegerNewMapped = assigner.mapExpression(col1IntegerNew)
    assert(col1IntegerNewMapped.exprId == col1IntegerNew.exprId)

    val col2IntegerMapped = assigner.mapExpression(col2Integer)
    assert(col2IntegerMapped.exprId == col2Integer.exprId)

    val col2IntegerNewMapped = assigner.mapExpression(col2IntegerNew)
    assert(col2IntegerNewMapped.exprId == col2IntegerNew.exprId)

    val col2IntegerNew2Mapped = assigner.mapExpression(col2IntegerNew2)
    assert(col2IntegerNew2Mapped.exprId == col2IntegerNew2.exprId)

    val col3IntegerMapped = assigner.mapExpression(col3Integer)
    assert(col3IntegerMapped.exprId == col3Integer.exprId)

    val col3IntegerNewMapped = assigner.mapExpression(col3IntegerNew)
    assert(col3IntegerNewMapped.exprId == col3IntegerNew.exprId)

    val col3IntegerNew2Mapped = assigner.mapExpression(col3IntegerNew2)
    assert(col3IntegerNew2Mapped.exprId == col3IntegerNew2.exprId)

    val col4IntegerMapped = assigner.mapExpression(col4Integer)
    assert(col4IntegerMapped.exprId == col4Integer.exprId)

    val col4IntegerNewMapped = assigner.mapExpression(col4IntegerNew)
    assert(col4IntegerNewMapped.exprId == col4IntegerNew.exprId)

    val col4IntegerNew2Mapped = assigner.mapExpression(col4IntegerNew2)
    assert(col4IntegerNew2Mapped.exprId == col4IntegerNew2.exprId)

    val col5IntegerMapped = assigner.mapExpression(col5Integer)
    assert(col5IntegerMapped.exprId == col5IntegerNew.exprId)

    val col5IntegerNewMapped = assigner.mapExpression(col5IntegerNew)
    assert(col5IntegerNewMapped.exprId == col5IntegerNew.exprId)
  }

  test("Several layers") {
    val assigner = new ExpressionIdAssigner
    val literalAlias1 = Alias(Literal(1), "a")()
    val literalAlias2 = Alias(Literal(2), "b")()

    val output1 = assigner.withNewMapping(collectChildMapping = true) {
      val output1 = assigner.withNewMapping(collectChildMapping = true) {
        val col1IntegerNew = col1Integer.newInstance()
        val col2IntegerNew = col2Integer.newInstance()

        val oldOperator = LocalRelation(output = Seq(col1IntegerNew, col2IntegerNew))

        assigner.createMappingForLeafOperator(
          newOperator = oldOperator
        )

        Seq(
          assigner.mapExpression(col1IntegerNew).toAttribute,
          assigner.mapExpression(col2IntegerNew).toAttribute
        )
      }

      val output2 = assigner.withNewMapping(collectChildMapping = true) {
        val oldOperator = LocalRelation(output = Seq(col1Integer, col2Integer))

        val col1IntegerNew = col1Integer.newInstance()
        val col2IntegerNew = col2Integer.newInstance()

        assigner.createMappingForLeafOperator(
          newOperator = LocalRelation(output = Seq(col1IntegerNew, col2IntegerNew)),
          oldOperator = Some(oldOperator)
        )

        Seq(
          assigner.mapExpression(col1Integer).toAttribute,
          assigner.mapExpression(col2Integer).toAttribute
        )
      }

      val output3 = assigner.withNewMapping() {
        assigner.createMappingForLeafOperator(newOperator = LocalRelation())

        intercept[SparkException] {
          assigner.mapExpression(col1Integer).toAttribute
        }
        intercept[SparkException] {
          assigner.mapExpression(col2Integer).toAttribute
        }
      }

      output1.zip(output2).zip(Seq(col1Integer, col2Integer)).foreach {
        case ((attribute1, attribute2), originalAttribute) =>
          assert(attribute1.exprId != originalAttribute.exprId)
          assert(attribute2.exprId != originalAttribute.exprId)
          assert(attribute1.exprId != attribute2.exprId)
      }

      assigner.createMappingFromChildMappings()

      val literalAlias1Remapped = assigner.mapExpression(literalAlias1)
      assert(literalAlias1Remapped.exprId == literalAlias1.exprId)

      val literalAlias2Remapped = assigner.mapExpression(literalAlias2)
      assert(literalAlias2Remapped.exprId == literalAlias2.exprId)

      Seq(literalAlias1Remapped.toAttribute, literalAlias2Remapped.toAttribute) ++ output2
    }

    val output2 = assigner.withNewMapping(collectChildMapping = true) {
      assigner.createMappingForLeafOperator(newOperator = LocalRelation())

      val literalAlias1Remapped = assigner.mapExpression(literalAlias1)
      assert(literalAlias1Remapped.exprId != literalAlias1.exprId)

      val literalAlias2Remapped = assigner.mapExpression(literalAlias2)
      assert(literalAlias2Remapped.exprId != literalAlias2.exprId)

      Seq(literalAlias1Remapped.toAttribute, literalAlias2Remapped.toAttribute)
    }

    output1.zip(output2).foreach {
      case (aliasReference1, aliasReference2) =>
        assert(aliasReference1.exprId != aliasReference2.exprId)
    }

    assigner.createMappingFromChildMappings()

    val aliasReferences = output1.map { aliasReference =>
      assigner.mapExpression(aliasReference)
    }

    aliasReferences.zip(output1).zip(output2).foreach {
      case ((aliasReference, aliasReference1), aliasReference2) =>
        assert(aliasReference.exprId == aliasReference1.exprId)
        assert(aliasReference.exprId != aliasReference2.exprId)
    }

    aliasReferences.map(_.toAttribute)
  }

  test("Outer reference error paths") {
    val assigner = new ExpressionIdAssigner

    val oldOperator = LocalRelation(output = Seq(col1Integer))

    val col1IntegerNew = col1Integer.newInstance()
    assert(col1IntegerNew.exprId != col1Integer.exprId)

    assigner.createMappingForLeafOperator(
      newOperator = LocalRelation(output = Seq(col1IntegerNew)),
      oldOperator = Some(oldOperator)
    )

    intercept[SparkException] {
      assigner.mapOuterReference(col1Integer)
    }
    intercept[SparkException] {
      assigner.mapOuterReference(col1IntegerNew)
    }

    assigner.withNewMapping(isSubqueryRoot = true) {
      val oldOperator = LocalRelation(output = Seq(col2Integer))

      val col2IntegerNew = col2Integer.newInstance()
      assert(col2IntegerNew.exprId != col2Integer.exprId)

      assigner.createMappingForLeafOperator(
        newOperator = LocalRelation(output = Seq(col2IntegerNew)),
        oldOperator = Some(oldOperator)
      )

      intercept[SparkException] {
        assigner.mapOuterReference(col2Integer)
      }
      intercept[SparkException] {
        assigner.mapOuterReference(col2IntegerNew)
      }
      intercept[SparkException] {
        assigner.mapOuterReference(col3Integer)
      }

      assigner.withNewMapping(isSubqueryRoot = true) {
        val oldOperator = LocalRelation(output = Seq(col3Integer))

        val col3IntegerNew = col3Integer.newInstance()
        assert(col3IntegerNew.exprId != col3Integer.exprId)

        assigner.createMappingForLeafOperator(
          newOperator = LocalRelation(output = Seq(col3IntegerNew)),
          oldOperator = Some(oldOperator)
        )

        intercept[SparkException] {
          assigner.mapOuterReference(col1Integer)
        }
        intercept[SparkException] {
          assigner.mapOuterReference(col1IntegerNew)
        }
        intercept[SparkException] {
          assigner.mapOuterReference(col3Integer)
        }
        intercept[SparkException] {
          assigner.mapOuterReference(col3IntegerNew)
        }
      }
    }
  }

  test("Simple outer reference") {
    val assigner = new ExpressionIdAssigner

    val oldOperator = LocalRelation(output = Seq(col1Integer, col2Integer))

    val col1IntegerNew = col1Integer.newInstance()
    assert(col1IntegerNew.exprId != col1Integer.exprId)

    val col2IntegerNew = col2Integer.newInstance()
    assert(col2IntegerNew.exprId != col2Integer.exprId)

    assigner.createMappingForLeafOperator(
      newOperator = LocalRelation(output = Seq(col1IntegerNew, col2IntegerNew)),
      oldOperator = Some(oldOperator)
    )

    assigner.withNewMapping(isSubqueryRoot = true) {
      val oldOperator = LocalRelation(output = Seq(col3Integer, col4Integer))

      val col3IntegerNew = col3Integer.newInstance()
      assert(col3IntegerNew.exprId != col3Integer.exprId)

      val col4IntegerNew = col4Integer.newInstance()
      assert(col4IntegerNew.exprId != col4Integer.exprId)

      assigner.createMappingForLeafOperator(
        newOperator = LocalRelation(output = Seq(col3IntegerNew, col4IntegerNew)),
        oldOperator = Some(oldOperator)
      )

      val col1IntegerRemapped = assigner.mapOuterReference(col1Integer)
      assert(col1IntegerRemapped.exprId == col1IntegerNew.exprId)

      val col2IntegerRemapped = assigner.mapOuterReference(col2Integer)
      assert(col2IntegerRemapped.exprId == col2IntegerNew.exprId)
    }
  }

  test("First CTE reference preserves its IDs") {
    val assigner = new ExpressionIdAssigner

    assigner.withNewMapping() {
      val oldOperator = LocalRelation(output = Seq(col1Integer, col2Integer))

      assert(assigner.shouldPreserveLeafOperatorIds(oldOperator))
      assigner.createMappingForLeafOperator(
        newOperator = oldOperator
      )
    }

    assigner.withNewMapping() {
      val oldOperator = CTERelationRef(
        cteId = 0,
        _resolved = true,
        output = Seq(col1Integer, col2Integer),
        isStreaming = false
      )

      assert(assigner.shouldPreserveLeafOperatorIds(oldOperator))
      assigner.createMappingForLeafOperator(
        newOperator = oldOperator
      )
    }

    assigner.withNewMapping() {
      val oldOperator = CTERelationRef(
        cteId = 0,
        _resolved = true,
        output = Seq(col1Integer, col2Integer),
        isStreaming = false
      )

      assert(!assigner.shouldPreserveLeafOperatorIds(oldOperator))
      assigner.createMappingForLeafOperator(
        newOperator = oldOperator
          .copy(output = Seq(col1Integer.newInstance(), col2Integer.newInstance())),
        oldOperator = Some(oldOperator)
      )
    }
  }

  test("Nested outer reference") {
    val assigner = new ExpressionIdAssigner

    val oldOperator = LocalRelation(output = Seq(col1Integer))

    val col1IntegerNew = col1Integer.newInstance()
    assert(col1IntegerNew.exprId != col1Integer.exprId)

    assigner.createMappingForLeafOperator(
      newOperator = LocalRelation(output = Seq(col1IntegerNew)),
      oldOperator = Some(oldOperator)
    )

    assigner.withNewMapping(isSubqueryRoot = true) {
      val oldOperator = LocalRelation(output = Seq(col2Integer))

      val col2IntegerNew = col2Integer.newInstance()
      assert(col2IntegerNew.exprId != col2Integer.exprId)

      assigner.createMappingForLeafOperator(
        newOperator = LocalRelation(output = Seq(col2IntegerNew)),
        oldOperator = Some(oldOperator)
      )

      val col1IntegerRemapped = assigner.mapOuterReference(col1Integer)
      assert(col1IntegerRemapped.exprId == col1IntegerNew.exprId)

      assigner.withNewMapping() {
        val oldOperator = LocalRelation(output = Seq(col3Integer))

        val col3IntegerNew = col3Integer.newInstance()
        assert(col3IntegerNew.exprId != col3Integer.exprId)

        assigner.createMappingForLeafOperator(
          newOperator = LocalRelation(output = Seq(col3IntegerNew)),
          oldOperator = Some(oldOperator)
        )

        val col1IntegerRemapped = assigner.mapOuterReference(col1Integer)
        assert(col1IntegerRemapped.exprId == col1IntegerNew.exprId)

        assigner.withNewMapping(isSubqueryRoot = true) {
          val oldOperator = LocalRelation(output = Seq(col4Integer))

          val col4IntegerNew = col4Integer.newInstance()
          assert(col4IntegerNew.exprId != col4Integer.exprId)

          assigner.createMappingForLeafOperator(
            newOperator = LocalRelation(output = Seq(col4IntegerNew)),
            oldOperator = Some(oldOperator)
          )

          val col3IntegerRemapped = assigner.mapOuterReference(col3Integer)
          assert(col3IntegerRemapped.exprId == col3IntegerNew.exprId)
        }
      }
    }
  }

  test("Simple select") {
    val result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
      spark
        .sql("""
        SELECT
          col1, 1 AS a, col1, 1 AS a, col2, 2 AS b, col3, 3 AS c
        FROM
          VALUES (1, 2, 3)
        """)
    }
    checkExpressionIdAssignment(result.queryExecution.analyzed)
  }

  test("Simple select, aliases referenced") {
    val result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
      spark
        .sql("""
        SELECT
          col3, c, col2, b, col1, a, col1, a
        FROM (
          SELECT
            col1, 1 AS a, col1, col2, 2 AS b, col3, 3 AS c
          FROM
            VALUES (1, 2, 3)
        )""")
    }
    checkExpressionIdAssignment(result.queryExecution.analyzed)
  }

  test("Simple select, aliases referenced and rewritten") {
    val result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
      spark
        .sql("""
        SELECT
          col3, 3 AS c, col2, 2 AS b, col1, 1 AS a, col1, 1 AS a
        FROM (
          SELECT
            col2, b, col1, a, col1, a, col3, c
          FROM (
            SELECT
              col1, 1 AS a, col1, col2, 2 AS b, col3, 3 AS c
            FROM
              VALUES (1, 2, 3)
          )
        )""")
    }
    checkExpressionIdAssignment(result.queryExecution.analyzed)
  }

  test("SQL Union, same table") {
    withTable("t1") {
      spark.sql("CREATE TABLE t1 (col1 INT, col2 INT, col3 INT)")

      val result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
        spark
          .sql("""
          SELECT * FROM (
            SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1
            UNION ALL
            SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1
            UNION ALL
            SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1
          )""")
      }
      checkExpressionIdAssignment(result.queryExecution.analyzed)
    }
  }

  test("SQL Union, different tables") {
    withTable("t1") {
      spark.sql("CREATE TABLE t1 (col1 INT, col2 INT, col3 INT)")
      withTable("t2") {
        spark.sql("CREATE TABLE t2 (col1 INT, col2 INT, col3 INT)")
        withTable("t3") {
          spark.sql("CREATE TABLE t3 (col1 INT, col2 INT, col3 INT)")

          val result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
            spark
              .sql("""
              SELECT * FROM (
                SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1
                UNION ALL
                SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t2
                UNION ALL
                SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t3
              )""")
          }
          checkExpressionIdAssignment(result.queryExecution.analyzed)
        }
      }
    }
  }

  test("SQL Union, same table, several layers") {
    withTable("t1") {
      spark.sql("CREATE TABLE t1 (col1 INT, col2 INT, col3 INT)")

      val result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
        spark
          .sql("""
          SELECT * FROM (
            SELECT * FROM (
              SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1
              UNION ALL
              SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1
            )
            UNION ALL
            SELECT * FROM (
              SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1
              UNION ALL
              SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1
            )
          )
          UNION ALL
          SELECT * FROM (
            SELECT * FROM (
              SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1
              UNION ALL
              SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1
            )
            UNION ALL
            SELECT * FROM (
              SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1
              UNION ALL
              SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1
            )
          )""")
      }
      checkExpressionIdAssignment(result.queryExecution.analyzed)
    }
  }

  test("SQL Join, same table") {
    withTable("t1") {
      spark.sql("CREATE TABLE t1 (col1 INT, col2 INT, col3 INT)")

      val result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
        spark.sql("""
          SELECT q1.col1 FROM (
            (SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1) q1
            JOIN
            (SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1) q2
            JOIN
            (SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1) q3
            ON true
          )""")
      }

      checkExpressionIdAssignment(result.queryExecution.analyzed)
    }
  }

  test("SQL Join, different tables") {
    withTable("t1") {
      spark.sql("CREATE TABLE t1 (col1 INT, col2 INT, col3 INT)")
      withTable("t2") {
        spark.sql("CREATE TABLE t2 (col1 INT, col2 INT, col3 INT)")
        withTable("t3") {
          spark.sql("CREATE TABLE t3 (col1 INT, col2 INT, col3 INT)")

          val result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
            spark.sql("""
              SELECT q1.col1, q1.a, q2.col2, q2.b, q3.col3, q3.c FROM (
                (SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1) q1
                JOIN
                (SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t2) q2
                JOIN
                (SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t3) q3
                ON true
              )""")
          }

          checkExpressionIdAssignment(result.queryExecution.analyzed)
        }
      }
    }
  }

  test("SQL Join, same table, several layers") {
    withTable("t1") {
      spark.sql("CREATE TABLE t1 (col1 INT, col2 INT, col3 INT)")

      val result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
        spark.sql("""
          SELECT q1.col1, q1.a, q1.col2, q2.b, q2.col3, q2.c FROM (
            (SELECT q1.col1, q1.a, q1.col2, q2.b, q2.col3, q2.c FROM (
              (SELECT q1.col1, q1.a, q1.col2, q2.b, q2.col3, q2.c FROM (
                (SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1) q1
                JOIN
                (SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1) q2
                ON true
              )) q1
              JOIN
              (SELECT q1.col1, q1.a, q1.col2, q2.b, q2.col3, q2.c FROM (
                (SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1) q1
                JOIN
                (SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1) q2
                ON true
              )) q2
              ON true
            )) q1
            JOIN
            (SELECT q1.col1, q1.a, q1.col2, q2.b, q2.col3, q2.c FROM (
              (SELECT q1.col1, q1.a, q1.col2, q2.b, q2.col3, q2.c FROM (
                (SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1) q1
                JOIN
                (SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1) q2
                ON true
              )) q1
              JOIN
              (SELECT q1.col1, q1.a, q1.col2, q2.b, q2.col3, q2.c FROM (
                (SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1) q1
                JOIN
                (SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1) q2
                ON true
              )) q2
              ON true
            )) q2
            ON true
          )
          """)
      }

      checkExpressionIdAssignment(result.queryExecution.analyzed)
    }
  }

  test("DataFrame Union, same table") {
    withTable("t1") {
      spark.sql("CREATE TABLE t1 (col1 INT, col2 INT, col3 INT)")

      val result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
        val df = spark.sql("SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1")
        df.union(df)
      }
      checkExpressionIdAssignment(result.queryExecution.analyzed)
    }
  }

  test("DataFrame Union, different tables") {
    withTable("t1") {
      spark.sql("CREATE TABLE t1 (col1 INT, col2 INT, col3 INT)")

      withTable("t2") {
        spark.sql("CREATE TABLE t2 (col1 INT, col2 INT, col3 INT)")

        val result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
          val df1 = spark.sql("SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1")
          val df2 = spark.sql("SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t2")
          df1.union(df2)
        }
        checkExpressionIdAssignment(result.queryExecution.analyzed)
      }
    }
  }

  test("DataFrame Union, same table, several layers") {
    withTable("t1") {
      spark.sql("CREATE TABLE t1 (col1 INT, col2 INT, col3 INT)")

      val result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
        val df1 = spark.sql("SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1")
        val df2 = df1
          .union(df1)
          .select(df1("col1"), df1("a"), df1("col2"), df1("b"), df1("col3"), df1("c"))
        val df3 = df2
          .union(df2)
          .select(df2("col1"), df2("a"), df2("col2"), df2("b"), df2("col3"), df2("c"))
        df3
          .union(df3)
          .select(df3("col1"), df3("a"), df3("col2"), df3("b"), df3("col3"), df3("c"))
      }
      checkExpressionIdAssignment(result.queryExecution.analyzed)
    }
  }

  test("DataFrame Join, same table") {
    withTable("t1") {
      spark.sql("CREATE TABLE t1 (col1 INT, col2 INT, col3 INT)")

      val result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
        val df = spark.sql("SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1")
        df.join(df, df("col1") === 0)
      }
      checkExpressionIdAssignment(result.queryExecution.analyzed)
    }
  }

  test("DataFrame Join, different tables") {
    withTable("t1") {
      spark.sql("CREATE TABLE t1 (col1 INT, col2 INT, col3 INT)")

      withTable("t2") {
        spark.sql("CREATE TABLE t2 (col1 INT, col2 INT, col3 INT)")

        val result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
          val df1 = spark.sql("SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1")
          val df2 = spark.sql("SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t2")
          df1.join(df2, df1("col1") === 0)
        }
        checkExpressionIdAssignment(result.queryExecution.analyzed)
      }
    }
  }

  test("DataFrame Join, same table, several layers") {
    withTable("t1") {
      spark.sql("CREATE TABLE t1 (col1 INT, col2 INT, col3 INT)")

      val result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
        val df1 = spark.sql("SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1")
        val df2 = df1
          .join(df1, df1("col1") === 0)
          .select(df1("col1"), df1("a"), df1("col2"), df1("b"), df1("col3"), df1("c"))
        val df3 = df2
          .join(df2, df2("col1") === 0)
          .select(df2("col1"), df2("a"), df2("col2"), df2("b"), df2("col3"), df2("c"))
        df3
          .join(df3, df3("col1") === 0)
          .select(df3("col1"), df3("a"), df3("col2"), df3("b"), df3("col3"), df3("c"))
      }
      checkExpressionIdAssignment(result.queryExecution.analyzed)
    }
  }

  test("The case of output attribute names is preserved") {
    val df = spark.sql("SELECT col1, COL1, cOl2, CoL2 FROM VALUES (1, 2)")

    checkExpressionIdAssignment(df.queryExecution.analyzed)
  }

  test("The metadata of output attributes is preserved") {
    val metadata1 = new MetadataBuilder().putString("m1", "1").putString("m2", "2").build()
    val metadata2 = new MetadataBuilder().putString("m2", "3").putString("m3", "4").build()
    val schema = new StructType().add("a", IntegerType, nullable = true, metadata = metadata2)
    val df =
      spark.sql("SELECT col1 FROM VALUES (1)").select(col("col1").as("a", metadata1)).to(schema)

    checkExpressionIdAssignment(df.queryExecution.analyzed)
  }

  test("Alias with the same ID in multiple Projects") {
    val result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
      val df = spark.sql("SELECT col1 AS a FROM VALUES (5), (6), (7), (8), (9)")
      val alias = (col("a") + 1).as("a")
      df.select(alias).select(alias).select(alias)
    }

    checkAnswer(result, Array(Row(8), Row(9), Row(10), Row(11), Row(12)))
  }

  test("Raw union, same table") {
    val t = LocalRelation.fromExternalRows(
      Seq("col1".attr.int, "col2".attr.int),
      0.until(10).map(_ => Row(1, 2))
    )
    val query = t.select("col1".attr, Literal(1).as("a"), "col2".attr, Literal(2).as("b"))
    val plan = query.union(query)

    checkExpressionIdAssignment(plan)
  }

  test("DataFrame with binary arithmetic re-resolved") {
    val result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
      val df = spark.sql("SELECT col1 + col2 AS a FROM VALUES (1, 2)")
      df.union(df)
    }
    checkAnswer(result, Array(Row(3), Row(3)))
  }

  test("DataFrame Union with correlated subqueries") {
    withTable("range1") {
      withTable("range2") {
        spark.range(0, 10).write.saveAsTable("range1")
        spark.range(5, 15).write.saveAsTable("range2")

        var result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
          val df = spark.sql("""
            SELECT * FROM range1 WHERE EXISTS (
              SELECT * FROM range2 WHERE range2.id == range1.id
            )
          """)
          df.union(df)
        }
        checkAnswer(
          result,
          Array(Row(5), Row(5), Row(6), Row(6), Row(7), Row(7), Row(8), Row(8), Row(9), Row(9))
        )
        checkExpressionIdAssignment(result.queryExecution.analyzed)

        result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
          val df = spark.sql("""
            SELECT * FROM range1 WHERE EXISTS (
              SELECT * FROM (
                SELECT * FROM (
                  SELECT range1.id AS id1, range2.id AS id2 FROM range2
                )
              )
              WHERE id1 == id2
            )
          """)
          df.union(df)
        }
        checkAnswer(
          result,
          Array(Row(5), Row(5), Row(6), Row(6), Row(7), Row(7), Row(8), Row(8), Row(9), Row(9))
        )
        checkExpressionIdAssignment(result.queryExecution.analyzed)

        result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
          val df = spark.sql("""
            SELECT * FROM range1 WHERE EXISTS (
              SELECT * FROM (
                SELECT id FROM range2 WHERE (range2.id == range1.id) AND (id % 2 == 0)
                UNION ALL
                SELECT id FROM range2 WHERE (range2.id == range1.id) AND (id % 2 != 0)
              )
            )
          """)
          df.union(df)
        }
        checkAnswer(
          result,
          Array(Row(5), Row(5), Row(6), Row(6), Row(7), Row(7), Row(8), Row(8), Row(9), Row(9))
        )
        checkExpressionIdAssignment(result.queryExecution.analyzed)

        result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
          val df = spark.sql("""
            SELECT * FROM range1 WHERE id IN (
              SELECT id FROM range2
            )
          """)
          df.union(df)
        }
        checkAnswer(
          result,
          Array(Row(5), Row(5), Row(6), Row(6), Row(7), Row(7), Row(8), Row(8), Row(9), Row(9))
        )
        checkExpressionIdAssignment(result.queryExecution.analyzed)

        result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
          val df = spark.sql("""
            SELECT col1, (
              SELECT * FROM range1 WHERE EXISTS (
                SELECT * FROM range2 WHERE range2.id == range1.id
              )
              LIMIT 1
            ) FROM VALUES (1)
          """)
          df.union(df)
        }
        checkAnswer(result, Array(Row(1, 5), Row(1, 5)))
        checkExpressionIdAssignment(result.queryExecution.analyzed)

        result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
          val df = spark.sql("""
            SELECT * FROM range1 WHERE EXISTS (
              SELECT * FROM range2 WHERE range2.id == range1.id
            ) AND EXISTS (
              SELECT * FROM range2 WHERE range2.id == range1.id
            )
          """)
          df.union(df)
        }
        checkAnswer(
          result,
          Array(Row(5), Row(5), Row(6), Row(6), Row(7), Row(7), Row(8), Row(8), Row(9), Row(9))
        )
        checkExpressionIdAssignment(result.queryExecution.analyzed)

        withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
          intercept[AnalysisException] {
            spark.sql("""
              SELECT * FROM range1 WHERE EXISTS (
                SELECT * FROM VALUES (1) WHERE EXISTS (
                  SELECT * FROM range2 WHERE range2.id == range1.id
                )
              )
            """)
          }
        }
      }
    }
  }

  test("DataFrame Union with CTEs") {
    withTable("range1") {
      spark.range(0, 3).write.saveAsTable("range1")

      val expectedResult = Array(
        Row(0),
        Row(0),
        Row(0),
        Row(0),
        Row(1),
        Row(1),
        Row(1),
        Row(1),
        Row(2),
        Row(2),
        Row(2),
        Row(2)
      )

      var result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
        val df = spark.sql("""
          WITH cte AS (SELECT * FROM range1)
          SELECT * FROM cte
          UNION ALL
          SELECT * FROM cte
        """)
        df.union(df)
      }
      checkAnswer(result, expectedResult)

      result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
        val df = spark.sql("""
          WITH cte AS (SELECT * FROM range1)
          SELECT * FROM range1
          UNION ALL
          SELECT * FROM cte
        """)
        df.union(df)
      }
      checkAnswer(result, expectedResult)

      result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
        val df = spark.sql("""
          WITH cte AS (SELECT * FROM range1)
          SELECT * FROM cte
          UNION ALL
          SELECT * FROM range1
        """)
        df.union(df)
      }
      checkAnswer(result, expectedResult)
    }
  }

  test("Leftmost branch attributes are not regenerated in DataFrame") {
    withTable("t1") {
      spark.sql("CREATE TABLE t1 (col1 INT, col2 INT)")
      spark.sql("INSERT INTO t1 VALUES (0, 1), (2, 3)")

      var result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
        val df1 = spark.table("t1")
        df1.select(col("col1"), col("col2")).filter(df1("col1") === 0)
      }
      checkAnswer(result, Array(Row(0, 1)))

      result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
        val df1 = spark.table("t1").select(col("col1").as("a"), col("col2").as("b"))
        df1.select(col("a"), col("b")).filter(df1("a") === 0)
      }
      checkAnswer(result, Array(Row(0, 1)))

      result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
        val df1 = spark.table("t1")
        df1.union(df1).filter(df1("col1") === 0)
      }
      checkAnswer(result, Array(Row(0, 1), Row(0, 1)))
    }
  }

  test("Attribute ID under ExtractValue is remapped correctly") {
    withTable("t1") {
      spark.sql("CREATE TABLE t1 (col1 STRUCT<f1: INT, f2: STRUCT<f3: STRING, f4: INT>>)")
      spark.sql(
        "INSERT INTO t1 VALUES (named_struct('f1', 0, 'f2', named_struct('f3', 'a', 'f4', 1)))"
      )

      var result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
        val df1 = spark.table("t1")
        val df2 = df1.select("col1.f1")
        df2.union(df2)
      }
      checkAnswer(result, Array(Row(0), Row(0)))

      result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
        val df1 = spark.table("t1")
        val df2 = df1.select("col1.f2.f4")
        df2.union(df2)
      }
      checkAnswer(result, Array(Row(1), Row(1)))

      result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
        spark.sql("SELECT col1.f1 FROM t1 UNION ALL SELECT col1.f1 FROM t1")
      }
      checkAnswer(result, Array(Row(0), Row(0)))

      result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
        spark.sql("SELECT col1.f2.f4 FROM t1 UNION ALL SELECT col1.f2.f4 FROM t1")
      }
      checkAnswer(result, Array(Row(1), Row(1)))
    }
  }

  private def checkExpressionIdAssignment(originalPlan: LogicalPlan): Unit = {
    val resolver = new Resolver(
      catalogManager = spark.sessionState.catalogManager,
      extensions = spark.sessionState.analyzer.singlePassResolverExtensions
    )
    val newPlan = resolver.resolve(originalPlan)

    checkPlanConstraints(originalPlan, newPlan, preserveExpressionIds = true)
    checkSubtreeConstraints(originalPlan, newPlan, preserveExpressionIds = true)
  }

  private def checkPlanConstraints(
      originalPlan: LogicalPlan,
      newPlan: LogicalPlan,
      preserveExpressionIds: Boolean): Unit = {
    val preserveExpressionIdsInChildren = originalPlan.resolved && !ExpressionIdAssigner
        .doOutputsHaveConflictingExpressionIds(originalPlan.children.map(_.output))

    originalPlan.children.zip(newPlan.children).zipWithIndex.foreach {
      case ((originalChild, newChild), index) =>
        checkPlanConstraints(
          originalChild,
          newChild,
          preserveExpressionIds && (preserveExpressionIdsInChildren || index == 0)
        )
    }

    if (originalPlan.children.length > 1) {
      ExpressionIdAssigner.assertOutputsHaveNoConflictingExpressionIds(
        newPlan.children.map(_.output)
      )
      originalPlan.children.zip(newPlan.children).zipWithIndex.foreach {
        case ((oldChild, newChild), index) =>
          checkSubtreeConstraints(
            oldChild,
            newChild,
            preserveExpressionIds && (preserveExpressionIdsInChildren || index == 0)
          )
      }
    }

    originalPlan.expressions.zip(newPlan.expressions).foreach {
      case (oldChild, newChild) =>
        val oldSubqueryExpressions = oldChild.collect {
          case subqueryExpression: SubqueryExpression => subqueryExpression
        }
        val newSubqueryExpressions = newChild.collect {
          case subqueryExpression: SubqueryExpression => subqueryExpression
        }

        oldSubqueryExpressions.zip(newSubqueryExpressions).foreach {
          case (oldSubqueryExpression, newSubqueryExpression) =>
            checkPlanConstraints(
              oldSubqueryExpression.plan,
              newSubqueryExpression.plan,
              preserveExpressionIds = preserveExpressionIds
            )
            checkSubtreeConstraints(
              oldSubqueryExpression.plan,
              newSubqueryExpression.plan,
              preserveExpressionIds = preserveExpressionIds
            )
        }
    }
  }

  private def checkSubtreeConstraints(
      originalPlan: LogicalPlan,
      newPlan: LogicalPlan,
      preserveExpressionIds: Boolean): Unit = {
    val originalOperators = new ArrayBuffer[LogicalPlan]
    originalPlan.foreach {
      case operator if !operator.getTagValue(CONSTRAINTS_VALIDATED).getOrElse(false) =>
        originalOperators.append(operator)
      case _ =>
    }

    val newOperators = new ArrayBuffer[LogicalPlan]

    val operatorsWithPreservedIds = new IdentityHashMap[LogicalPlan, Boolean]
    if (preserveExpressionIds) {
      operatorsWithPreservedIds.put(newPlan, true)
    }

    newPlan.foreach {
      case operator if !operator.getTagValue(CONSTRAINTS_VALIDATED).getOrElse(false) =>
        newOperators.append(operator)

        if (operator.children.nonEmpty && operatorsWithPreservedIds.containsKey(operator)) {
          operatorsWithPreservedIds.put(operator.children.head, true)
        }
      case _ =>
    }

    val attributesByName = new HashMap[String, ArrayBuffer[AttributeReference]]
    val aliasesByName = new HashMap[String, ArrayBuffer[Alias]]
    originalOperators
      .zip(newOperators)
      .collect {
        case (originalOperator: LogicalPlan, newOperator: LogicalPlan) =>
          if (originalOperator.resolved) {
            (
              collectAttributesAndAliases(originalOperator),
              collectAttributesAndAliases(newOperator),
              newOperator
            )
          } else {
            (collectAttributesAndAliases(newOperator), newOperator)
          }
      }
      .foreach {
        case (
            originalExpressions: Seq[_],
            newExpressions: Seq[_],
            newOperator: LogicalPlan
            ) =>
          originalExpressions.zip(newExpressions).zipWithIndex.foreach {
            case (
                (originalAttribute: AttributeReference, newAttribute: AttributeReference),
                index
                ) =>
              if (operatorsWithPreservedIds.containsKey(newOperator)) {
                assert(
                  originalAttribute.exprId == newAttribute.exprId,
                  s"Attribute at $index was regenerated: $originalAttribute, $newAttribute"
                )
              } else {
                assert(
                  originalAttribute.exprId != newAttribute.exprId,
                  s"Attribute at $index was not regenerated: $originalAttribute, $newAttribute"
                )
              }

              attributesByName
                .getOrElseUpdate(newAttribute.name, new ArrayBuffer[AttributeReference])
                .append(newAttribute)
            case ((originalAlias: Alias, newAlias: Alias), index) =>
              if (operatorsWithPreservedIds.containsKey(newOperator)) {
                assert(
                  originalAlias.exprId == newAlias.exprId,
                  s"Alias at $index was regenerated: $originalAlias, $newAlias"
                )
              } else {
                assert(
                  originalAlias.exprId != newAlias.exprId,
                  s"Alias at $index was not regenerated: $originalAlias, $newAlias"
                )
              }

              aliasesByName.getOrElseUpdate(newAlias.name, new ArrayBuffer[Alias]).append(newAlias)
          }
        case (newExpressions: Seq[_], newOperator: LogicalPlan) =>
          newExpressions.foreach {
            case newAttribute: AttributeReference =>
              attributesByName
                .getOrElseUpdate(newAttribute.name, new ArrayBuffer[AttributeReference])
                .append(newAttribute)
            case newAlias: Alias =>
              aliasesByName.getOrElseUpdate(newAlias.name, new ArrayBuffer[Alias]).append(newAlias)
          }
      }

    attributesByName.values.foreach { attributes =>
      val ids = attributes.map(attribute => attribute.exprId).distinct
      assert(
        ids.length == 1,
        s"Different IDs for the same attribute in the plan: $attributes, $newPlan"
      )
    }
    aliasesByName.values.foreach { aliases =>
      val ids = aliases.map(alias => alias.exprId).distinct
      assert(
        ids.length == aliases.length,
        s"Duplicate IDs for aliases with the same name: $aliases"
      )
    }

    for (operator <- originalOperators) {
      operator.setTagValue(CONSTRAINTS_VALIDATED, true)
    }
    for (operator <- newOperators) {
      operator.setTagValue(CONSTRAINTS_VALIDATED, true)
    }

    if (originalPlan.resolved) {
      assert(newPlan.schema == originalPlan.schema)
    }
  }

  private def collectAttributesAndAliases(plan: LogicalPlan): Seq[NamedExpression] = {
    plan.expressions.flatMap { expression =>
      expression.collect {
        case attribute: AttributeReference => attribute
        case alias: Alias => alias
      }
    }
  }
}
