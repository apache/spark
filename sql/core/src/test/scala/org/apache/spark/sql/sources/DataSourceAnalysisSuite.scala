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

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.execution.datasources.DataSourceAnalysis
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy
import org.apache.spark.sql.types.{DataType, IntegerType, StructType}

class DataSourceAnalysisSuite extends SparkFunSuite with BeforeAndAfterAll with SQLHelper {

  private var targetAttributes: Seq[Attribute] = _
  private var targetPartitionSchema: StructType = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    targetAttributes = Seq($"a".int, $"d".int, $"b".int, $"c".int)
    targetPartitionSchema = new StructType()
      .add("b", IntegerType)
      .add("c", IntegerType)
  }

  private def checkProjectList(actual: Seq[Expression], expected: Seq[Expression]): Unit = {
    // Remove aliases since we have no control on their exprId.
    val withoutAliases = actual.map {
      case alias: Alias => alias.child
      case other => other
    }
    assert(withoutAliases === expected)
  }

  Seq(true, false).foreach { caseSensitive =>
    def testRule(testName: String, caseSensitive: Boolean)(func: => Unit): Unit = {
      test(s"$testName (caseSensitive: $caseSensitive)") {
        withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
          func
        }
      }
    }

    def cast(e: Expression, dt: DataType): Expression = {
      SQLConf.get.storeAssignmentPolicy match {
        case StoreAssignmentPolicy.ANSI | StoreAssignmentPolicy.STRICT =>
          val cast = Cast(e, dt, Option(SQLConf.get.sessionLocalTimeZone), ansiEnabled = true)
          cast.setTagValue(Cast.BY_TABLE_INSERTION, ())
          cast
        case _ =>
          Cast(e, dt, Option(SQLConf.get.sessionLocalTimeZone))
      }
    }
    val rule = DataSourceAnalysis(SimpleAnalyzer)
    testRule(
      "convertStaticPartitions only handle INSERT having at least static partitions",
        caseSensitive) {
      intercept[AssertionError] {
        rule.convertStaticPartitions(
          sourceAttributes = Seq($"e".int, $"f".int),
          providedPartitions = Map("b" -> None, "c" -> None),
          targetAttributes = targetAttributes,
          targetPartitionSchema = targetPartitionSchema)
      }
    }

    testRule("Missing columns", caseSensitive) {
      // Missing columns.
      intercept[AnalysisException] {
        rule.convertStaticPartitions(
          sourceAttributes = Seq($"e".int),
          providedPartitions = Map("b" -> Some("1"), "c" -> None),
          targetAttributes = targetAttributes,
          targetPartitionSchema = targetPartitionSchema)
      }
    }

    testRule("Missing partitioning columns", caseSensitive) {
      // Missing partitioning columns.
      intercept[AnalysisException] {
        rule.convertStaticPartitions(
          sourceAttributes = Seq($"e".int, $"f".int),
          providedPartitions = Map("b" -> Some("1")),
          targetAttributes = targetAttributes,
          targetPartitionSchema = targetPartitionSchema)
      }

      // Missing partitioning columns.
      intercept[AnalysisException] {
        rule.convertStaticPartitions(
          sourceAttributes = Seq($"e".int, $"f".int, $"g".int),
          providedPartitions = Map("b" -> Some("1")),
          targetAttributes = targetAttributes,
          targetPartitionSchema = targetPartitionSchema)
      }

      // Wrong partitioning columns.
      intercept[AnalysisException] {
        rule.convertStaticPartitions(
          sourceAttributes = Seq($"e".int, $"f".int),
          providedPartitions = Map("b" -> Some("1"), "d" -> None),
          targetAttributes = targetAttributes,
          targetPartitionSchema = targetPartitionSchema)
      }
    }

    testRule("Wrong partitioning columns", caseSensitive) {
      // Wrong partitioning columns.
      intercept[AnalysisException] {
        rule.convertStaticPartitions(
          sourceAttributes = Seq($"e".int, $"f".int),
          providedPartitions = Map("b" -> Some("1"), "d" -> Some("2")),
          targetAttributes = targetAttributes,
          targetPartitionSchema = targetPartitionSchema)
      }

      // Wrong partitioning columns.
      intercept[AnalysisException] {
        rule.convertStaticPartitions(
          sourceAttributes = Seq($"e".int),
          providedPartitions = Map("b" -> Some("1"), "c" -> Some("3"), "d" -> Some("2")),
          targetAttributes = targetAttributes,
          targetPartitionSchema = targetPartitionSchema)
      }

      if (caseSensitive) {
        // Wrong partitioning columns.
        intercept[AnalysisException] {
          rule.convertStaticPartitions(
            sourceAttributes = Seq($"e".int, $"f".int),
            providedPartitions = Map("b" -> Some("1"), "C" -> Some("3")),
            targetAttributes = targetAttributes,
            targetPartitionSchema = targetPartitionSchema)
        }
      }
    }

    testRule("Static partitions need to appear before dynamic partitions", caseSensitive) {
      // Static partitions need to appear before dynamic partitions.
      intercept[AnalysisException] {
        rule.convertStaticPartitions(
          sourceAttributes = Seq($"e".int, $"f".int),
          providedPartitions = Map("b" -> None, "c" -> Some("3")),
          targetAttributes = targetAttributes,
          targetPartitionSchema = targetPartitionSchema)
      }
    }

    testRule("All static partitions", caseSensitive) {
      if (!caseSensitive) {
        val nonPartitionedAttributes = Seq($"e".int, $"f".int)
        val expected = nonPartitionedAttributes ++
          Seq(cast(Literal("1"), IntegerType), cast(Literal("3"), IntegerType))
        val actual = rule.convertStaticPartitions(
          sourceAttributes = nonPartitionedAttributes,
          providedPartitions = Map("b" -> Some("1"), "C" -> Some("3")),
          targetAttributes = targetAttributes,
          targetPartitionSchema = targetPartitionSchema)
        checkProjectList(actual, expected)
      }

      {
        val nonPartitionedAttributes = Seq($"e".int, $"f".int)
        val expected = nonPartitionedAttributes ++
          Seq(cast(Literal("1"), IntegerType), cast(Literal("3"), IntegerType))
        val actual = rule.convertStaticPartitions(
          sourceAttributes = nonPartitionedAttributes,
          providedPartitions = Map("b" -> Some("1"), "c" -> Some("3")),
          targetAttributes = targetAttributes,
          targetPartitionSchema = targetPartitionSchema)
        checkProjectList(actual, expected)
      }

      // Test the case having a single static partition column.
      {
        val nonPartitionedAttributes = Seq($"e".int, $"f".int)
        val expected = nonPartitionedAttributes ++ Seq(cast(Literal("1"), IntegerType))
        val actual = rule.convertStaticPartitions(
          sourceAttributes = nonPartitionedAttributes,
          providedPartitions = Map("b" -> Some("1")),
          targetAttributes = Seq($"a".int, $"d".int, $"b".int),
          targetPartitionSchema = new StructType().add("b", IntegerType))
        checkProjectList(actual, expected)
      }
    }

    testRule("Static partition and dynamic partition", caseSensitive) {
      val nonPartitionedAttributes = Seq($"e".int, $"f".int)
      val dynamicPartitionAttributes = Seq($"g".int)
      val expected =
        nonPartitionedAttributes ++
          Seq(cast(Literal("1"), IntegerType)) ++
          dynamicPartitionAttributes
      val actual = rule.convertStaticPartitions(
        sourceAttributes = nonPartitionedAttributes ++ dynamicPartitionAttributes,
        providedPartitions = Map("b" -> Some("1"), "c" -> None),
        targetAttributes = targetAttributes,
        targetPartitionSchema = targetPartitionSchema)
      checkProjectList(actual, expected)
    }
  }
}
