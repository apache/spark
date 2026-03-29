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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.SchemaPruningTest
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class PruneNestedFieldsThroughGenerateForScanSuite extends SchemaPruningTest {

  private val itemStruct = StructType(Seq(
    StructField("a", IntegerType),
    StructField("b", StringType),
    StructField("c", DoubleType)))

  private val rel = LocalRelation(
    $"id".int,
    $"items".array(itemStruct))

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Column Pruning", FixedPoint(10),
        ColumnPruning,
        CollapseProject,
        RemoveNoopOperators) ::
      Batch("PruneNestedFieldsThroughGenerate", FixedPoint(1),
        PruneNestedFieldsThroughGenerateForScan) :: Nil
  }

  private def explodeItems(outer: Boolean = false): Generate = {
    val explode = Explode($"items")
    Generate(
      explode,
      unrequiredChildIndex = Nil,
      outer = outer,
      qualifier = None,
      generatorOutput = Seq(AttributeReference("item", itemStruct)()),
      child = rel)
  }

  private def posexplodeItems(outer: Boolean = false): Generate = {
    val posexplode = PosExplode($"items")
    Generate(
      posexplode,
      unrequiredChildIndex = Nil,
      outer = outer,
      qualifier = None,
      generatorOutput = Seq(
        AttributeReference("pos", IntegerType)(),
        AttributeReference("item", itemStruct)()),
      child = rel)
  }

  test("multi-field: prunes to required fields only") {
    withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "true") {
      val gen = explodeItems()
      val item = gen.generatorOutput.head
      val first = GetStructField(item, 0, Some("a"))
      val second = GetStructField(item, 1, Some("b"))
      val query = gen.select(first, second).analyze

      val optimized = Optimize.execute(query)

      // The optimized plan should have a Generate with a pruned array type
      val generates = optimized.collect { case g: Generate => g }
      assert(generates.nonEmpty, "Expected a Generate node in the optimized plan")

      val newGen = generates.head
      val newChildType = newGen.generator.children.head.dataType
      newChildType match {
        case ArrayType(st: StructType, _) =>
          assert(st.fieldNames.toSet === Set("a", "b"),
            s"Expected pruned struct with fields {a, b} but got ${st.fieldNames.mkString(", ")}")
        case other =>
          fail(s"Expected ArrayType(StructType) but got $other")
      }
    }
  }

  test("multi-field: ordinals are correct after pruning non-contiguous fields") {
    withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "true") {
      val gen = explodeItems()
      val item = gen.generatorOutput.head
      // Select field 'a' (ordinal 0) and 'c' (ordinal 2), skipping 'b' (ordinal 1)
      val first = GetStructField(item, 0, Some("a"))
      val third = GetStructField(item, 2, Some("c"))
      val query = gen.select(first, third).analyze

      val optimized = Optimize.execute(query)

      // Check that the element struct has only a and c
      val generates = optimized.collect { case g: Generate => g }
      assert(generates.nonEmpty)
      val newGen = generates.head
      newGen.generator.children.head.dataType match {
        case ArrayType(st: StructType, _) =>
          assert(st.fieldNames.toSeq === Seq("a", "c"),
            "Fields should be in original schema order")
          assert(st.fields(0).dataType === IntegerType)
          assert(st.fields(1).dataType === DoubleType)
        case other =>
          fail(s"Expected ArrayType(StructType) but got $other")
      }

      // Check that GetStructField ordinals in the project are correct
      val projects = optimized.collect { case p: Project => p.projectList }
      assert(projects.nonEmpty)
      val topProject = projects.head
      val structFields = topProject.flatMap(_.collect {
        case gsf: GetStructField => gsf
      })
      val ordinals = structFields.map(_.ordinal)
      assert(ordinals === Seq(0, 1),
        s"Expected ordinals [0, 1] for pruned struct {a, c} but got $ordinals")
    }
  }

  test("no pruning when whole struct is referenced") {
    withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "true") {
      val gen = explodeItems()
      val item = gen.generatorOutput.head
      // Reference the whole struct directly
      val query = gen.select(item).analyze

      val optimized = Optimize.execute(query)

      // The generator child should still be the original items column
      val generates = optimized.collect { case g: Generate => g }
      assert(generates.nonEmpty)
      generates.head.generator.children.head.dataType match {
        case ArrayType(st: StructType, _) =>
          assert(st.fields.length === 3,
            "No pruning should occur when whole struct is referenced")
        case _ =>
      }
    }
  }

  test("no pruning when all fields are selected") {
    withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "true") {
      val gen = explodeItems()
      val item = gen.generatorOutput.head
      val fieldA = GetStructField(item, 0, Some("a"))
      val fieldB = GetStructField(item, 1, Some("b"))
      val fieldC = GetStructField(item, 2, Some("c"))
      val query = gen.select(fieldA, fieldB, fieldC).analyze

      val optimized = Optimize.execute(query)

      val generates = optimized.collect { case g: Generate => g }
      assert(generates.nonEmpty)
      generates.head.generator.children.head.dataType match {
        case ArrayType(st: StructType, _) =>
          assert(st.fields.length === 3,
            "No pruning should occur when all fields are selected")
        case _ =>
      }
    }
  }

  test("disabled when nestedSchemaPruningEnabled is false") {
    withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "false") {
      val gen = explodeItems()
      val item = gen.generatorOutput.head
      val first = GetStructField(item, 0, Some("a"))
      val second = GetStructField(item, 1, Some("b"))
      val query = gen.select(first, second).analyze

      val optimized = Optimize.execute(query)

      val generates = optimized.collect { case g: Generate => g }
      assert(generates.nonEmpty)
      generates.head.generator.children.head.dataType match {
        case ArrayType(st: StructType, _) =>
          assert(st.fields.length === 3,
            "No pruning when nestedSchemaPruningEnabled is false")
        case _ =>
      }
    }
  }

  test("posexplode: multi-field prune on element fields") {
    withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "true") {
      val gen = posexplodeItems()
      val pos = gen.generatorOutput(0)
      val item = gen.generatorOutput(1)
      val fieldA = GetStructField(item, 0, Some("a"))
      val fieldB = GetStructField(item, 1, Some("b"))
      val query = gen.select(pos, fieldA, fieldB).analyze

      val optimized = Optimize.execute(query)

      val generates = optimized.collect { case g: Generate => g }
      assert(generates.nonEmpty)
      val newGen = generates.head
      newGen.generator.children.head.dataType match {
        case ArrayType(st: StructType, _) =>
          assert(st.fieldNames.toSet === Set("a", "b"),
            s"Expected pruned struct {a, b} but got ${st.fieldNames.mkString(", ")}")
        case other =>
          fail(s"Expected ArrayType(StructType) but got $other")
      }
    }
  }

  test("posexplode: pos-only selects minimal-weight field") {
    withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "true") {
      val gen = posexplodeItems()
      val pos = gen.generatorOutput(0)
      // Only reference pos, not any element fields
      val query = gen.select(pos).analyze

      val optimized = Optimize.execute(query)

      val generates = optimized.collect { case g: Generate => g }
      assert(generates.nonEmpty)
      val newGen = generates.head
      newGen.generator.children.head.dataType match {
        case ArrayType(st: StructType, _) =>
          // Should pick the minimal-weight field: 'a' (IntegerType, defaultSize=4)
          // over 'b' (StringType, defaultSize=20) and 'c' (DoubleType, defaultSize=8)
          assert(st.fields.length === 1,
            s"Expected 1 field but got ${st.fields.length}")
          assert(st.fieldNames.toSet === Set("a"),
            s"Expected minimal-weight field 'a' but got '${st.fieldNames.head}'")
        case other =>
          fail(s"Expected ArrayType(StructType) but got $other")
      }
    }
  }

  // Test case for bug: when posexplode element fields are selected WITHOUT the position column,
  // pruning should still work correctly. This test verifies the fix for the "consecutive
  // OUTER POSEXPLODE without pos columns" issue.
  test("posexplode: element fields only (no pos column) still prunes correctly") {
    withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "true") {
      val gen = posexplodeItems()
      val item = gen.generatorOutput(1)
      val fieldA = GetStructField(item, 0, Some("a"))
      val fieldB = GetStructField(item, 1, Some("b"))
      // Only element fields, NOT the position column
      val query = gen.select(fieldA, fieldB).analyze

      val optimized = Optimize.execute(query)

      val generates = optimized.collect { case g: Generate => g }
      assert(generates.nonEmpty)
      val newGen = generates.head
      newGen.generator.children.head.dataType match {
        case ArrayType(st: StructType, _) =>
          // Should prune to just a, b - c should be removed
          assert(st.fieldNames.toSet === Set("a", "b"),
            s"Expected pruned struct {a, b} but got ${st.fieldNames.mkString(", ")}")
        case other =>
          fail(s"Expected ArrayType(StructType) but got $other")
      }
    }
  }

  // Nested data for consecutive posexplode tests
  // Structure: outer_array -> array<struct<b, b_int, b_string, b_array>>
  //            b_array    -> array<struct<c, c_int, c_2>>
  private val innerStruct = StructType(Seq(
    StructField("c", StringType),
    StructField("c_int", LongType),
    StructField("c_2", StringType)))

  private val outerStruct = StructType(Seq(
    StructField("b", StringType),
    StructField("b_int", LongType),
    StructField("b_string", StringType),
    StructField("b_array", ArrayType(innerStruct))))

  private val nestedRel = LocalRelation(
    $"a".string,
    $"a_int".long,
    $"a_array".array(outerStruct))

  // Test consecutive posexplodes with position columns selected (should work)
  test("consecutive posexplode: with pos columns selected") {
    withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "true") {
      // Create outer posexplode
      val outerPosExplode = PosExplode($"a_array")
      val outerGen = Generate(
        outerPosExplode,
        unrequiredChildIndex = Nil,
        outer = true,
        qualifier = None,
        generatorOutput = Seq(
          AttributeReference("a_idx", IntegerType)(),
          AttributeReference("a_array_item", outerStruct)()),
        child = nestedRel)

      val outerPos = outerGen.generatorOutput(0)
      val outerItem = outerGen.generatorOutput(1)
      val outerFieldB = GetStructField(outerItem, 0, Some("b"))
      val outerFieldBArray = GetStructField(outerItem, 3, Some("b_array"))

      // Create inner posexplode on a_array_item.b_array
      val innerPosExplode = PosExplode(outerFieldBArray)
      val innerGen = Generate(
        innerPosExplode,
        unrequiredChildIndex = Nil,
        outer = true,
        qualifier = None,
        generatorOutput = Seq(
          AttributeReference("b_idx", IntegerType)(),
          AttributeReference("b_array_item", innerStruct)()),
        child = outerGen)

      val innerPos = innerGen.generatorOutput(0)
      val innerItem = innerGen.generatorOutput(1)
      val innerFieldC = GetStructField(innerItem, 0, Some("c"))

      // Select WITH position columns: a_idx, a_array_item.b, b_idx, b_array_item.c
      val query = innerGen.select(outerPos, outerFieldB, innerPos, innerFieldC).analyze

      val optimized = Optimize.execute(query)

      // Check outer struct is pruned correctly (only b and b_array)
      val generates = optimized.collect { case g: Generate => g }
      // Find the outer generate (the one with a_array as source)
      val outerGenerates = generates.filter { g =>
        g.generator.children.head.dataType match {
          case ArrayType(ArrayType(_, _), _) => false // This would be inner with nested array
          case ArrayType(st: StructType, _) =>
            // Check if this is outer struct (has b_array field)
            st.fieldNames.exists(_.contains("b"))
          case _ => false
        }
      }

      if (outerGenerates.nonEmpty) {
        val outerGenResult = outerGenerates.head
        outerGenResult.generator.children.head.dataType match {
          case ArrayType(st: StructType, _) =>
            // Outer struct should have b and b_array (b_int and b_string pruned)
            assert(st.fieldNames.toSet === Set("b", "b_array"),
              s"Expected pruned outer struct {b, b_array} but got " +
                s"${st.fieldNames.mkString(", ")}")
          case other =>
            fail(s"Expected ArrayType(StructType) for outer but got $other")
        }
      }
    }
  }

  // Test consecutive posexplodes WITHOUT position columns (this is the bug case)
  test("consecutive posexplode: without pos columns should still prune outer fields") {
    withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "true") {
      // Create outer posexplode
      val outerPosExplode = PosExplode($"a_array")
      val outerGen = Generate(
        outerPosExplode,
        unrequiredChildIndex = Nil,
        outer = true,
        qualifier = None,
        generatorOutput = Seq(
          AttributeReference("a_idx", IntegerType)(),
          AttributeReference("a_array_item", outerStruct)()),
        child = nestedRel)

      val outerItem = outerGen.generatorOutput(1)
      val outerFieldB = GetStructField(outerItem, 0, Some("b"))
      val outerFieldBArray = GetStructField(outerItem, 3, Some("b_array"))

      // Create inner posexplode on a_array_item.b_array
      val innerPosExplode = PosExplode(outerFieldBArray)
      val innerGen = Generate(
        innerPosExplode,
        unrequiredChildIndex = Nil,
        outer = true,
        qualifier = None,
        generatorOutput = Seq(
          AttributeReference("b_idx", IntegerType)(),
          AttributeReference("b_array_item", innerStruct)()),
        child = outerGen)

      val innerItem = innerGen.generatorOutput(1)
      val innerFieldC = GetStructField(innerItem, 0, Some("c"))

      // Select WITHOUT position columns: a_array_item.b, b_array_item.c
      // This is the bug case - outer field "b" may be missing from scan
      val query = innerGen.select(outerFieldB, innerFieldC).analyze

      val optimized = Optimize.execute(query)

      // Check outer struct is pruned correctly (should have b and b_array)
      val generates = optimized.collect { case g: Generate => g }
      val outerGenerates = generates.filter { g =>
        g.generator.children.head.dataType match {
          case ArrayType(ArrayType(_, _), _) => false
          case ArrayType(st: StructType, _) =>
            st.fieldNames.exists(_.contains("b"))
          case _ => false
        }
      }

      if (outerGenerates.nonEmpty) {
        val outerGenResult = outerGenerates.head
        outerGenResult.generator.children.head.dataType match {
          case ArrayType(st: StructType, _) =>
            // BUG: Without pos columns, outer struct may only have b_array, missing "b"
            // EXPECTED: {b, b_array}
            assert(st.fieldNames.contains("b"),
              s"BUG: Outer struct missing 'b' field! Got: ${st.fieldNames.mkString(", ")}. " +
                "This happens when consecutive posexplode doesn't select position columns.")
            assert(st.fieldNames.contains("b_array"),
              s"Expected b_array in outer struct, got: ${st.fieldNames.mkString(", ")}")
          case other =>
            fail(s"Expected ArrayType(StructType) for outer but got $other")
        }
      }
    }
  }
}
