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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, BoundReference, Cast,
  CreateNamedStruct, Divide, DropField, Expression, GetStructField, Literal, UnsafeProjection,
  UpdateFields,
  With, WithField}
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.planning.PhysicalAggregation
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.util.{quoteIdentifier, toPrettySQL}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class ReplaceUpdateFieldsExpressionSuite extends PlanTest {
  private object Optimize extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] =
      Batch("ReplaceUpdateFieldsExpression", Once,
        ReplaceUpdateFieldsExpression) :: Nil
  }

  private def nestedUpdates(struct: Expression, depth: Int): Expression = {
    (0 until depth).foldLeft(struct) { (current, index) =>
      UpdateFields(current, s"nested.field$index", Literal(index))
    }
  }

  private def expressionNodeCount(nullable: Boolean, depth: Int): Int = {
    val nested = if (nullable) {
      $"nested".struct($"value".int)
    } else {
      $"nested".struct($"value".int).notNull
    }
    val struct = if (nullable) {
      $"struct".struct(nested, $"a".int, $"b".int, $"c".int)
    } else {
      $"struct".struct(nested, $"a".int, $"b".int, $"c".int).notNull
    }
    val plan = LocalRelation(struct)
      .select(Alias(nestedUpdates(struct, depth), "result")())
    val optimized = Optimize.execute(plan)
    assert(optimized.exists(_.expressions.exists(_.exists(_.isInstanceOf[With]))))
    assert(!optimized.exists(_.expressions.exists(_.exists(_.isInstanceOf[UpdateFields]))))
    optimized.collect {
      case node => node.expressions.map(_.collect { case expression => expression }.size).sum
    }.sum
  }

  test("UpdateFields replacement grows linearly with nested update depth") {
    Seq(true, false).foreach { nullable =>
      val depthThree = expressionNodeCount(nullable, 3)
      val depthSix = expressionNodeCount(nullable, 6)
      assert(depthSix < depthThree * 3,
        s"nullable=$nullable: depth 3 has $depthThree nodes, depth 6 has $depthSix")
    }
  }

  test("UpdateFields replacement prunes named struct inputs in one pass") {
    val source = CreateNamedStruct(Seq(
      Literal("nested"), CreateNamedStruct(Seq(Literal("a"), Literal(1))),
      Literal("discarded"), Literal(100)))
    val updated = UpdateFields(UpdateFields(source, "nested.b", Literal(2)), "discarded")
    val plan = LocalRelation().select(Alias(updated, "result")())
    val replaced = ReplaceUpdateFieldsExpression(OptimizeUpdateFields(plan))
    val expressions = replaced.flatMap(_.expressions)
    assert(!expressions.exists(_.exists(_.isInstanceOf[UpdateFields])))
    assert(!expressions.exists(_.exists(_.isInstanceOf[With])))
    assert(!expressions.exists(_.exists(_ == Literal(100))))
    comparePlans(ReplaceUpdateFieldsExpression(replaced), replaced)
  }

  test("UpdateFields replacement removes unused sources and nested definitions") {
    val innerType = StructType(Seq(StructField("a", StringType)))
    val outerType = StructType(Seq(StructField("nested", innerType, nullable = false)))
    Seq(false, true).foreach { nested =>
      val structType = if (nested) outerType else innerType
      val source = Cast(AttributeReference("source", structType, nullable = false)(), structType)
      val update = UpdateFields(source, if (nested) "nested.a" else "a", Literal(7))
      val plan = LocalRelation().select(Alias(update, "result")())
      val replaced = ReplaceUpdateFieldsExpression(plan)
      val expressions = replaced.flatMap(_.expressions)
      assert(!expressions.exists(_.exists(_ == source)))
      assert(!expressions.exists(_.exists(_.isInstanceOf[With])))
      assert(!expressions.exists(_.exists(_.isInstanceOf[UpdateFields])))
      val value = CreateNamedStruct(Seq(Literal("a"), Literal(7)))
      val expected = if (nested) {
        CreateNamedStruct(Seq(Literal("nested"), value))
      } else {
        value
      }
      comparePlans(replaced, LocalRelation().select(Alias(expected, "result")()))
    }
  }

  test("UpdateFields replacement removes unused aggregates behind casts") {
    val x = $"x".int.notNull
    val z = $"z".int.notNull
    val source = Cast(CreateNamedStruct(Seq(Literal("a"),
      Sum(Divide(x, z)).toAggregateExpression())),
      StructType(Seq(StructField("a", StringType))))
    val nestedSource = CreateNamedStruct(Seq(Literal("nested"), source))
    Seq(source -> "a", nestedSource -> "nested.a").foreach { case (input, path) =>
      val updated = UpdateFields(input, path, Literal(7))
      val plan = Aggregate(Nil, Seq(Alias(updated, "result")()), LocalRelation(x, z))
      val PhysicalAggregation(_, aggregates, _, _) = ReplaceUpdateFieldsExpression(plan)
      assert(aggregates.isEmpty)
    }
  }

  test("UpdateFields replacement preserves null inputs when every field is replaced") {
    val structType = StructType(Seq(StructField("a", IntegerType)))
    val source = BoundReference(0, structType, nullable = true)
    val expression = UpdateFields(source, Seq(WithField("a", Literal(7)))).evalExpr
    assert(expression.nullable)
    assert(expression.exists(_ == source))
    assert(expression.eval(InternalRow(null)) == null)
    assert(expression.eval(InternalRow(InternalRow(1))) == InternalRow(7))
    val projection = UnsafeProjection.create(Seq(expression))
    assert(projection(InternalRow(null)).isNullAt(0))
    assert(projection(InternalRow(InternalRow(1))).getStruct(0, 1).getInt(0) == 7)
  }

  test("nested UpdateFields SQL matches expanded operations") {
    Seq("nested", "a.b", "a`b").foreach { name =>
      val nestedType = StructType(Seq(StructField("a", IntegerType)))
      val structType = StructType(Seq(StructField(name, nestedType)))
      val resolved = AttributeReference("s", structType)()
      Seq(resolved, UnresolvedAttribute("s")).foreach { struct =>
        val nested = if (struct.resolved) {
          GetStructField(struct, 0, Some(name))
        } else {
          UnresolvedExtractValue(struct, Literal(name))
        }
        val expanded = UpdateFields(struct, Seq(WithField(name,
          UpdateFields(nested, Seq(WithField("a", Literal(2)))))))
        val compact = UpdateFields(struct, s"${quoteIdentifier(name)}.a", Literal(2))
        assert(compact.sql == expanded.sql)
        if (struct.resolved) {
          assert(toPrettySQL(compact) == toPrettySQL(expanded))
        }
      }
    }
  }

  test("UpdateFields replacement does not introduce scopes for non-nested operations") {
    val structType = StructType(Seq(
      StructField("a", IntegerType), StructField("b", IntegerType)))
    Seq(true, false).foreach { nullable =>
      val struct = AttributeReference("struct", structType, nullable)()
      Seq(
        Seq(WithField("a", Literal(1))),
        Seq(DropField("b")),
        Seq(WithField("a", Literal(1)), DropField("b"))).foreach { operations =>
        val optimized = Optimize.execute(LocalRelation(struct).select(
          Alias(UpdateFields(struct, operations), "result")()))
        assert(!optimized.exists(_.expressions.exists(_.exists(_.isInstanceOf[With]))))
        assert(!optimized.exists(_.expressions.exists(_.exists(_.isInstanceOf[UpdateFields]))))
        assert(optimized.output.head.nullable == nullable)
      }
    }
  }

  test("UpdateFields codegen checks only nullable copied fields") {
    Seq(true, false).foreach { structNullable =>
      Seq(true, false).foreach { fieldNullable =>
        val structType = StructType(Seq(
          StructField("a", IntegerType, nullable = false),
          StructField("b", IntegerType, nullable = fieldNullable)))
        val struct = BoundReference(0, structType, structNullable)
        val expression = UpdateFields(struct, Seq(WithField("a", Literal(7)))).evalExpr
        val code = expression.genCode(new CodegenContext).code.toString
        val checksCopiedField = code.linesIterator.exists { line =>
          line.contains("sourceRow") && line.contains(".isNullAt(")
        }
        assert(checksCopiedField == fieldNullable)
        val projection = UnsafeProjection.create(Seq(expression))
        val result = projection(InternalRow(InternalRow(1, 2))).getStruct(0, 2)
        assert(result.getInt(0) == 7)
        assert(result.getInt(1) == 2)
        if (fieldNullable) {
          assert(projection(InternalRow(InternalRow(1, null))).getStruct(0, 2).isNullAt(1))
        }
        if (structNullable) {
          assert(projection(InternalRow(null)).isNullAt(0))
        }
      }
    }
  }

  test("UpdateFields codegen does not load ordinal arrays for contiguous copies") {
    Seq(5, 100, 500).foreach { width =>
      val structType = StructType((0 until width).map { index =>
        StructField(s"field$index", IntegerType, nullable = false)
      })
      val struct = BoundReference(0, structType, nullable = false)
      Seq(false, true).foreach { dropField =>
        val operations = Seq(WithField("field0", Literal(7))) ++
          (if (dropField) Seq(DropField("field1")) else Nil)
        val expression = UpdateFields(struct, operations).evalExpr
        val context = new CodegenContext
        expression.genCode(context)
        assert(!context.references.exists(_.isInstanceOf[Array[Int]]))
        val outputWidth = if (dropField) width - 1 else width
        val result = UnsafeProjection.create(Seq(expression))(
          InternalRow(InternalRow.fromSeq(0 until width))).getStruct(0, outputWidth)
        assert(result.getInt(0) == 7)
        (1 until outputWidth).foreach { index =>
          assert(result.getInt(index) == index + (if (dropField) 1 else 0))
        }
      }
    }
  }

  test("UpdateFields replacement does not grow with struct width when reused") {
    def nodeCount(width: Int, updateCount: Int): Int = {
      val structType = StructType((0 until width).map { index =>
        StructField(s"field$index", IntegerType)
      })
      val struct = AttributeReference("struct", structType)()
      val updated = UpdateFields(struct, (0 until updateCount).map { index =>
        WithField(s"field$index", Literal(index))
      })
      val optimized = Optimize.execute(LocalRelation(struct).select(
        Alias(updated, "first")(), Alias(updated, "second")()))
      optimized.expressions.map(_.collect { case expression => expression }.size).sum
    }

    Seq(1, 5).foreach { updateCount =>
      val widthTen = nodeCount(10, updateCount)
      val widthHundred = nodeCount(100, updateCount)
      assert(widthHundred < widthTen * 2,
        s"$updateCount updates: width 10 has $widthTen nodes, width 100 has $widthHundred")
    }
  }

  test("wide UpdateFields replacements preserve rewritten field types and values") {
    for {
      width <- Seq(100, 500)
      nullable <- Seq(false, true)
    } {
      val structType = StructType((0 until width).map { index =>
        StructField(s"field$index", IntegerType, nullable = false)
      })
      val struct = BoundReference(0, structType, nullable)
      val operations = (0 until width by 3).map { index =>
        WithField(s"field$index", Literal(index))
      }.toList
      val expression = UpdateFields(struct, operations).evalExpr
      val rewritten = expression.withNewChildren(
        expression.children.head +: expression.children.tail.map(_ => Literal("updated")))
      val expectedType = StructType((0 until width).map { index =>
        StructField(s"field$index",
          if (index % 3 == 0) StringType else IntegerType, nullable = false)
      })
      assert(rewritten.dataType == expectedType)
      assert(rewritten.nullable == nullable)
      val input = InternalRow(InternalRow.fromSeq(0 until width))
      val interpreted = rewritten.eval(input).asInstanceOf[InternalRow]
      val projection = UnsafeProjection.create(Seq(rewritten))
      val generated = projection(input).getStruct(0, width)
      Seq(interpreted, generated).foreach { result =>
        (0 until width).foreach { index =>
          if (index % 3 == 0) {
            assert(result.getUTF8String(index).toString == "updated")
          } else {
            assert(result.getInt(index) == index)
          }
        }
      }
      if (nullable) {
        assert(rewritten.eval(InternalRow(null)) == null)
        assert(projection(InternalRow(null)).isNullAt(0))
      }
    }
  }
}
