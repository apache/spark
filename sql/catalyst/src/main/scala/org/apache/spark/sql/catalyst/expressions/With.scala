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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.trees.TreePattern.{COMMON_EXPR_REF, TreePattern, WITH_EXPRESSION}
import org.apache.spark.sql.types.DataType

/**
 * The value of one common expression on the row being evaluated, computed the first time a
 * [[CommonExpressionRef]] reads it and reused by every later reference.
 *
 * The cell holds no expression of its own: the reference passes the definition in, so the cell stays
 * a pair of mutable slots and serializes with the plan. [[With]] clears it each time it is
 * evaluated, which is once per row for the row the enclosing expression is evaluating, so a
 * reference is never read against a value left over from an earlier row -- and on a row that does
 * not reach the branch holding the `With`, nothing is cleared because nothing is read.
 */
class CommonExpressionCell extends Serializable {
  @transient private var computed: Boolean = false
  @transient private var cached: Any = _

  private[expressions] def clear(): Unit = {
    computed = false
    cached = null
  }

  private[expressions] def get(definition: Expression, input: InternalRow): Any = {
    if (!computed) {
      cached = definition.eval(input)
      computed = true
    }
    cached
  }
}

/**
 * An expression holder that keeps a list of common expressions and allow the actual expression to
 * reference these common expressions. The common expressions are guaranteed to be evaluated only
 * once even if it's referenced more than once. This is similar to CTE but is expression-level.
 */
case class With(child: Expression, defs: Seq[CommonExpressionDef])
  extends Expression {
  // We do not allow creating a With expression with an AggregateExpression that contains a
  // reference to a common expression defined in that scope (note that it can contain another With
  // expression with a common expression ref of the inner With). This is to prevent the creation of
  // a dangling CommonExpressionRef after rewriting it in RewriteWithExpression.
  assert(!With.childContainsUnsupportedAggExpr(this))

  override val nodePatterns: Seq[TreePattern] = Seq(WITH_EXPRESSION)
  override def dataType: DataType = child.dataType
  override def nullable: Boolean = child.nullable
  override def children: Seq[Expression] = child +: defs

  /**
   * Points every reference in `child` at the definition it names and the cell that definition owns.
   * Done once per tree: a rule that rewrites this `With` produces a new one, which wires its own.
   */
  @transient private lazy val wireRefs: Unit = {
    val idToDef = defs.map(d => d.id -> d).toMap
    child.foreach {
      case ref: CommonExpressionRef => idToDef.get(ref.id).foreach(ref.bindTo)
      case _ =>
    }
  }

  /**
   * Clears each definition's cell for this row, then evaluates the child. A reference reached by
   * that evaluation computes its definition once and every later reference reads the value back, so
   * a definition is evaluated where the child would have evaluated it -- once -- rather than once
   * per reference. See [[CommonExpressionCell]].
   */
  override def eval(input: InternalRow): Any = {
    wireRefs
    defs.foreach(_.cell.clear())
    child.eval(input)
  }

  // The cells are cleared per row, so this holds state for the duration of one evaluation.
  override def stateful: Boolean = true

  /**
   * Clears each definition's flag for this row, then generates the child. A reference generated
   * inside that child fills its slots the first time it is reached and reads them back afterwards.
   * The flags are cleared in the same block the child is generated into, so a reference cannot run
   * against a flag left set by an earlier row: on a row that does not reach the branch holding this
   * `With`, neither the clearing nor any reference runs.
   */
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    ctx.withCommonExprs(defs) { slots =>
      val clearFlags = slots.map(s => s"${s.computed} = false;").mkString("\n")
      val childGen = child.genCode(ctx)
      ev.copy(
        code = code"""
           |$clearFlags
           |${childGen.code}
         """.stripMargin,
        isNull = childGen.isNull,
        value = childGen.value)
    }
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    val newDefs = newChildren.tail.map(_.asInstanceOf[CommonExpressionDef])
    // If any `CommonExpressionDef` has been updated (data type or nullability), also update its
    // `CommonExpressionRef` in the `child`.
    val newChild = newDefs.filter(_.resolved).foldLeft(newChildren.head) { (result, newDef) =>
      defs.find(_.id == newDef.id).map { oldDef =>
        if (newDef.dataType != oldDef.dataType || newDef.nullable != oldDef.nullable) {
          val newRef = new CommonExpressionRef(newDef)
          result.transform {
            case oldRef: CommonExpressionRef if oldRef.id == newRef.id =>
              newRef
          }
        } else {
          result
        }
      }.getOrElse(result)
    }
    copy(child = newChild, defs = newDefs)
  }

  /**
   * Builds a map of ids (originally assigned ids -> canonicalized ids) to be re-assigned during
   * canonicalization.
   */
  private lazy val canonicalizationIdMap: Map[Long, Long] = {
    // Start numbering after taking into account all nested With expression id maps.
    var currentId = child.map {
      case w: With => w.canonicalizationIdMap.size
      case _ => 0L
    }.sum
    defs.map { d =>
      currentId += 1
      d.id.id -> currentId
    }.toMap
  }

  /**
   * Canonicalize by re-assigning all ids in CommonExpressionRef's and CommonExpressionDef's
   * starting from 0. This uses [[canonicalizationIdMap]], which contains all mappings for
   * CommonExpressionDef's defined in this scope.
   * Note that this takes into account nested With expressions by sharing a numbering scope (see
   * [[canonicalizationIdMap]].
   */
  override lazy val canonicalized: Expression = copy(
    child = child.transformWithPruning(_.containsPattern(COMMON_EXPR_REF)) {
      case r: CommonExpressionRef if !r.id.canonicalized =>
        r.copy(id = r.id.canonicalize(canonicalizationIdMap))
    }.canonicalized,
    defs = defs.map {
      case d: CommonExpressionDef if !d.id.canonicalized =>
        d.copy(id = d.id.canonicalize(canonicalizationIdMap)).canonicalized
          .asInstanceOf[CommonExpressionDef]
      case d => d.canonicalized.asInstanceOf[CommonExpressionDef]
    }
  )
}

object With {
  /**
   * Helper function to create a [[With]] statement with an arbitrary number of common expressions.
   * Note that the number of arguments in `commonExprs` should be the same as the number of
   * arguments taken by `replaced`.
   *
   * @param commonExprs list of common expressions
   * @param replaced    closure that defines the common expressions in the main expression
   * @return the expression returned by replaced with its arguments replaced by commonExprs in order
   */
  def apply(commonExprs: Expression*)(replaced: Seq[Expression] => Expression): With = {
    val commonExprDefs = commonExprs.map(CommonExpressionDef(_))
    val commonExprRefs = commonExprDefs.map(new CommonExpressionRef(_))
    With(replaced(commonExprRefs), commonExprDefs)
  }

  private[sql] def childContainsUnsupportedAggExpr(withExpr: With): Boolean = {
    lazy val commonExprIds = withExpr.defs.map(_.id).toSet
    withExpr.child.exists {
      case agg: AggregateExpression =>
        // Check that the aggregate expression does not contain a reference to a common expression
        // in the outer With expression (it is ok if it contains a reference to a common expression
        // for a nested With expression).
        agg.exists {
          case r: CommonExpressionRef => commonExprIds.contains(r.id)
          case _ => false
        }
      case _ => false
    }
  }
}

case class CommonExpressionId(id: Long = CommonExpressionId.newId, canonicalized: Boolean = false) {
  /**
   * Re-assign to a canonicalized id based on idMap. If it is not found in idMap, the id is defined
   * in an outer scope and will be replaced later.
   */
  def canonicalize(idMap: Map[Long, Long]): CommonExpressionId = {
    if (idMap.contains(id)) {
      copy(id = idMap(id), canonicalized = true)
    } else {
      this
    }
  }
}

object CommonExpressionId {
  private[sql] val curId = new java.util.concurrent.atomic.AtomicLong()
  def newId: Long = curId.getAndIncrement()
}

/**
 * A wrapper of common expression to carry the id.
 *
 * The `cell` holds the value on the row being evaluated. It is outside the case class parameters, so
 * that a definition still compares and canonicalizes by its id and child, and a fresh one is created
 * by `copy` -- the enclosing [[With]] wires each reference to the cell of the definition that stands
 * in the tree with it, so a cell that a transform left behind is never read.
 */
case class CommonExpressionDef(child: Expression, id: CommonExpressionId = new CommonExpressionId())
  extends UnaryExpression with Unevaluable {
  private[expressions] val cell: CommonExpressionCell = new CommonExpressionCell

  override def dataType: DataType = child.dataType
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

/**
 * A reference to the common expression by its id. Only resolved common expressions can be
 * referenced, so that we can determine the data type and nullable of the reference node.
 */
case class CommonExpressionRef(id: CommonExpressionId, dataType: DataType, nullable: Boolean)
  extends LeafExpression {
  def this(exprDef: CommonExpressionDef) = this(exprDef.id, exprDef.dataType, exprDef.nullable)

  /**
   * The definition this reference names, and the cell holding its value for the current row. Both
   * are wired by the enclosing [[With]] before it evaluates its child, and are left out of the case
   * class parameters so that equality and canonicalization are unchanged -- and so that a rule
   * comparing two references does not compare their cells.
   */
  private var definition: Expression = _
  private var cell: CommonExpressionCell = _

  private[expressions] def bindTo(exprDef: CommonExpressionDef): Unit = {
    definition = exprDef.child
    cell = exprDef.cell
  }

  override val nodePatterns: Seq[TreePattern] = Seq(COMMON_EXPR_REF)

  // The cell is cleared once per row by the enclosing `With`, so this reads mutable state.
  override def stateful: Boolean = true

  override def eval(input: InternalRow): Any = {
    if (cell == null) {
      throw SparkException.internalError(
        s"Cannot evaluate a common expression reference outside its With: $this")
    }
    cell.get(definition, input)
  }

  /**
   * Computes the definition into the shared slots if this row has not done so yet, then reads them.
   * The definition's code is generated here rather than by the enclosing `With`, so it runs where
   * the first reference is reached -- behind a short-circuiting operator or a nested conditional if
   * that is where the reference sits. A second reference generates the code again but never runs it,
   * since the flag is set.
   */
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val slots = ctx.getCommonExpr(id.id)
    val defGen = slots.definition.genCode(ctx)
    val assignIsNull = if (nullable) s"${slots.value.isNull} = ${defGen.isNull};" else ""
    ev.copy(
      code = code"""
         |if (!${slots.computed}) {
         |  ${defGen.code}
         |  $assignIsNull
         |  ${slots.value.value} = ${defGen.value};
         |  ${slots.computed} = true;
         |}
       """.stripMargin,
      isNull = slots.value.isNull,
      value = slots.value.value)
  }
}
