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

import scala.collection.mutable

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.trees.TreePattern.{COMMON_EXPR_REF, TreePattern, WITH_EXPRESSION}
import org.apache.spark.sql.types.DataType

/**
 * The value of one common expression, computed the first time a [[CommonExpressionRef]] reads it
 * and reused by every later reference. The reference passes the definition in, so this is a pair of
 * mutable slots and nothing else.
 *
 * [[With]] clears it on entry, which is per entry rather than per row: one `With` object can sit at
 * two positions of a tree, and `GenerateOrdering` enters one twice, once per comparison side.
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
   * The references in `child` that name one of these definitions, paired with the definition each
   * names, found once since the tree does not change between evaluations. Only `child` is scanned,
   * which is why the checks below refuse a definition holding a reference of this scope.
   */
  @transient private lazy val refsToBind: IndexedSeq[(CommonExpressionRef, CommonExpressionDef)] = {
    // Three shapes whose bindings cannot be kept straight: before these checks this path answered
    // the first two with a wrong value, and the third either overflowed the stack or failed as an
    // unbound reference. Generating them is refused for the first two, by an id-keyed rule in
    // `withCommonExprs`, and for a definition that reaches its own id, by `filling` -- but not for
    // one that only reads a sibling, which generates correctly, since every sibling slot is
    // registered before the child. Only a caller building the case class directly reaches any of
    // this: `With.apply` mints both the definitions and their ids. The last check keys on the whole
    // `CommonExpressionId`, so it also fires on a canonicalized or `NormalizePlan`-normalized tree,
    // where ids are renumbered per scope; those forms are compared, never evaluated.
    if (defs.map(_.id).distinct.length != defs.length) {
      throw SparkException.internalError(
        "Duplicate common expression ids in one With: " + defs.map(_.id.id).mkString(", "))
    }
    (child +: defs.map(_.child)).foreach(_.foreach {
      case w: With if w.defs.exists(innerDef => defs.exists(_ eq innerDef)) =>
        throw SparkException.internalError(
          "A nested With shares a common expression definition object with the With around it")
      case _ =>
    })
    val ownIds = defs.map(_.id).toSet
    defs.foreach(_.child.foreach {
      case r: CommonExpressionRef if ownIds.contains(r.id) =>
        throw SparkException.internalError(
          "A common expression definition references an id its own With defines: " + r.id.id)
      case _ =>
    })
    val idToDef = defs.map(d => d.id -> d).toMap
    val found = mutable.ArrayBuffer.empty[(CommonExpressionRef, CommonExpressionDef)]
    child.foreach {
      // One entry per reference *object*, not per occurrence: `BETWEEN` reads one object twice, and
      // a second entry would save the binding the first just installed. Identity, not equality --
      // two objects of one id compare equal while needing separate saves.
      case r: CommonExpressionRef if idToDef.contains(r.id) && !found.exists(_._1 eq r) =>
        found += ((r, idToDef(r.id)))
      case _ =>
    }
    found.toIndexedSeq
  }

  /**
   * Where each reference pointed before this `With` bound it, restored when the child returns, so a
   * reference shared with an enclosing `With` goes back to answering for that one. Instance state
   * rather than per-row allocation: a `With` cannot be re-entered during its own entry, which would
   * need the tree to contain itself.
   */
  @transient private lazy val savedDefinitions = new Array[Expression](refsToBind.length)
  @transient private lazy val savedCells = new Array[CommonExpressionCell](refsToBind.length)

  /**
   * Binds this `With`'s references to its own cells, clears them, and evaluates the child, so a
   * definition is evaluated where the child would have evaluated it, once. See
   * [[CommonExpressionCell]].
   *
   * Binding happens per evaluation, not once: two `With`s can share a reference object, because a
   * rebuilt reference compares equal to the one it replaces and `transform` therefore keeps the
   * original. Binding once would leave whichever bound last deciding for both. Bind-and-restore
   * instead makes the `With` currently evaluating the owner, and only until its child is done.
   */
  override def eval(input: InternalRow): Any = {
    var i = 0
    while (i < refsToBind.length) {
      val (ref, exprDef) = refsToBind(i)
      savedDefinitions(i) = ref.boundDefinition
      savedCells(i) = ref.boundCell
      ref.bindTo(exprDef)
      i += 1
    }
    defs.foreach(_.cell.clear())
    try {
      child.eval(input)
    } finally {
      var j = 0
      while (j < refsToBind.length) {
        refsToBind(j)._1.bindTo(savedDefinitions(j), savedCells(j))
        j += 1
      }
    }
  }

  // The cells are cleared on entry, so this holds state for the duration of one evaluation.
  override def stateful: Boolean = true

  /**
   * Whether one of this `With`'s references will be evaluated interpretively even though this
   * `With` is generated: it sits under a [[CodegenFallback]], or under a nested `With` that itself
   * falls back (`With` is not a `CodegenFallback`, so it has to be named rather than matched). Such
   * a reference needs its cell bound and cleared; the generated code clears codegen flags instead.
   */
  @transient private lazy val refUnderCodegenFallback: Boolean = {
    val ids = defs.map(_.id).toSet
    def holdsMyRef(e: Expression): Boolean = e.exists {
      case r: CommonExpressionRef => ids.contains(r.id)
      case _ => false
    }
    child.exists {
      case f: CodegenFallback => holdsMyRef(f)
      case w: With if w.refUnderCodegenFallback => holdsMyRef(w)
      case _ => false
    }
  }

  /**
   * Clears each definition's flag, then generates the child into the same block, so on a row that
   * does not reach the branch holding this `With` neither the clearing nor any reference runs.
   *
   * A reference that will be evaluated interpretively (see [[refUnderCodegenFallback]]) sends the
   * whole `With` down [[eval]] instead. Generating part of it would compute a definition once
   * through the flags and once through the cell, holding two values for one row. `ctx.INPUT_ROW` is
   * set on that path because `CollapseCodegenStages.supportCodegen` turns whole-stage codegen off
   * for a plan whose expressions hold the offending `CodegenFallback`. No builder in the tree
   * reaches it today -- `Between` and `NullIf` put their references under nodes that generate code
   * -- so this is for the next builder and for the nested case.
   *
   * One object is registered however many times this is generated, so a stateful definition
   * advances across the two keys `GenerateOrdering` generates. That predates this expression: a
   * stateful `CodegenFallback` sort key behaves the same way with no `With` in the tree, since
   * `freshCopyIfContainsStatefulExpression` does not reach a stateful leaf that leaves
   * `LeafLike`'s default `withNewChildrenInternal` in place.
   */
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    if (refUnderCodegenFallback) {
      return CodegenFallback.generate(this, ctx, ev)
    }
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
    // `CommonExpressionRef` in the `child`. This cannot be used to hand the new `With` its own
    // reference objects: a rebuilt reference is `==` the one it replaces, since the binding it
    // carries is not part of its equality, so `transform` keeps the original. `eval` rebinds
    // instead of relying on the references being unshared -- see `refsToBind`.
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
 * The `cell` holds the value on the row being evaluated. It sits outside the case class parameters,
 * so a definition still compares and canonicalizes by its id and child, and `copy` gives the copy a
 * fresh one. The enclosing [[With]] binds each reference to the cell of the definition standing in
 * the tree with it, so a cell that a transform left behind is never read.
 *
 * Staying `Unevaluable` is what keeps optimizer-time folding away from a `With` that survives in a
 * conditional branch: `ConvertToLocalRelation` refuses a projection holding one, so a branch over a
 * `LocalRelation` is not executed while the plan is still being optimized. Making a definition
 * evaluable would start folding those branches.
 */
case class CommonExpressionDef(child: Expression, id: CommonExpressionId = new CommonExpressionId())
  extends UnaryExpression with Unevaluable {
  private[expressions] val cell: CommonExpressionCell = new CommonExpressionCell

  // The definition owns the cell its references read, so a copy must own a different one. `copy`
  // gives it one; declaring this is what makes `freshCopyIfContainsStatefulExpression` ask for the
  // copy even when the definition's own child did not change.
  override def stateful: Boolean = true

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
   *
   * `@transient` for the reason [[CommonExpressionCell]]'s own fields are: a binding lives only for
   * the duration of one `With.eval`, which restores it on the way out, so serializing a task sees
   * null. A serialization that happens while an evaluation is still on the stack would otherwise
   * capture a live binding, and the deserialized reference would evaluate that definition rather
   * than raise, dragging the definition's whole subtree along with it.
   */
  @transient private var definition: Expression = _
  @transient private var cell: CommonExpressionCell = _

  private[expressions] def bindTo(exprDef: CommonExpressionDef): Unit = {
    definition = exprDef.child
    cell = exprDef.cell
  }

  private[expressions] def boundDefinition: Expression = definition
  private[expressions] def boundCell: CommonExpressionCell = cell

  private[expressions] def bindTo(
      newDefinition: Expression,
      newCell: CommonExpressionCell): Unit = {
    definition = newDefinition
    cell = newCell
  }

  override val nodePatterns: Seq[TreePattern] = Seq(COMMON_EXPR_REF)

  // The cell is cleared by the enclosing `With` on every entry, so this reads mutable state.
  override def stateful: Boolean = true

  /**
   * A copy must not carry this reference's binding: the copy belongs to a different `With`, which
   * wires it to its own cell. `LeafLike` returns `this` here, which would hand two `With`s one
   * reference object and let whichever wires last decide what both of them read --
   * `NamedLambdaVariable` overrides this for the same reason.
   */
  override def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): CommonExpressionRef = copy()

  override def eval(input: InternalRow): Any = {
    if (cell == null) {
      throw SparkException.internalError(
        s"Cannot evaluate a common expression reference outside its With: $this")
    }
    cell.get(definition, input)
  }

  /**
   * Computes the definition into the shared slots if this row has not done so yet, then reads them.
   * The code that computes it is emitted here rather than by the enclosing `With`, so it runs where
   * the first reference is reached -- behind a short-circuiting operator or a nested conditional,
   * if that is where the reference sits.
   *
   * A second reference emits the same code again, which never runs because the flag is set. What
   * that code is depends on the definition: a call, where the definition can be put in a method, so
   * that a definition that is or holds another `With` is not pasted once per reference at every
   * level; the body itself otherwise, whose locals are declared inside each guard. No copy of one
   * body encloses another, so repeating it declares nothing twice in one scope: a definition
   * reading a sibling is generated inside that sibling's guard, not inside its own, and one that
   * reaches its own id -- directly or around a cycle -- re-enters `fill` and is refused before any
   * Java exists. See `CommonExprSlots.fill`, which also says what bounds the code when a method is
   * not possible.
   */
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val slots = ctx.getCommonExpr(id.id)
    ev.copy(
      code = code"""
         |if (!${slots.computed}) {
         |  ${slots.fill}
         |}
       """.stripMargin,
      isNull = slots.value.isNull,
      value = slots.value.value)
  }
}
