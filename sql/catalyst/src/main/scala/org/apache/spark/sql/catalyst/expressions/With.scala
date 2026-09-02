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
 * The value of one common expression on the row being evaluated, computed the first time a
 * [[CommonExpressionRef]] reads it and reused by every later reference.
 *
 * The cell holds no expression of its own: the reference passes the definition in, so the cell is
 * a pair of mutable slots and serializes with the plan. [[With]] clears it on entry, every time it
 * is entered, which is not the same as once per row: the same `With` object can sit at two
 * positions of one tree, and a `With` that falls back to `eval` can be generated twice and entered
 * once per copy, as `GenerateOrdering` does for the two sides of a comparison. Clearing on entry is
 * what makes each entry self-contained, so a reference is never read against a value left over from
 * an earlier entry -- and on a row that does not reach the branch holding the `With`, nothing is
 * cleared because nothing is read.
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
   * names. The list is found once, since the tree does not change between evaluations.
   *
   * Only `child` is scanned, which relies on a reference to one of these definitions never living
   * inside another one of them. The helper `With(commonExprs: _*)(replaced)` builds the references
   * outside the definitions and cannot produce that, and `RewriteWithExpression`, which does
   * rewrite inside a `With`, only ever replaces a reference with its definition's child or with an
   * attribute -- it never puts a reference inside a definition. The case class constructor does
   * take `child` and `defs` directly, so a caller can hand a definition a reference to any of these
   * ids; that reference is then never bound, and evaluating it raises "Cannot evaluate a common
   * expression reference outside its With", which is the failure to want. Scanning `children`
   * instead would look safer and be worse -- it would bind such a reference, and since
   * `CommonExpressionCell.get` sets `computed` only after the nested evaluation returns, that loud
   * error would become a StackOverflowError. A nested `With` is not affected either way: `children`
   * is `child +: defs`, so this scan already descends into an inner `With`'s own definitions.
   */
  @transient private lazy val refsToBind: IndexedSeq[(CommonExpressionRef, CommonExpressionDef)] = {
    val idToDef = defs.map(d => d.id -> d).toMap
    val found = mutable.ArrayBuffer.empty[(CommonExpressionRef, CommonExpressionDef)]
    child.foreach {
      // One entry per reference *object*: `BETWEEN` reads one object twice, and two entries for it
      // would have the second save the binding the first just installed, so the restore below could
      // not put back what was there before. Equality would not do here, since two distinct objects
      // of the same id compare equal while needing separate saves.
      case r: CommonExpressionRef if idToDef.contains(r.id) && !found.exists(_._1 eq r) =>
        found += ((r, idToDef(r.id)))
      case _ =>
    }
    found.toIndexedSeq
  }

  /**
   * Where each reference pointed before this `With` bound it, restored when the child's evaluation
   * returns. A reference can be shared with another `With` -- see the binding note on [[eval]] --
   * and a shared one can be read again after a nested `With` has returned, so leaving the nested
   * binding in place would let the inner definition answer for the outer scope. The arrays are
   * instance state rather than allocated per row; the same `With` object cannot be entered while
   * one of its own entries is in progress, since that would need the tree to contain itself.
   */
  @transient private lazy val savedDefinitions = new Array[Expression](refsToBind.length)
  @transient private lazy val savedCells = new Array[CommonExpressionCell](refsToBind.length)

  /**
   * Binds this `With`'s references to its own cells, clears them, and evaluates the child. A
   * reference reached by that evaluation computes its definition once and every later reference
   * reads the value back, so a definition is evaluated where the child would have evaluated it,
   * once, rather than once per reference. See [[CommonExpressionCell]].
   *
   * The binding is redone on every evaluation rather than once, because a reference can be reached
   * from two `With`s. `withNewChildrenInternal` cannot hand the new `With` its own references: a
   * rebuilt reference compares equal to the one it replaces, since the binding it carries is not
   * part of its equality, so `transform` keeps the original. Binding once would then leave the
   * `With` that bound last deciding what both of them read. Rebinding costs one pass over the
   * distinct reference objects on entry and one to restore them on exit, and makes the `With`
   * currently evaluating the owner -- restoring what was there before makes it the owner only until
   * its child is done, which is what a lexical scope means. Without the restore, an outer reference
   * read after a nested `With` returned would still point at the inner definition.
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
   * Whether one of this `With`'s references sits somewhere that will be evaluated interpretively
   * even though this `With` is generated. Two shapes do that: a [[CodegenFallback]], which is
   * evaluated by calling `eval` on it from the generated code, and a nested `With` that itself
   * takes the fallback below -- `With` does not mix in `CodegenFallback`, so it has to be named
   * here rather than matched as one. A reference reached that way needs its cell bound and
   * cleared, which the generated code does not do: it clears the codegen flags.
   *
   * This is the same shape `EquivalentExpressions.childrenToRecurse` already refuses to look past,
   * for the same reason.
   *
   * Each level memoizes, but `holdsMyRef` runs again at every nested `With` the scan passes, so a
   * chain of them nested in each other's `child` costs on the order of the square of the depth.
   * `nullif(a, nullif(b, c))` does produce such a chain -- only the memoized input becomes a
   * definition, the rest stays in `child` -- but these chains are shallow in practice. Reading a
   * nested `With`'s own `lazy val` from here also takes its monitor while holding this one; the
   * edges only ever run from an ancestor to a proper descendant of an immutable tree, so the order
   * is a strict partial one and cannot deadlock. `canonicalizationIdMap` below relies on the same.
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
   * Clears each definition's flag, then generates the child. The flags are cleared in the same
   * block the child is generated into, so a reference cannot run against a flag left set by an
   * earlier row: on a row that does not reach the branch holding this `With`, neither the clearing
   * nor any reference runs.
   *
   * When a reference sits under a [[CodegenFallback]], or inside a nested `With` that itself falls
   * back, the whole `With` is evaluated interpretively instead. Generating the child would leave
   * that reference reading a cell nobody bound and nobody clears, and generating part of it is
   * worse still: a definition reached from both sides would be computed once through the flags and
   * once through the cell, holding two values for one row. [[eval]] binds and clears both, so
   * handing it the whole subtree keeps one mechanism in play. `ctx.INPUT_ROW` is available on that
   * path because `CollapseCodegenStages.supportCodegen` turns whole-stage codegen off for a plan
   * whose expressions hold the offending `CodegenFallback` -- it is visible there, since a `With`
   * in a conditional branch reaches execution inside `plan.expressions` like any other expression.
   *
   * No builder in the tree reaches this today. `Between` and `NullIf` are the only expressions
   * that build a `With` over a user expression, and every position they put a reference in is under
   * a node that generates code, once `ReplaceExpressions` has removed the `TypedNullLiteral` that
   * `NullIf` wraps one of them in, so a user's `CodegenFallback` cannot come to hold one.
   * Substituting a definition would relocate the references inside it, and duplicate them, but a
   * definition never holds a reference of its own `With`: the builder creates those objects and
   * puts them only in the child it replaces. A nested `With` standing as a definition takes its own
   * references along, whose parents move with them. It is here for the next builder that does, and
   * for the nested case, which needs a real one underneath it.
   *
   * One object is registered however many times this is generated, so two generated occurrences of
   * one `With` call `eval` on the same instance. `GenerateOrdering` does generate a key twice, once
   * per side of the comparison, which makes a stateful definition advance across the two sides.
   * That predates this expression and is not specific to it: a stateful `CodegenFallback` used as a
   * sort key behaves the same way with no `With` in the tree, and `InterpretedOrdering` escapes it
   * only where `freshCopyIfContainsStatefulExpression` on its right side reaches the state. That
   * copy does rebuild a stateful non-leaf, and a leaf that overrides `withNewChildrenInternal` as
   * `MonotonicallyIncreasingID` does, but a leaf that leaves `LeafLike`'s default in place is
   * handed to the copy as it is and keeps advancing. Nothing tracks that today.
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
   * body encloses another -- for that, a definition would have to reference its own id, directly or
   * through a sibling, which recurses in `fill` before any Java exists -- so repeating it declares
   * nothing twice in one scope. See `CommonExprSlots.fill`, which also says what bounds the code
   * when a method is not possible.
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
