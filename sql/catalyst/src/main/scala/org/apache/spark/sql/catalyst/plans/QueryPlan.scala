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

package org.apache.spark.sql.catalyst.plans

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, TreeNode}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}

abstract class QueryPlan[PlanType <: QueryPlan[PlanType]] extends TreeNode[PlanType] {
  self: PlanType =>

  /**
   * The active config object within the current scope.
   * See [[SQLConf.get]] for more information.
   */
  def conf: SQLConf = SQLConf.get

  def output: Seq[Attribute]

  /**
   * Returns the set of attributes that are output by this node.
   */
  def outputSet: AttributeSet = AttributeSet(output)

  /**
   * All Attributes that appear in expressions from this operator.  Note that this set does not
   * include attributes that are implicitly referenced by being passed through to the output tuple.
   */
  def references: AttributeSet = AttributeSet.fromAttributeSets(expressions.map(_.references))

  /**
   * The set of all attributes that are input to this operator by its children.
   */
  def inputSet: AttributeSet =
    AttributeSet(children.flatMap(_.asInstanceOf[QueryPlan[PlanType]].output))

  /**
   * The set of all attributes that are produced by this node.
   */
  def producedAttributes: AttributeSet = AttributeSet.empty

  /**
   * Attributes that are referenced by expressions but not provided by this node's children.
   * Subclasses should override this method if they produce attributes internally as it is used by
   * assertions designed to prevent the construction of invalid plans.
   */
  def missingInput: AttributeSet = references -- inputSet -- producedAttributes

  /**
   * Runs [[transformExpressionsDown]] with `rule` on all expressions present
   * in this query operator.
   * Users should not expect a specific directionality. If a specific directionality is needed,
   * transformExpressionsDown or transformExpressionsUp should be used.
   *
   * @param rule the rule to be applied to every expression in this operator.
   */
  def transformExpressions(rule: PartialFunction[Expression, Expression]): this.type = {
    transformExpressionsDown(rule)
  }

  /**
   * Runs [[transformDown]] with `rule` on all expressions present in this query operator.
   *
   * @param rule the rule to be applied to every expression in this operator.
   */
  def transformExpressionsDown(rule: PartialFunction[Expression, Expression]): this.type = {
    mapExpressions(_.transformDown(rule))
  }

  /**
   * Runs [[transformUp]] with `rule` on all expressions present in this query operator.
   *
   * @param rule the rule to be applied to every expression in this operator.
   * @return
   */
  def transformExpressionsUp(rule: PartialFunction[Expression, Expression]): this.type = {
    mapExpressions(_.transformUp(rule))
  }

  /**
   * Apply a map function to each expression present in this query operator, and return a new
   * query operator based on the mapped expressions.
   */
  def mapExpressions(f: Expression => Expression): this.type = {
    var changed = false

    @inline def transformExpression(e: Expression): Expression = {
      val newE = CurrentOrigin.withOrigin(e.origin) {
        f(e)
      }
      if (newE.fastEquals(e)) {
        e
      } else {
        changed = true
        newE
      }
    }

    def recursiveTransform(arg: Any): AnyRef = arg match {
      case e: Expression => transformExpression(e)
      case Some(value) => Some(recursiveTransform(value))
      case m: Map[_, _] => m
      case d: DataType => d // Avoid unpacking Structs
      case stream: Stream[_] => stream.map(recursiveTransform).force
      case seq: Iterable[_] => seq.map(recursiveTransform)
      case other: AnyRef => other
      case null => null
    }

    val newArgs = mapProductIterator(recursiveTransform)

    if (changed) makeCopy(newArgs).asInstanceOf[this.type] else this
  }

  /**
   * Returns the result of running [[transformExpressions]] on this node
   * and all its children.
   */
  def transformAllExpressions(rule: PartialFunction[Expression, Expression]): this.type = {
    transform {
      case q: QueryPlan[_] => q.transformExpressions(rule).asInstanceOf[PlanType]
    }.asInstanceOf[this.type]
  }

  /** Returns all of the expressions present in this query plan operator. */
  final def expressions: Seq[Expression] = {
    // Recursively find all expressions from a traversable.
    def seqToExpressions(seq: Iterable[Any]): Iterable[Expression] = seq.flatMap {
      case e: Expression => e :: Nil
      case s: Iterable[_] => seqToExpressions(s)
      case other => Nil
    }

    productIterator.flatMap {
      case e: Expression => e :: Nil
      case s: Some[_] => seqToExpressions(s.toSeq)
      case seq: Iterable[_] => seqToExpressions(seq)
      case other => Nil
    }.toSeq
  }

  lazy val schema: StructType = StructType.fromAttributes(output)

  /** Returns the output schema in the tree format. */
  def schemaString: String = schema.treeString

  /** Prints out the schema in the tree format */
  // scalastyle:off println
  def printSchema(): Unit = println(schemaString)
  // scalastyle:on println

  /**
   * A prefix string used when printing the plan.
   *
   * We use "!" to indicate an invalid plan, and "'" to indicate an unresolved plan.
   */
  protected def statePrefix = if (missingInput.nonEmpty && children.nonEmpty) "!" else ""

  override def simpleString(maxFields: Int): String = statePrefix + super.simpleString(maxFields)

  override def verboseString(maxFields: Int): String = simpleString(maxFields)

  /**
   * All the subqueries of current plan.
   */
  def subqueries: Seq[PlanType] = {
    expressions.flatMap(_.collect {
      case e: PlanExpression[_] => e.plan.asInstanceOf[PlanType]
    })
  }

  override protected def innerChildren: Seq[QueryPlan[_]] = subqueries

  /**
   * A private mutable variable to indicate whether this plan is the result of canonicalization.
   * This is used solely for making sure we wouldn't execute a canonicalized plan.
   * See [[canonicalized]] on how this is set.
   */
  @transient private var _isCanonicalizedPlan: Boolean = false

  protected def isCanonicalizedPlan: Boolean = _isCanonicalizedPlan

  /**
   * Returns a plan where a best effort attempt has been made to transform `this` in a way
   * that preserves the result but removes cosmetic variations (case sensitivity, ordering for
   * commutative operations, expression id, etc.)
   *
   * Plans where `this.canonicalized == other.canonicalized` will always evaluate to the same
   * result.
   *
   * Plan nodes that require special canonicalization should override [[doCanonicalize()]].
   * They should remove expressions cosmetic variations themselves.
   */
  @transient final lazy val canonicalized: PlanType = {
    var plan = doCanonicalize()
    // If the plan has not been changed due to canonicalization, make a copy of it so we don't
    // mutate the original plan's _isCanonicalizedPlan flag.
    if (plan eq this) {
      plan = plan.makeCopy(plan.mapProductIterator(x => x.asInstanceOf[AnyRef]))
    }
    plan._isCanonicalizedPlan = true
    plan
  }

  /**
   * Defines how the canonicalization should work for the current plan.
   */
  protected def doCanonicalize(): PlanType = {
    val canonicalizedChildren = children.map(_.canonicalized)
    var id = -1
    mapExpressions {
      case a: Alias =>
        id += 1
        // As the root of the expression, Alias will always take an arbitrary exprId, we need to
        // normalize that for equality testing, by assigning expr id from 0 incrementally. The
        // alias name doesn't matter and should be erased.
        val normalizedChild = QueryPlan.normalizeExprId(a.child, allAttributes)
        Alias(normalizedChild, "")(ExprId(id), a.qualifier)

      case ar: AttributeReference if allAttributes.indexOf(ar.exprId) == -1 =>
        // Top level `AttributeReference` may also be used for output like `Alias`, we should
        // normalize the epxrId too.
        id += 1
        ar.withExprId(ExprId(id)).canonicalized

      case other => QueryPlan.normalizeExprId(other, allAttributes)
    }.withNewChildren(canonicalizedChildren)
  }

  /**
   * Returns true when the given query plan will return the same results as this query plan.
   *
   * Since its likely undecidable to generally determine if two given plans will produce the same
   * results, it is okay for this function to return false, even if the results are actually
   * the same.  Such behavior will not affect correctness, only the application of performance
   * enhancements like caching.  However, it is not acceptable to return true if the results could
   * possibly be different.
   *
   * This function performs a modified version of equality that is tolerant of cosmetic
   * differences like attribute naming and or expression id differences.
   */
  final def sameResult(other: PlanType): Boolean = this.canonicalized == other.canonicalized

  /**
   * Returns a `hashCode` for the calculation performed by this plan. Unlike the standard
   * `hashCode`, an attempt has been made to eliminate cosmetic differences.
   */
  final def semanticHash(): Int = canonicalized.hashCode()

  /**
   * All the attributes that are used for this plan.
   */
  lazy val allAttributes: AttributeSeq = children.flatMap(_.output)
}

object QueryPlan extends PredicateHelper {
  /**
   * Normalize the exprIds in the given expression, by updating the exprId in `AttributeReference`
   * with its referenced ordinal from input attributes. It's similar to `BindReferences` but we
   * do not use `BindReferences` here as the plan may take the expression as a parameter with type
   * `Attribute`, and replace it with `BoundReference` will cause error.
   */
  def normalizeExprId[T <: Expression](e: T, input: AttributeSeq): T = {
    e.transformUp {
      case s: PlanExpression[_] => s.canonicalize(input)
      case ar: AttributeReference =>
        val ordinal = input.indexOf(ar.exprId)
        if (ordinal == -1) {
          ar
        } else {
          ar.withExprId(ExprId(ordinal)).canonicalized
        }
    }.canonicalized.asInstanceOf[T]
  }

  /**
   * Composes the given predicates into a conjunctive predicate, which is normalized and reordered.
   * Then returns a new sequence of predicates by splitting the conjunctive predicate.
   */
  def normalizePredicates(predicates: Seq[Expression], output: AttributeSeq): Seq[Expression] = {
    if (predicates.nonEmpty) {
      val normalized = normalizeExprId(predicates.reduce(And), output)
      splitConjunctivePredicates(normalized)
    } else {
      Nil
    }
  }

  /**
   * Converts the query plan to string and appends it via provided function.
   */
  def append[T <: QueryPlan[T]](
      plan: => QueryPlan[T],
      append: String => Unit,
      verbose: Boolean,
      addSuffix: Boolean,
      maxFields: Int = SQLConf.get.maxToStringFields): Unit = {
    try {
      plan.treeString(append, verbose, addSuffix, maxFields)
    } catch {
      case e: AnalysisException => append(e.toString)
    }
  }
}
