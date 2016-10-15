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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.types.{DataType, StructType}

abstract class QueryPlan[PlanType <: QueryPlan[PlanType]] extends TreeNode[PlanType] {
  self: PlanType =>

  def output: Seq[Attribute]

  /**
   * Extracts the relevant constraints from a given set of constraints based on the attributes that
   * appear in the [[outputSet]].
   */
  protected def getRelevantConstraints(constraints: Set[Expression]): Set[Expression] = {
    constraints
      .union(inferAdditionalConstraints(constraints))
      .union(constructIsNotNullConstraints(constraints))
      .filter(constraint =>
        constraint.references.nonEmpty && constraint.references.subsetOf(outputSet) &&
          constraint.deterministic)
  }

  /**
   * Infers a set of `isNotNull` constraints from a given set of equality/comparison expressions as
   * well as non-nullable attributes. For e.g., if an expression is of the form (`a > 5`), this
   * returns a constraint of the form `isNotNull(a)`
   */
  private def constructIsNotNullConstraints(constraints: Set[Expression]): Set[Expression] = {
    // First, we propagate constraints from the null intolerant expressions.
    var isNotNullConstraints: Set[Expression] =
      constraints.flatMap(scanNullIntolerantExpr).map(IsNotNull(_))

    // Second, we infer additional constraints from non-nullable attributes that are part of the
    // operator's output
    val nonNullableAttributes = output.filterNot(_.nullable)
    isNotNullConstraints ++= nonNullableAttributes.map(IsNotNull).toSet

    isNotNullConstraints -- constraints
  }

  /**
   * Recursively explores the expressions which are null intolerant and returns all attributes
   * in these expressions.
   */
  private def scanNullIntolerantExpr(expr: Expression): Seq[Attribute] = expr match {
    case a: Attribute => Seq(a)
    case _: NullIntolerant | IsNotNull(_: NullIntolerant) =>
      expr.children.flatMap(scanNullIntolerantExpr)
    case _ => Seq.empty[Attribute]
  }

  /**
   * Infers an additional set of constraints from a given set of equality constraints.
   * For e.g., if an operator has constraints of the form (`a = 5`, `a = b`), this returns an
   * additional constraint of the form `b = 5`
   */
  private def inferAdditionalConstraints(constraints: Set[Expression]): Set[Expression] = {
    // Collect alias from expressions to avoid producing non-converging set of constraints
    // for recursive functions.
    //
    // Don't apply transform on constraints if the attribute used to replace is an alias,
    // because then both `QueryPlan.inferAdditionalConstraints` and
    // `UnaryNode.getAliasedConstraints` applies and may produce a non-converging set of
    // constraints.
    // For more details, refer to https://issues.apache.org/jira/browse/SPARK-17733
    val aliasSet = AttributeSet((expressions ++ children.flatMap(_.expressions)).collect {
      case a: Alias => a.toAttribute
    })

    var inferredConstraints = Set.empty[Expression]
    constraints.foreach {
      case eq @ EqualTo(l: Attribute, r: Attribute) =>
        inferredConstraints ++= (constraints - eq).map(_ transform {
          case a: Attribute if a.semanticEquals(l) && !aliasSet.contains(r) => r
        })
        inferredConstraints ++= (constraints - eq).map(_ transform {
          case a: Attribute if a.semanticEquals(r) && !aliasSet.contains(l) => l
        })
      case _ => // No inference
    }
    inferredConstraints -- constraints
  }

  /**
   * An [[ExpressionSet]] that contains invariants about the rows output by this operator. For
   * example, if this set contains the expression `a = 2` then that expression is guaranteed to
   * evaluate to `true` for all rows produced.
   */
  lazy val constraints: ExpressionSet = ExpressionSet(getRelevantConstraints(validConstraints))

  /**
   * This method can be overridden by any child class of QueryPlan to specify a set of constraints
   * based on the given operator's constraint propagation logic. These constraints are then
   * canonicalized and filtered automatically to contain only those attributes that appear in the
   * [[outputSet]].
   *
   * See [[Canonicalize]] for more details.
   */
  protected def validConstraints: Set[Expression] = Set.empty

  /**
   * Returns the set of attributes that are output by this node.
   */
  def outputSet: AttributeSet = AttributeSet(output)

  /**
   * All Attributes that appear in expressions from this operator.  Note that this set does not
   * include attributes that are implicitly referenced by being passed through to the output tuple.
   */
  def references: AttributeSet = AttributeSet(expressions.flatMap(_.references))

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
   * Attributes that are referenced by expressions but not provided by this nodes children.
   * Subclasses should override this method if they produce attributes internally as it is used by
   * assertions designed to prevent the construction of invalid plans.
   */
  def missingInput: AttributeSet = references -- inputSet -- producedAttributes

  /**
   * Runs [[transform]] with `rule` on all expressions present in this query operator.
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
    var changed = false

    @inline def transformExpressionDown(e: Expression): Expression = {
      val newE = e.transformDown(rule)
      if (newE.fastEquals(e)) {
        e
      } else {
        changed = true
        newE
      }
    }

    def recursiveTransform(arg: Any): AnyRef = arg match {
      case e: Expression => transformExpressionDown(e)
      case Some(e: Expression) => Some(transformExpressionDown(e))
      case m: Map[_, _] => m
      case d: DataType => d // Avoid unpacking Structs
      case seq: Traversable[_] => seq.map(recursiveTransform)
      case other: AnyRef => other
      case null => null
    }

    val newArgs = mapProductIterator(recursiveTransform)

    if (changed) makeCopy(newArgs).asInstanceOf[this.type] else this
  }

  /**
   * Runs [[transformUp]] with `rule` on all expressions present in this query operator.
   *
   * @param rule the rule to be applied to every expression in this operator.
   * @return
   */
  def transformExpressionsUp(rule: PartialFunction[Expression, Expression]): this.type = {
    var changed = false

    @inline def transformExpressionUp(e: Expression): Expression = {
      val newE = e.transformUp(rule)
      if (newE.fastEquals(e)) {
        e
      } else {
        changed = true
        newE
      }
    }

    def recursiveTransform(arg: Any): AnyRef = arg match {
      case e: Expression => transformExpressionUp(e)
      case Some(e: Expression) => Some(transformExpressionUp(e))
      case m: Map[_, _] => m
      case d: DataType => d // Avoid unpacking Structs
      case seq: Traversable[_] => seq.map(recursiveTransform)
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
    def seqToExpressions(seq: Traversable[Any]): Traversable[Expression] = seq.flatMap {
      case e: Expression => e :: Nil
      case s: Traversable[_] => seqToExpressions(s)
      case other => Nil
    }

    productIterator.flatMap {
      case e: Expression => e :: Nil
      case Some(e: Expression) => e :: Nil
      case seq: Traversable[_] => seqToExpressions(seq)
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

  override def simpleString: String = statePrefix + super.simpleString

  override def verboseString: String = simpleString

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
   * Canonicalized copy of this query plan.
   */
  protected lazy val canonicalized: PlanType = this

  /**
   * Returns true when the given query plan will return the same results as this query plan.
   *
   * Since its likely undecidable to generally determine if two given plans will produce the same
   * results, it is okay for this function to return false, even if the results are actually
   * the same.  Such behavior will not affect correctness, only the application of performance
   * enhancements like caching.  However, it is not acceptable to return true if the results could
   * possibly be different.
   *
   * By default this function performs a modified version of equality that is tolerant of cosmetic
   * differences like attribute naming and or expression id differences. Operators that
   * can do better should override this function.
   */
  def sameResult(plan: PlanType): Boolean = {
    val left = this.canonicalized
    val right = plan.canonicalized
    left.getClass == right.getClass &&
      left.children.size == right.children.size &&
      left.cleanArgs == right.cleanArgs &&
      (left.children, right.children).zipped.forall(_ sameResult _)
  }

  /**
   * All the attributes that are used for this plan.
   */
  lazy val allAttributes: AttributeSeq = children.flatMap(_.output)

  protected def cleanExpression(e: Expression): Expression = e match {
    case a: Alias =>
      // As the root of the expression, Alias will always take an arbitrary exprId, we need
      // to erase that for equality testing.
      val cleanedExprId =
        Alias(a.child, a.name)(ExprId(-1), a.qualifier, isGenerated = a.isGenerated)
      BindReferences.bindReference(cleanedExprId, allAttributes, allowFailures = true)
    case other =>
      BindReferences.bindReference(other, allAttributes, allowFailures = true)
  }

  /** Args that have cleaned such that differences in expression id should not affect equality */
  protected lazy val cleanArgs: Seq[Any] = {
    def cleanArg(arg: Any): Any = arg match {
      // Children are checked using sameResult above.
      case tn: TreeNode[_] if containsChild(tn) => null
      case e: Expression => cleanExpression(e).canonicalized
      case other => other
    }

    mapProductIterator {
      case s: Option[_] => s.map(cleanArg)
      case s: Seq[_] => s.map(cleanArg)
      case m: Map[_, _] => m.mapValues(cleanArg)
      case other => cleanArg(other)
    }.toSeq
  }
}
