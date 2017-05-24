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
   * Infers a set of `isNotNull` constraints from null intolerant expressions as well as
   * non-nullable attributes. For e.g., if an expression is of the form (`a > 5`), this
   * returns a constraint of the form `isNotNull(a)`
   */
  private def constructIsNotNullConstraints(constraints: Set[Expression]): Set[Expression] = {
    // First, we propagate constraints from the null intolerant expressions.
    var isNotNullConstraints: Set[Expression] = constraints.flatMap(inferIsNotNullConstraints)

    // Second, we infer additional constraints from non-nullable attributes that are part of the
    // operator's output
    val nonNullableAttributes = output.filterNot(_.nullable)
    isNotNullConstraints ++= nonNullableAttributes.map(IsNotNull).toSet

    isNotNullConstraints -- constraints
  }

  /**
   * Infer the Attribute-specific IsNotNull constraints from the null intolerant child expressions
   * of constraints.
   */
  private def inferIsNotNullConstraints(constraint: Expression): Seq[Expression] =
    constraint match {
      // When the root is IsNotNull, we can push IsNotNull through the child null intolerant
      // expressions
      case IsNotNull(expr) => scanNullIntolerantAttribute(expr).map(IsNotNull(_))
      // Constraints always return true for all the inputs. That means, null will never be returned.
      // Thus, we can infer `IsNotNull(constraint)`, and also push IsNotNull through the child
      // null intolerant expressions.
      case _ => scanNullIntolerantAttribute(constraint).map(IsNotNull(_))
    }

  /**
   * Recursively explores the expressions which are null intolerant and returns all attributes
   * in these expressions.
   */
  private def scanNullIntolerantAttribute(expr: Expression): Seq[Attribute] = expr match {
    case a: Attribute => Seq(a)
    case _: NullIntolerant => expr.children.flatMap(scanNullIntolerantAttribute)
    case _ => Seq.empty[Attribute]
  }

  // Collect aliases from expressions of the whole tree rooted by the current QueryPlan node, so
  // we may avoid producing recursive constraints.
  private lazy val aliasMap: AttributeMap[Expression] = AttributeMap(
    expressions.collect {
      case a: Alias => (a.toAttribute, a.child)
    } ++ children.flatMap(_.aliasMap))

  /**
   * Infers an additional set of constraints from a given set of equality constraints.
   * For e.g., if an operator has constraints of the form (`a = 5`, `a = b`), this returns an
   * additional constraint of the form `b = 5`.
   *
   * [SPARK-17733] We explicitly prevent producing recursive constraints of the form `a = f(a, b)`
   * as they are often useless and can lead to a non-converging set of constraints.
   */
  private def inferAdditionalConstraints(constraints: Set[Expression]): Set[Expression] = {
    val constraintClasses = generateEquivalentConstraintClasses(constraints)

    var inferredConstraints = Set.empty[Expression]
    constraints.foreach {
      case eq @ EqualTo(l: Attribute, r: Attribute) =>
        val candidateConstraints = constraints - eq
        inferredConstraints ++= candidateConstraints.map(_ transform {
          case a: Attribute if a.semanticEquals(l) &&
            !isRecursiveDeduction(r, constraintClasses) => r
        })
        inferredConstraints ++= candidateConstraints.map(_ transform {
          case a: Attribute if a.semanticEquals(r) &&
            !isRecursiveDeduction(l, constraintClasses) => l
        })
      case _ => // No inference
    }
    inferredConstraints -- constraints
  }

  /*
   * Generate a sequence of expression sets from constraints, where each set stores an equivalence
   * class of expressions. For example, Set(`a = b`, `b = c`, `e = f`) will generate the following
   * expression sets: (Set(a, b, c), Set(e, f)). This will be used to search all expressions equal
   * to an selected attribute.
   */
  private def generateEquivalentConstraintClasses(
      constraints: Set[Expression]): Seq[Set[Expression]] = {
    var constraintClasses = Seq.empty[Set[Expression]]
    constraints.foreach {
      case eq @ EqualTo(l: Attribute, r: Attribute) =>
        // Transform [[Alias]] to its child.
        val left = aliasMap.getOrElse(l, l)
        val right = aliasMap.getOrElse(r, r)
        // Get the expression set for an equivalence constraint class.
        val leftConstraintClass = getConstraintClass(left, constraintClasses)
        val rightConstraintClass = getConstraintClass(right, constraintClasses)
        if (leftConstraintClass.nonEmpty && rightConstraintClass.nonEmpty) {
          // Combine the two sets.
          constraintClasses = constraintClasses
            .diff(leftConstraintClass :: rightConstraintClass :: Nil) :+
            (leftConstraintClass ++ rightConstraintClass)
        } else if (leftConstraintClass.nonEmpty) { // && rightConstraintClass.isEmpty
          // Update equivalence class of `left` expression.
          constraintClasses = constraintClasses
            .diff(leftConstraintClass :: Nil) :+ (leftConstraintClass + right)
        } else if (rightConstraintClass.nonEmpty) { // && leftConstraintClass.isEmpty
          // Update equivalence class of `right` expression.
          constraintClasses = constraintClasses
            .diff(rightConstraintClass :: Nil) :+ (rightConstraintClass + left)
        } else { // leftConstraintClass.isEmpty && rightConstraintClass.isEmpty
          // Create new equivalence constraint class since neither expression presents
          // in any classes.
          constraintClasses = constraintClasses :+ Set(left, right)
        }
      case _ => // Skip
    }

    constraintClasses
  }

  /*
   * Get all expressions equivalent to the selected expression.
   */
  private def getConstraintClass(
      expr: Expression,
      constraintClasses: Seq[Set[Expression]]): Set[Expression] =
    constraintClasses.find(_.contains(expr)).getOrElse(Set.empty[Expression])

  /*
   *  Check whether replace by an [[Attribute]] will cause a recursive deduction. Generally it
   *  has the form like: `a -> f(a, b)`, where `a` and `b` are expressions and `f` is a function.
   *  Here we first get all expressions equal to `attr` and then check whether at least one of them
   *  is a child of the referenced expression.
   */
  private def isRecursiveDeduction(
      attr: Attribute,
      constraintClasses: Seq[Set[Expression]]): Boolean = {
    val expr = aliasMap.getOrElse(attr, attr)
    getConstraintClass(expr, constraintClasses).exists { e =>
      expr.children.exists(_.semanticEquals(e))
    }
  }

  /**
   * An [[ExpressionSet]] that contains invariants about the rows output by this operator. For
   * example, if this set contains the expression `a = 2` then that expression is guaranteed to
   * evaluate to `true` for all rows produced.
   */
  lazy val constraints: ExpressionSet = ExpressionSet(getRelevantConstraints(validConstraints))

  /**
   * Returns [[constraints]] depending on the config of enabling constraint propagation. If the
   * flag is disabled, simply returning an empty constraints.
   */
  private[spark] def getConstraints(constraintPropagationEnabled: Boolean): ExpressionSet =
    if (constraintPropagationEnabled) {
      constraints
    } else {
      ExpressionSet(Set.empty)
    }

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
      val newE = f(e)
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
      case s: Some[_] => seqToExpressions(s.toSeq)
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
   * Returns a plan where a best effort attempt has been made to transform `this` in a way
   * that preserves the result but removes cosmetic variations (case sensitivity, ordering for
   * commutative operations, expression id, etc.)
   *
   * Plans where `this.canonicalized == other.canonicalized` will always evaluate to the same
   * result.
   *
   * Some nodes should overwrite this to provide proper canonicalize logic.
   */
  lazy val canonicalized: PlanType = {
    val canonicalizedChildren = children.map(_.canonicalized)
    var id = -1
    preCanonicalized.mapExpressions {
      case a: Alias =>
        id += 1
        // As the root of the expression, Alias will always take an arbitrary exprId, we need to
        // normalize that for equality testing, by assigning expr id from 0 incrementally. The
        // alias name doesn't matter and should be erased.
        val normalizedChild = QueryPlan.normalizeExprId(a.child, allAttributes)
        Alias(normalizedChild, "")(ExprId(id), a.qualifier, isGenerated = a.isGenerated)

      case ar: AttributeReference if allAttributes.indexOf(ar.exprId) == -1 =>
        // Top level `AttributeReference` may also be used for output like `Alias`, we should
        // normalize the epxrId too.
        id += 1
        ar.withExprId(ExprId(id))

      case other => QueryPlan.normalizeExprId(other, allAttributes)
    }.withNewChildren(canonicalizedChildren)
  }

  /**
   * Do some simple transformation on this plan before canonicalizing. Implementations can override
   * this method to provide customized canonicalize logic without rewriting the whole logic.
   */
  protected def preCanonicalized: PlanType = this


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
      case s: SubqueryExpression => s.canonicalize(input)
      case ar: AttributeReference =>
        val ordinal = input.indexOf(ar.exprId)
        if (ordinal == -1) {
          ar
        } else {
          ar.withExprId(ExprId(ordinal))
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
}
