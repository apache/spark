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

import java.lang.{Boolean => JBoolean}
import java.util.IdentityHashMap

import scala.collection.mutable

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.rules.RuleId
import org.apache.spark.sql.catalyst.rules.UnknownRuleId
import org.apache.spark.sql.catalyst.trees.{AlwaysProcess, CurrentOrigin, TreeNode, TreeNodeTag}
import org.apache.spark.sql.catalyst.trees.TreePattern
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.catalyst.trees.TreePatternBits
import org.apache.spark.sql.catalyst.trees.TreePatternBits.toPatternBits
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.util.{BestEffortLazyVal, TransientBestEffortLazyVal}
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.BitSet

/**
 * An abstraction of the Spark SQL query plan tree, which can be logical or physical. This class
 * defines some basic properties of a query plan node, as well as some new transform APIs to
 * transform the expressions of the plan node.
 *
 * Note that, the query plan is a mutually recursive structure:
 *   QueryPlan -> Expression (subquery) -> QueryPlan
 * The tree traverse APIs like `transform`, `foreach`, `collect`, etc. that are
 * inherited from `TreeNode`, do not traverse into query plans inside subqueries.
 */
abstract class QueryPlan[PlanType <: QueryPlan[PlanType]]
  extends TreeNode[PlanType] with SQLConfHelper {
  self: PlanType =>

  def output: Seq[Attribute]

  /**
   * Returns the set of attributes that are output by this node.
   */
  def outputSet: AttributeSet = _outputSet()

  private val _outputSet = new TransientBestEffortLazyVal(() => AttributeSet(output))

  /**
   * Returns the output ordering that this plan generates, although the semantics differ in logical
   * and physical plans. In the logical plan it means global ordering of the data while in physical
   * it means ordering in each partition.
   */
  def outputOrdering: Seq[SortOrder] = Nil

  // Override `treePatternBits` to propagate bits for its expressions.
  override lazy val treePatternBits: BitSet = {
    val bits: BitSet = getDefaultTreePatternBits
    // Propagate expressions' pattern bits
    val exprIterator = expressions.iterator
    while (exprIterator.hasNext) {
      bits.union(exprIterator.next().treePatternBits)
    }
    bits
  }

  /**
   * The set of all attributes that are input to this operator by its children.
   */
  def inputSet: AttributeSet = {
    children match {
      case Seq() => AttributeSet.empty
      case Seq(c) => c.outputSet
      case _ => AttributeSet.fromAttributeSets(children.map(_.outputSet))
    }
  }

  /**
   * The set of all attributes that are produced by this node.
   */
  def producedAttributes: AttributeSet = AttributeSet.empty

  /**
   * All Attributes that appear in expressions from this operator.  Note that this set does not
   * include attributes that are implicitly referenced by being passed through to the output tuple.
   */
  def references: AttributeSet = _references()

  private val _references = new TransientBestEffortLazyVal(() =>
    AttributeSet(expressions) -- producedAttributes)

  /**
   * Returns true when the all the expressions in the current node as well as all of its children
   * are deterministic
   */
  def deterministic: Boolean = _deterministic()

  private val _deterministic = new BestEffortLazyVal[JBoolean](() =>
    expressions.forall(_.deterministic) && children.forall(_.deterministic))

  /**
   * Attributes that are referenced by expressions but not provided by this node's children.
   */
  final def missingInput: AttributeSet = {
    if (references.isEmpty) {
      AttributeSet.empty
    } else {
      references -- inputSet
    }
  }

  /**
   * Runs [[transformExpressionsDown]] with `rule` on all expressions present
   * in this query operator.
   * Users should not expect a specific directionality. If a specific directionality is needed,
   * transformExpressionsDown or transformExpressionsUp should be used.
   *
   * @param rule the rule to be applied to every expression in this operator.
   */
  def transformExpressions(rule: PartialFunction[Expression, Expression]): this.type = {
    transformExpressionsWithPruning(AlwaysProcess.fn, UnknownRuleId)(rule)
  }

  /**
   * Runs [[transformExpressionsDownWithPruning]] with `rule` on all expressions present
   * in this query operator.
   * Users should not expect a specific directionality. If a specific directionality is needed,
   * transformExpressionsDown or transformExpressionsUp should be used.
   *
   * @param rule the rule to be applied to every expression in this operator.
   * @param cond   a Lambda expression to prune tree traversals. If `cond.apply` returns false
   *               on an expression T, skips processing T and its subtree; otherwise, processes
   *               T and its subtree recursively.
   * @param ruleId is a unique Id for `rule` to prune unnecessary tree traversals. When it is
   *               UnknownRuleId, no pruning happens. Otherwise, if `rule`(with id `ruleId`)
   *               has been marked as in effective on an expression T, skips processing T and its
   *               subtree. Do not pass it if the rule is not purely functional and reads a
   *               varying initial state for different invocations.
   */
  def transformExpressionsWithPruning(cond: TreePatternBits => Boolean,
    ruleId: RuleId = UnknownRuleId)(rule: PartialFunction[Expression, Expression])
  : this.type = {
    transformExpressionsDownWithPruning(cond, ruleId)(rule)
  }

  /**
   * Runs [[transformDown]] with `rule` on all expressions present in this query operator.
   *
   * @param rule the rule to be applied to every expression in this operator.
   */
  def transformExpressionsDown(rule: PartialFunction[Expression, Expression]): this.type = {
    transformExpressionsDownWithPruning(AlwaysProcess.fn, UnknownRuleId)(rule)
  }

  /**
   * Runs [[transformDownWithPruning]] with `rule` on all expressions present in this query
   * operator.
   *
   * @param rule   the rule to be applied to every expression in this operator.
   * @param cond   a Lambda expression to prune tree traversals. If `cond.apply` returns false
   *               on an expression T, skips processing T and its subtree; otherwise, processes
   *               T and its subtree recursively.
   * @param ruleId is a unique Id for `rule` to prune unnecessary tree traversals. When it is
   *               UnknownRuleId, no pruning happens. Otherwise, if `rule` (with id `ruleId`)
   *               has been marked as in effective on an expression T, skips processing T and its
   *               subtree. Do not pass it if the rule is not purely functional and reads a
   *               varying initial state for different invocations.
   */
  def transformExpressionsDownWithPruning(cond: TreePatternBits => Boolean,
    ruleId: RuleId = UnknownRuleId)(rule: PartialFunction[Expression, Expression])
  : this.type = {
    mapExpressions(_.transformDownWithPruning(cond, ruleId)(rule))
  }

  /**
   * Runs [[transformUp]] with `rule` on all expressions present in this query operator.
   *
   * @param rule the rule to be applied to every expression in this operator.
   */
  def transformExpressionsUp(rule: PartialFunction[Expression, Expression]): this.type = {
    transformExpressionsUpWithPruning(AlwaysProcess.fn, UnknownRuleId)(rule)
  }

  /**
   * Runs [[transformExpressionsUpWithPruning]] with `rule` on all expressions present in this
   * query operator.
   *
   * @param rule the rule to be applied to every expression in this operator.
   * @param cond   a Lambda expression to prune tree traversals. If `cond.apply` returns false
   *               on an expression T, skips processing T and its subtree; otherwise, processes
   *               T and its subtree recursively.
   * @param ruleId is a unique Id for `rule` to prune unnecessary tree traversals. When it is
   *               UnknownRuleId, no pruning happens. Otherwise, if `rule` (with id `ruleId`)
   *               has been marked as in effective on an expression T, skips processing T and its
   *               subtree. Do not pass it if the rule is not purely functional and reads a
   *               varying initial state for different invocations.
   */
  def transformExpressionsUpWithPruning(cond: TreePatternBits => Boolean,
    ruleId: RuleId = UnknownRuleId)(rule: PartialFunction[Expression, Expression])
  : this.type = {
    mapExpressions(_.transformUpWithPruning(cond, ruleId)(rule))
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

    @scala.annotation.nowarn("cat=deprecation")
    def recursiveTransform(arg: Any): AnyRef = arg match {
      case e: Expression => transformExpression(e)
      case Some(value) => Some(recursiveTransform(value))
      case m: Map[_, _] => m
      case d: DataType => d // Avoid unpacking Structs
      case stream: Stream[_] => stream.map(recursiveTransform).force
      case lazyList: LazyList[_] => lazyList.map(recursiveTransform).force
      case seq: Iterable[_] => seq.map(recursiveTransform)
      case other: AnyRef => other
      case null => null
    }

    val newArgs = mapProductIterator(recursiveTransform)

    if (changed) makeCopy(newArgs).asInstanceOf[this.type] else this
  }

  /**
   * Returns the result of running [[transformExpressions]] on this node
   * and all its children. Note that this method skips expressions inside subqueries.
   */
  def transformAllExpressions(rule: PartialFunction[Expression, Expression]): this.type = {
    transformAllExpressionsWithPruning(AlwaysProcess.fn, UnknownRuleId)(rule)
  }

  /**
   * A variant of [[transformAllExpressions]] which considers plan nodes inside subqueries as well.
   */
  def transformAllExpressionsWithSubqueries(
    rule: PartialFunction[Expression, Expression]): this.type = {
    transformWithSubqueries {
      case q => q.transformExpressions(rule)
    }.asInstanceOf[this.type]
  }

  /**
   * Returns the result of running [[transformExpressionsWithPruning]] on this node
   * and all its children. Note that this method skips expressions inside subqueries.
   */
  def transformAllExpressionsWithPruning(cond: TreePatternBits => Boolean,
    ruleId: RuleId = UnknownRuleId)(rule: PartialFunction[Expression, Expression])
  : this.type = {
    transformWithPruning(cond, ruleId) {
      case q: QueryPlan[_] =>
        q.transformExpressionsWithPruning(cond, ruleId)(rule).asInstanceOf[PlanType]
    }.asInstanceOf[this.type]
  }

  /** Returns all of the expressions present in this query plan operator. */
  final def expressions: Seq[Expression] = _expressions()

  private val _expressions = new BestEffortLazyVal[Seq[Expression]](() => {
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
  })

  /**
   * A variant of `transformUp`, which takes care of the case that the rule replaces a plan node
   * with a new one that has different output expr IDs, by updating the attribute references in
   * the parent nodes accordingly.
   *
   * @param rule the function to transform plan nodes, and return new nodes with attributes mapping
   *             from old attributes to new attributes. The attribute mapping will be used to
   *             rewrite attribute references in the parent nodes.
   * @param skipCond a boolean condition to indicate if we can skip transforming a plan node to save
   *                 time.
   * @param canGetOutput a boolean condition to indicate if we can get the output of a plan node
   *                     to prune the attributes mapping to be propagated. The default value is true
   *                     as only unresolved logical plan can't get output.
   */
  def transformUpWithNewOutput(
      rule: PartialFunction[PlanType, (PlanType, Seq[(Attribute, Attribute)])],
      skipCond: PlanType => Boolean = _ => false,
      canGetOutput: PlanType => Boolean = _ => true): PlanType = {
    def rewrite(plan: PlanType): (PlanType, Seq[(Attribute, Attribute)]) = {
      if (skipCond(plan)) {
        plan -> Nil
      } else {
        val attrMapping = new mutable.ArrayBuffer[(Attribute, Attribute)]()
        var newPlan = plan.mapChildren { child =>
          val (newChild, childAttrMapping) = rewrite(child)
          attrMapping ++= childAttrMapping
          newChild
        }

        plan match {
          case _: ReferenceAllColumns[_] =>
            // It's dangerous to call `references` on an unresolved `ReferenceAllColumns`, and
            // it's unnecessary to rewrite its attributes that all of references come from children

          case _ =>
            val attrMappingForCurrentPlan = attrMapping.filter {
              // The `attrMappingForCurrentPlan` is used to replace the attributes of the
              // current `plan`, so the `oldAttr` must be part of `plan.references`.
              case (oldAttr, _) => plan.references.contains(oldAttr)
            }

            if (attrMappingForCurrentPlan.nonEmpty) {
              assert(!attrMappingForCurrentPlan.groupBy(_._1.exprId)
                .exists(_._2.map(_._2.exprId).distinct.length > 1),
                s"Found duplicate rewrite attributes.\n$plan")

              val attributeRewrites = AttributeMap(attrMappingForCurrentPlan)
              // Using attrMapping from the children plans to rewrite their parent node.
              // Note that we shouldn't rewrite a node using attrMapping from its sibling nodes.
              newPlan = newPlan.rewriteAttrs(attributeRewrites)
            }
        }

        val (planAfterRule, newAttrMapping) = CurrentOrigin.withOrigin(origin) {
          rule.applyOrElse(newPlan, (plan: PlanType) => plan -> Nil)
        }

        val newValidAttrMapping = newAttrMapping.filter {
          case (a1, a2) => a1.exprId != a2.exprId
        }

        // Updates the `attrMapping` entries that are obsoleted by generated entries in `rule`.
        // For example, `attrMapping` has a mapping entry 'id#1 -> id#2' and `rule`
        // generates a new entry 'id#2 -> id#3'. In this case, we need to update
        // the corresponding old entry from 'id#1 -> id#2' to '#id#1 -> #id#3'.
        val updatedAttrMap = AttributeMap(newValidAttrMapping)
        val transferAttrMapping = attrMapping.map {
          case (a1, a2) => (a1, updatedAttrMap.getOrElse(a2, a2))
        }
        val newOtherAttrMapping = {
          val existingAttrMappingSet = transferAttrMapping.map(_._2).toSet
          newValidAttrMapping.filterNot { case (_, a) => existingAttrMappingSet.contains(a) }
        }
        val resultAttrMapping = if (canGetOutput(plan)) {
          // We propagate the attributes mapping to the parent plan node to update attributes, so
          // the `newAttr` must be part of this plan's output.
          (transferAttrMapping ++ newOtherAttrMapping).filter {
            case (_, newAttr) => planAfterRule.outputSet.contains(newAttr)
          }
        } else {
          transferAttrMapping ++ newOtherAttrMapping
        }
        if (!(plan eq planAfterRule)) {
          planAfterRule.copyTagsFrom(plan)
        }
        planAfterRule -> resultAttrMapping.toSeq
      }
    }
    rewrite(this)._1
  }

  def rewriteAttrs(attrMap: AttributeMap[Attribute]): PlanType = {
    transformExpressions {
      case a: AttributeReference =>
        updateAttr(a, attrMap)
      case pe: PlanExpression[PlanType @unchecked] =>
        pe.withNewPlan(updateOuterReferencesInSubquery(pe.plan, attrMap))
    }.asInstanceOf[PlanType]
  }

  private def updateAttr(a: Attribute, attrMap: AttributeMap[Attribute]): Attribute = {
    attrMap.get(a) match {
      case Some(b) =>
        // The new Attribute has to
        // - use a.nullable, because nullability cannot be propagated bottom-up without considering
        //   enclosed operators, e.g., operators such as Filters and Outer Joins can change
        //   nullability;
        // - use b.dataType because transformUpWithNewOutput is used in the Analyzer for resolution,
        //   e.g., WidenSetOperationTypes uses it to propagate types bottom-up.
        AttributeReference(a.name, b.dataType, a.nullable, a.metadata)(b.exprId, a.qualifier)
      case None => a
    }
  }

  /**
   * The outer plan may have old references and the function below updates the
   * outer references to refer to the new attributes.
   */
  protected def updateOuterReferencesInSubquery(
      plan: PlanType,
      attrMap: AttributeMap[Attribute]): PlanType = {
    plan.transformDown { case currentFragment =>
      currentFragment.transformExpressions {
        case OuterReference(a: AttributeReference) =>
          OuterReference(updateAttr(a, attrMap))
        case pe: PlanExpression[PlanType @unchecked] =>
          pe.withNewPlan(updateOuterReferencesInSubquery(pe.plan, attrMap))
      }
    }
  }

  def schema: StructType = _schema()

  private val _schema = new BestEffortLazyVal[StructType](() =>
    DataTypeUtils.fromAttributes(output))

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

  override def simpleStringWithNodeId(): String = {
    val operatorId = Option(QueryPlan.localIdMap.get().get(this)).map(id => s"$id")
      .getOrElse("unknown")
    s"$nodeName ($operatorId)".trim
  }

  def verboseStringWithOperatorId(): String = {
    val argumentString = argString(conf.maxToStringFields)

    if (argumentString.nonEmpty) {
      s"""
         |$formattedNodeName
         |Arguments: $argumentString
         |""".stripMargin
    } else {
      s"""
         |$formattedNodeName
         |""".stripMargin
    }
  }

  protected def formattedNodeName: String = {
    val opId = Option(QueryPlan.localIdMap.get().get(this)).map(id => s"$id")
      .getOrElse("unknown")
    val codegenId =
      getTagValue(QueryPlan.CODEGEN_ID_TAG).map(id => s" [codegen id : $id]").getOrElse("")
    s"($opId) $nodeName$codegenId"
  }

  /**
   * All the top-level subqueries of the current plan node. Nested subqueries are not included.
   */
  def subqueries: Seq[PlanType] = _subqueries()

  private val _subqueries = new TransientBestEffortLazyVal(() =>
    expressions.filter(_.containsPattern(PLAN_EXPRESSION)).flatMap(_.collect {
      case e: PlanExpression[_] => e.plan.asInstanceOf[PlanType]
    })
  )

  /**
   * All the subqueries of the current plan node and all its children. Nested subqueries are also
   * included.
   */
  def subqueriesAll: Seq[PlanType] = {
    val subqueries = this.flatMap(_.subqueries)
    subqueries ++ subqueries.flatMap(_.subqueriesAll)
  }

  /**
   * This method is similar to the transform method, but also applies the given partial function
   * also to all the plans in the subqueries of a node. This method is useful when we want
   * to rewrite the whole plan, include its subqueries, in one go.
   */
  def transformWithSubqueries(f: PartialFunction[PlanType, PlanType]): PlanType =
    transformDownWithSubqueries(f)

  /**
   * Returns a copy of this node where the given partial function has been recursively applied
   * first to the subqueries in this node's children, then this node's children, and finally
   * this node itself (post-order). When the partial function does not apply to a given node,
   * it is left unchanged.
   */
  def transformUpWithSubqueries(f: PartialFunction[PlanType, PlanType]): PlanType = {
    transformUp { case plan =>
      val transformed = plan transformExpressionsUp {
        case planExpression: PlanExpression[PlanType @unchecked] =>
          val newPlan = planExpression.plan.transformUpWithSubqueries(f)
          planExpression.withNewPlan(newPlan)
      }
      f.applyOrElse[PlanType, PlanType](transformed, identity)
    }
  }

  /**
   * This method is the top-down (pre-order) counterpart of transformUpWithSubqueries.
   * Returns a copy of this node where the given partial function has been recursively applied
   * first to this node, then this node's subqueries and finally this node's children.
   * When the partial function does not apply to a given node, it is left unchanged.
   */
  def transformDownWithSubqueries(f: PartialFunction[PlanType, PlanType]): PlanType = {
    transformDownWithSubqueriesAndPruning(AlwaysProcess.fn, UnknownRuleId)(f)
  }

  /**
   * Same as `transformUpWithSubqueries` except allows for pruning opportunities.
   */
  def transformUpWithSubqueriesAndPruning(
    cond: TreePatternBits => Boolean,
    ruleId: RuleId = UnknownRuleId)
    (f: PartialFunction[PlanType, PlanType]): PlanType = {
    val g: PartialFunction[PlanType, PlanType] = new PartialFunction[PlanType, PlanType] {
      override def isDefinedAt(x: PlanType): Boolean = true

      override def apply(plan: PlanType): PlanType = {
        val transformed = plan.transformExpressionsUpWithPruning(t =>
          t.containsPattern(PLAN_EXPRESSION) && cond(t)) {
          case planExpression: PlanExpression[PlanType@unchecked] =>
            val newPlan = planExpression.plan.transformUpWithSubqueriesAndPruning(cond, ruleId)(f)
            planExpression.withNewPlan(newPlan)
        }
        f.applyOrElse[PlanType, PlanType](transformed, identity)
      }
    }

    transformUpWithPruning(cond, ruleId)(g)
  }

  /**
   * This method is the top-down (pre-order) counterpart of transformUpWithSubqueries.
   * Returns a copy of this node where the given partial function has been recursively applied
   * first to this node, then this node's subqueries and finally this node's children.
   * When the partial function does not apply to a given node, it is left unchanged.
   */
  def transformDownWithSubqueriesAndPruning(
      cond: TreePatternBits => Boolean,
      ruleId: RuleId = UnknownRuleId)
      (f: PartialFunction[PlanType, PlanType]): PlanType = {
    val g: PartialFunction[PlanType, PlanType] = new PartialFunction[PlanType, PlanType] {
      override def isDefinedAt(x: PlanType): Boolean = true

      override def apply(plan: PlanType): PlanType = {
        val transformed = f.applyOrElse[PlanType, PlanType](plan, identity)
        transformed transformExpressionsDown {
          case planExpression: PlanExpression[PlanType @unchecked] =>
            val newPlan = planExpression.plan.transformDownWithSubqueriesAndPruning(cond, ruleId)(f)
            planExpression.withNewPlan(newPlan)
        }
      }
    }

    transformDownWithPruning(cond, ruleId)(g)
  }

  /**
   * A variant of [[foreach]] which considers plan nodes inside subqueries as well.
   */
  def foreachWithSubqueries(f: PlanType => Unit): Unit = {
    def actualFunc(plan: PlanType): Unit = {
      f(plan)
      plan.subqueries.foreach(_.foreachWithSubqueries(f))
    }
    foreach(actualFunc)
  }

  /**
   * A variant of `collect`. This method not only apply the given function to all elements in this
   * plan, also considering all the plans in its (nested) subqueries.
   */
  def collectWithSubqueries[B](f: PartialFunction[PlanType, B]): Seq[B] =
    (this +: subqueriesAll).flatMap(_.collect(f))

  /**
   * A variant of collectFirst. This method not only apply the given function to all elements in
   * this plan, also considering all the plans in its (nested) subqueries.
   */
  def collectFirstWithSubqueries[B](f: PartialFunction[PlanType, B]): Option[B] = {
    this.collectFirst(f).orElse {
      subqueriesAll.foldLeft(None: Option[B]) { (l, r) => l.orElse(r.collectFirst(f)) }
    }
  }

  override def innerChildren: Seq[QueryPlan[_]] = subqueries

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
  def canonicalized: PlanType = _canonicalized()

  private val _canonicalized = new TransientBestEffortLazyVal(() => {
    var plan = doCanonicalize()
    // If the plan has not been changed due to canonicalization, make a copy of it so we don't
    // mutate the original plan's _isCanonicalizedPlan flag.
    if (plan eq this) {
      plan = plan.makeCopy(plan.mapProductIterator(x => x.asInstanceOf[AnyRef]))
    }
    plan._isCanonicalizedPlan = true
    plan
  })

  /**
   * Defines how the canonicalization should work for the current plan.
   */
  protected def doCanonicalize(): PlanType = {
    val canonicalizedChildren = children.map(_.canonicalized)
    var id = -1
    val allAttributesSeq = this.allAttributes
    mapExpressions {
      case a: Alias =>
        id += 1
        // As the root of the expression, Alias will always take an arbitrary exprId, we need to
        // normalize that for equality testing, by assigning expr id from 0 incrementally. The
        // alias name doesn't matter and should be erased.
        val normalizedChild = QueryPlan.normalizeExpressions(a.child, allAttributesSeq)
        Alias(normalizedChild, "")(ExprId(id), a.qualifier)

      case ar: AttributeReference if allAttributesSeq.indexOf(ar.exprId) == -1 =>
        // Top level `AttributeReference` may also be used for output like `Alias`, we should
        // normalize the exprId too.
        id += 1
        ar.withExprId(ExprId(id)).canonicalized

      case other => QueryPlan.normalizeExpressions(other, allAttributesSeq)
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
   *
   * `def` instead of a `lazy val` to avoid holding references to a large number of
   * attributes, thereby reducing memory pressure on the driver. The number of attributes
   * referenced here can be very large, especially for logical plans with wide schemas where the
   * column pruning hasn't happened yet. Holding references to all of them can lead to
   * significant memory overhead on the driver.
   */
  def allAttributes: AttributeSeq = children.flatMap(_.output)

  final override def validateNodePatterns(): Unit = {
    if (Utils.isTesting) {
      assert(
        !toPatternBits(nodePatterns: _*).intersects(QueryPlanPatternBitMask.mask),
        s"${this.getClass.getName} returns Expression patterns")
    }
  }
}

object QueryPlanPatternBitMask {
  val mask: BitSet = {
    val bits = new BitSet(TreePattern.maxId)
    bits.clear()
    bits.setUntil(OPERATOR_START.id)
    bits
  }
}

object QueryPlan extends PredicateHelper {
  val CODEGEN_ID_TAG = new TreeNodeTag[Int]("wholeStageCodegenId")

  /**
   * A thread local map to store the mapping between the query plan and the query plan id.
   * The scope of this thread local is within ExplainUtils.processPlan. The reason we define it here
   * is because [[ QueryPlan ]] also needs this, and it doesn't have access to `execution` package
   * from `catalyst`.
   */
  val localIdMap: ThreadLocal[java.util.Map[QueryPlan[_], Int]] = ThreadLocal.withInitial(() =>
    new IdentityHashMap[QueryPlan[_], Int]())

  /**
   * Normalize the exprIds in the given expression, by updating the exprId in `AttributeReference`
   * with its referenced ordinal from input attributes. It's similar to `BindReferences` but we
   * do not use `BindReferences` here as the plan may take the expression as a parameter with type
   * `Attribute`, and replace it with `BoundReference` will cause error.
   */
  def normalizeExpressions[T <: Expression](e: T, input: AttributeSeq): T = {
    e.transformUp {
      case s: PlanExpression[QueryPlan[_] @unchecked] =>
        // Normalize the outer references in the subquery plan.
        val normalizedPlan = s.plan.transformAllExpressionsWithPruning(
          _.containsPattern(OUTER_REFERENCE)) {
          case OuterReference(r) => OuterReference(QueryPlan.normalizeExpressions(r, input))
        }
        s.withNewPlan(normalizedPlan)

      case ar: AttributeReference =>
        val ordinal = input.indexOf(ar.exprId)
        if (ordinal == -1) {
          ar
        } else {
          ar.withExprId(ExprId(ordinal))
        }

      // Top-level Alias is already handled by `QueryPlan#doCanonicalize`. For inner Alias, the id
      // doesn't matter and we normalize it to 0 here.
      case a: Alias =>
        Alias(a.child, a.name)(
          ExprId(0), a.qualifier, a.explicitMetadata, a.nonInheritableMetadataKeys)
    }.canonicalized.asInstanceOf[T]
  }

  /**
   * Composes the given predicates into a conjunctive predicate, which is normalized and reordered.
   * Then returns a new sequence of predicates by splitting the conjunctive predicate.
   */
  def normalizePredicates(predicates: Seq[Expression], output: AttributeSeq): Seq[Expression] = {
    if (predicates.nonEmpty) {
      val normalized = normalizeExpressions(predicates.reduce(And), output)
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
      maxFields: Int = SQLConf.get.maxToStringFields,
      printOperatorId: Boolean = false): Unit = {
    try {
      plan.treeString(append, verbose, addSuffix, maxFields, printOperatorId)
    } catch {
      case e: AnalysisException => append(e.toString)
    }
  }

  /**
   * Generate detailed field string with different format based on type of input value. Supported
   * input values are sequences and strings. An empty sequences converts to []. A non-empty
   * sequences converts to square brackets-enclosed, comma-separated values, prefixed with a
   * sequence length. An empty string converts to None, while a non-empty string is verbatim
   * outputted. In all four cases, user-provided fieldName prefixes the output. Examples:
   * List("Hello", "World") -> <fieldName>: [2]: [Hello, World]
   * List()                 -> <fieldName>: []
   * "hello_world"          -> <fieldName>: hello_world
   * ""                     -> <fieldName>: None
   */
  def generateFieldString(fieldName: String, values: Any): String = values match {
    case iter: Iterable[_] if iter.isEmpty => s"${fieldName}: []"
    case iter: Iterable[_] => s"${fieldName} [${iter.size}]: ${iter.mkString("[", ", ", "]")}"
    case str: String if (str == null || str.isEmpty) => s"${fieldName}: None"
    case str: String => s"${fieldName}: ${str}"
    case _ => throw new IllegalArgumentException(s"Unsupported type for argument values: $values")
  }
}
