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

package org.apache.spark.sql.catalyst.trees

import java.util.{IdentityHashMap, UUID}

import scala.annotation.nowarn
import scala.collection.{mutable, Map}
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.hashing.MurmurHash3

import org.apache.commons.lang3.ClassUtils
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.{AliasIdentifier, CatalystIdentifier}
import org.apache.spark.sql.catalyst.ScalaReflection._
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStorageFormat, CatalogTable, CatalogTableType, FunctionResource}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.TableSpec
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, Partitioning}
import org.apache.spark.sql.catalyst.rules.RuleId
import org.apache.spark.sql.catalyst.rules.RuleIdCollection
import org.apache.spark.sql.catalyst.rules.UnknownRuleId
import org.apache.spark.sql.catalyst.trees.TreePattern.TreePattern
import org.apache.spark.sql.catalyst.util.StringUtils.PlanStringConcat
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.BitSet

/** Used by [[TreeNode.getNodeNumbered]] when traversing the tree for a given number */
private class MutableInt(var i: Int)

// A tag of a `TreeNode`, which defines name and type
// Note: In general, if developers only care about its tagging capabilities,
// then Unit should be considered first before using Boolean.
case class TreeNodeTag[T](name: String)

// A functor that always returns true.
object AlwaysProcess {
  val fn: TreePatternBits => Boolean = { _ => true}
}

// scalastyle:off
abstract class TreeNode[BaseType <: TreeNode[BaseType]]
  extends Product
  with TreePatternBits
  with WithOrigin {
// scalastyle:on
  self: BaseType =>

  override val origin: Origin = CurrentOrigin.get

  /**
   * A mutable map for holding auxiliary information of this tree node. It will be carried over
   * when this node is copied via `makeCopy`, or transformed via `transformUp`/`transformDown`.
   * We lazily evaluate the `tags` since the default size of a `mutable.Map` is nonzero. This
   * will reduce unnecessary memory pressure.
   */
  private[this] var _tags: mutable.Map[TreeNodeTag[_], Any] = null
  private def tags: mutable.Map[TreeNodeTag[_], Any] = {
    if (_tags eq null) {
      _tags = mutable.Map.empty
    }
    _tags
  }

  /**
   * Default tree pattern [[BitSet] for a [[TreeNode]].
   */
  protected def getDefaultTreePatternBits: BitSet = {
    val bits: BitSet = new BitSet(TreePattern.maxId)
    validateNodePatterns()
    // Propagate node pattern bits
    val nodePatternIterator = nodePatterns.iterator
    while (nodePatternIterator.hasNext) {
      bits.set(nodePatternIterator.next().id)
    }
    // Propagate children's pattern bits
    val childIterator = children.iterator
    while (childIterator.hasNext) {
      bits.union(childIterator.next().treePatternBits)
    }
    bits
  }

  /**
   * For child classes to validate `nodePatterns`.
   */
  protected def validateNodePatterns(): Unit = {}

  /**
   * A BitSet of tree patterns for this TreeNode and its subtree. If this TreeNode and its
   * subtree contains a pattern `P`, the corresponding bit for `P.id` is set in this BitSet.
   */
  override lazy val treePatternBits: BitSet = getDefaultTreePatternBits

  /**
   * A BitSet of rule ids to record ineffective rules for this TreeNode and its subtree.
   * If a rule R (which does not read a varying, external state for each invocation) is
   * ineffective in one apply call for this TreeNode and its subtree, R will still be
   * ineffective for subsequent apply calls on this tree because query plan structures are
   * immutable.
   */
  private[this] var _ineffectiveRules: BitSet = null
  private def ineffectiveRules: BitSet = {
    if (_ineffectiveRules eq null) {
      _ineffectiveRules = new BitSet(RuleIdCollection.NumRules)
    }
    _ineffectiveRules
  }
  private def isIneffectiveRulesEmpty = _ineffectiveRules eq null

  /**
   * @return a sequence of tree pattern enums in a TreeNode T. It does not include propagated
   *         patterns in the subtree of T.
   */
  protected val nodePatterns: Seq[TreePattern] = Seq()

  /**
   * Mark that a rule (with id `ruleId`) is ineffective for this TreeNode and its subtree.
   *
   * @param ruleId the unique identifier of the rule. If `ruleId` is UnknownId, it is a no-op.
   */
  protected def markRuleAsIneffective(ruleId : RuleId): Unit = {
    if (ruleId eq UnknownRuleId) {
      return
    }
    ineffectiveRules.set(ruleId.id)
  }

  /**
   * Whether this TreeNode and its subtree have been marked as ineffective for the rule with id
   * `ruleId`.
   *
   * @param ruleId the unique id of the rule
   * @return true if the rule has been marked as ineffective; false otherwise. If `ruleId` is
   *         UnknownId, it returns false.
   */
  protected def isRuleIneffective(ruleId : RuleId): Boolean = {
    if (isIneffectiveRulesEmpty || (ruleId eq UnknownRuleId)) {
      return false
    }
    ineffectiveRules.get(ruleId.id)
  }

  def isTagsEmpty: Boolean = (_tags eq null) || _tags.isEmpty

  def copyTagsFrom(other: BaseType): Unit = {
    // SPARK-32753: it only makes sense to copy tags to a new node
    // but it's too expensive to detect other cases likes node removal
    // so we make a compromise here to copy tags to node with no tags
    if (isTagsEmpty && !other.isTagsEmpty) {
      tags ++= other.tags
    }
  }

  def setTagValue[T](tag: TreeNodeTag[T], value: T): Unit = {
    tags(tag) = value
  }

  def getTagValue[T](tag: TreeNodeTag[T]): Option[T] = {
    if (isTagsEmpty) {
      None
    } else {
      tags.get(tag).map(_.asInstanceOf[T])
    }
  }

  def unsetTagValue[T](tag: TreeNodeTag[T]): Unit = {
    if (!isTagsEmpty) {
      tags -= tag
    }
  }

  /**
   * Returns a Seq of the children of this node.
   * Children should not change. Immutability required for containsChild optimization
   */
  def children: Seq[BaseType]

  lazy val containsChild: Set[TreeNode[_]] = children.toSet

  lazy val height: Int = children.map(_.height).reduceOption(_ max _).getOrElse(0) + 1

  private lazy val _hashCode: Int = MurmurHash3.productHash(this)
  override def hashCode(): Int = _hashCode

  /**
   * Faster version of equality which short-circuits when two treeNodes are the same instance.
   * We don't just override Object.equals, as doing so prevents the scala compiler from
   * generating case class `equals` methods
   */
  def fastEquals(other: TreeNode[_]): Boolean = {
    this.eq(other) || this == other
  }

  /**
   * Find the first [[TreeNode]] that satisfies the condition specified by `f`.
   * The condition is recursively applied to this node and all of its children (pre-order).
   */
  def find(f: BaseType => Boolean): Option[BaseType] = if (f(this)) {
    Some(this)
  } else {
    children.foldLeft(Option.empty[BaseType]) { (l, r) => l.orElse(r.find(f)) }
  }

  /**
   * Test whether there is [[TreeNode]] satisfies the conditions specified in `f`.
   * The condition is recursively applied to this node and all of its children (pre-order).
   */
  def exists(f: BaseType => Boolean): Boolean = if (f(this)) {
    true
  } else {
    children.exists(_.exists(f))
  }

  /**
   * Runs the given function on this node and then recursively on [[children]].
   * @param f the function to be applied to each node in the tree.
   */
  def foreach(f: BaseType => Unit): Unit = {
    f(this)
    children.foreach(_.foreach(f))
  }

  /**
   * Runs the given function recursively on [[children]] then on this node.
   * @param f the function to be applied to each node in the tree.
   */
  def foreachUp(f: BaseType => Unit): Unit = {
    children.foreach(_.foreachUp(f))
    f(this)
  }

  /**
   * Returns a Seq containing the result of applying the given function to each
   * node in this tree in a preorder traversal.
   * @param f the function to be applied.
   */
  def map[A](f: BaseType => A): Seq[A] = {
    val ret = new collection.mutable.ArrayBuffer[A]()
    foreach(ret += f(_))
    ret.toSeq
  }

  /**
   * Returns a Seq by applying a function to all nodes in this tree and using the elements of the
   * resulting collections.
   */
  def flatMap[A](f: BaseType => IterableOnce[A]): Seq[A] = {
    val ret = new collection.mutable.ArrayBuffer[A]()
    foreach(ret ++= f(_))
    ret.toSeq
  }

  /**
   * Returns a Seq containing the result of applying a partial function to all elements in this
   * tree on which the function is defined.
   */
  def collect[B](pf: PartialFunction[BaseType, B]): Seq[B] = {
    val ret = new collection.mutable.ArrayBuffer[B]()
    val lifted = pf.lift
    foreach(node => lifted(node).foreach(ret.+=))
    ret.toSeq
  }

  /**
   * Returns a Seq containing the leaves in this tree.
   */
  def collectLeaves(): Seq[BaseType] = {
    this.collect { case p if p.children.isEmpty => p }
  }

  /**
   * Finds and returns the first [[TreeNode]] of the tree for which the given partial function
   * is defined (pre-order), and applies the partial function to it.
   */
  def collectFirst[B](pf: PartialFunction[BaseType, B]): Option[B] = {
    val lifted = pf.lift
    lifted(this).orElse {
      children.foldLeft(Option.empty[B]) { (l, r) => l.orElse(r.collectFirst(pf)) }
    }
  }

  /**
   * Efficient alternative to `productIterator.map(f).toArray`.
   */
  protected def mapProductIterator[B: ClassTag](f: Any => B): Array[B] = {
    val arr = Array.ofDim[B](productArity)
    var i = 0
    while (i < arr.length) {
      arr(i) = f(productElement(i))
      i += 1
    }
    arr
  }

  private def childrenFastEquals(
      originalChildren: IndexedSeq[BaseType], newChildren: IndexedSeq[BaseType]): Boolean = {
    val size = originalChildren.size
    var i = 0
    while (i < size) {
      if (!originalChildren(i).fastEquals(newChildren(i))) return false
      i += 1
    }
    true
  }

  // This is a temporary solution, we will change the type of children to IndexedSeq in a
  // followup PR
  private def asIndexedSeq(seq: Seq[BaseType]): IndexedSeq[BaseType] = {
    seq match {
      case types: IndexedSeq[BaseType] => types
      case other => other.toIndexedSeq
    }
  }

  final def withNewChildren(newChildren: Seq[BaseType]): BaseType = {
    val childrenIndexedSeq = asIndexedSeq(children)
    val newChildrenIndexedSeq = asIndexedSeq(newChildren)
    assert(newChildrenIndexedSeq.size == childrenIndexedSeq.size, "Incorrect number of children")
    if (childrenIndexedSeq.isEmpty ||
        childrenFastEquals(newChildrenIndexedSeq, childrenIndexedSeq)) {
      this
    } else {
      CurrentOrigin.withOrigin(origin) {
        val res = withNewChildrenInternal(newChildrenIndexedSeq)
        res.copyTagsFrom(this)
        res
      }
    }
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[BaseType]): BaseType

  /**
   * Returns a copy of this node with the children replaced.
   * TODO: Validate somewhere (in debug mode?) that children are ordered correctly.
   */
  protected final def legacyWithNewChildren(newChildren: Seq[BaseType]): BaseType = {
    assert(newChildren.size == children.size, "Incorrect number of children")
    var changed = false
    val remainingNewChildren = newChildren.toBuffer
    val remainingOldChildren = children.toBuffer
    def mapTreeNode(node: TreeNode[_]): TreeNode[_] = {
      val newChild = remainingNewChildren.remove(0)
      val oldChild = remainingOldChildren.remove(0)
      if (newChild fastEquals oldChild) {
        oldChild
      } else {
        changed = true
        newChild
      }
    }
    def mapChild(child: Any): Any = child match {
      case arg: TreeNode[_] if containsChild(arg) => mapTreeNode(arg)
      // CaseWhen Case or any tuple type
      case (left, right) => (mapChild(left), mapChild(right))
      case nonChild: AnyRef => nonChild
      case null => null
    }
    @nowarn("cat=deprecation")
    val newArgs = mapProductIterator {
      case s: StructType => s // Don't convert struct types to some other type of Seq[StructField]
      // Handle Seq[TreeNode] in TreeNode parameters.
      case s: Stream[_] =>
        // Stream is lazy so we need to force materialization
        s.map(mapChild).force
      case l: LazyList[_] =>
        // LazyList is lazy so we need to force materialization
        l.map(mapChild).force
      case s: Seq[_] =>
        s.map(mapChild)
      case m: Map[_, _] =>
        m.toMap.transform((_, v) => mapChild(v))
      case arg: TreeNode[_] if containsChild(arg) => mapTreeNode(arg)
      case Some(child) => Some(mapChild(child))
      case nonChild: AnyRef => nonChild
      case null => null
    }

    if (changed) makeCopy(newArgs) else this
  }

  /**
   * Returns a copy of this node where `rule` has been recursively applied to the tree.
   * When `rule` does not apply to a given node it is left unchanged.
   * Users should not expect a specific directionality. If a specific directionality is needed,
   * transformDown or transformUp should be used.
   *
   * @param rule the function used to transform this nodes children
   */
  def transform(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    transformDown(rule)
  }

  /**
   * Returns a copy of this node where `rule` has been recursively applied to the tree.
   * When `rule` does not apply to a given node it is left unchanged.
   * Users should not expect a specific directionality. If a specific directionality is needed,
   * transformDown or transformUp should be used.
   *
   * @param rule   the function used to transform this nodes children
   * @param cond   a Lambda expression to prune tree traversals. If `cond.apply` returns false
   *               on a TreeNode T, skips processing T and its subtree; otherwise, processes
   *               T and its subtree recursively.
   * @param ruleId is a unique Id for `rule` to prune unnecessary tree traversals. When it is
   *               UnknownRuleId, no pruning happens. Otherwise, if `rule` (with id `ruleId`)
   *               has been marked as in effective on a TreeNode T, skips processing T and its
   *               subtree. Do not pass it if the rule is not purely functional and reads a
   *               varying initial state for different invocations.
   */
  def transformWithPruning(cond: TreePatternBits => Boolean,
    ruleId: RuleId = UnknownRuleId)(rule: PartialFunction[BaseType, BaseType])
  : BaseType = {
    transformDownWithPruning(cond, ruleId)(rule)
  }

  /**
   * Returns a copy of this node where `rule` has been recursively applied to it and all of its
   * children (pre-order). When `rule` does not apply to a given node it is left unchanged.
   *
   * @param rule the function used to transform this nodes children
   */
  def transformDown(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    transformDownWithPruning(AlwaysProcess.fn, UnknownRuleId)(rule)
  }

  /**
   * Returns a copy of this node where `rule` has been recursively applied to it and all of its
   * children (pre-order). When `rule` does not apply to a given node it is left unchanged.
   *
   * @param rule   the function used to transform this nodes children
   * @param cond   a Lambda expression to prune tree traversals. If `cond.apply` returns false
   *               on a TreeNode T, skips processing T and its subtree; otherwise, processes
   *               T and its subtree recursively.
   * @param ruleId is a unique Id for `rule` to prune unnecessary tree traversals. When it is
   *               UnknownRuleId, no pruning happens. Otherwise, if `rule` (with id `ruleId`)
   *               has been marked as in effective on a TreeNode T, skips processing T and its
   *               subtree. Do not pass it if the rule is not purely functional and reads a
   *               varying initial state for different invocations.
   */
  def transformDownWithPruning(cond: TreePatternBits => Boolean,
    ruleId: RuleId = UnknownRuleId)(rule: PartialFunction[BaseType, BaseType])
  : BaseType = {
    if (!cond.apply(this) || isRuleIneffective(ruleId)) {
      return this
    }
    val afterRule = CurrentOrigin.withOrigin(origin) {
      rule.applyOrElse(this, identity[BaseType])
    }

    // Check if unchanged and then possibly return old copy to avoid gc churn.
    if (this fastEquals afterRule) {
      val rewritten_plan = mapChildren(_.transformDownWithPruning(cond, ruleId)(rule))
      if (this eq rewritten_plan) {
        markRuleAsIneffective(ruleId)
        this
      } else {
        rewritten_plan
      }
    } else {
      // If the transform function replaces this node with a new one, carry over the tags.
      afterRule.copyTagsFrom(this)
      afterRule.mapChildren(_.transformDownWithPruning(cond, ruleId)(rule))
    }
  }

  /**
   * Returns a copy of this node where `rule` has been recursively applied first to all of its
   * children and then itself (post-order). When `rule` does not apply to a given node, it is left
   * unchanged.
   *
   * @param rule   the function used to transform this nodes children
   */
  def transformUp(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    transformUpWithPruning(AlwaysProcess.fn, UnknownRuleId)(rule)
  }

  /**
   * Returns a copy of this node where `rule` has been recursively applied first to all of its
   * children and then itself (post-order). When `rule` does not apply to a given node, it is left
   * unchanged.
   *
   * @param rule   the function used to transform this nodes children
   * @param cond   a Lambda expression to prune tree traversals. If `cond.apply` returns false
   *               on a TreeNode T, skips processing T and its subtree; otherwise, processes
   *               T and its subtree recursively.
   * @param ruleId is a unique Id for `rule` to prune unnecessary tree traversals. When it is
   *               UnknownRuleId, no pruning happens. Otherwise, if `rule` (with id `ruleId`)
   *               has been marked as in effective on a TreeNode T, skips processing T and its
   *               subtree. Do not pass it if the rule is not purely functional and reads a
   *               varying initial state for different invocations.
   */
  def transformUpWithPruning(cond: TreePatternBits => Boolean,
    ruleId: RuleId = UnknownRuleId)(rule: PartialFunction[BaseType, BaseType])
  : BaseType = {
    if (!cond.apply(this) || isRuleIneffective(ruleId)) {
      return this
    }
    val afterRuleOnChildren = mapChildren(_.transformUpWithPruning(cond, ruleId)(rule))
    val newNode = if (this fastEquals afterRuleOnChildren) {
      CurrentOrigin.withOrigin(origin) {
        rule.applyOrElse(this, identity[BaseType])
      }
    } else {
      CurrentOrigin.withOrigin(origin) {
        rule.applyOrElse(afterRuleOnChildren, identity[BaseType])
      }
    }
    if (this eq newNode) {
      markRuleAsIneffective(ruleId)
      this
    } else {
      // If the transform function replaces this node with a new one, carry over the tags.
      newNode.copyTagsFrom(this)
      newNode
    }
  }

  /**
   * Returns a copy of this node where `rule` has been recursively applied first to all of its
   * children and then itself (post-order). When `rule` does not apply to a given node, it is left
   * unchanged.
   *
   * @param cond   a Lambda expression to prune tree traversals. If `cond.apply` returns false
   *               on a TreeNode T, skips processing T and its subtree; otherwise, processes
   *               T and its subtree recursively.
   * @param rule   the function use to transform this node and its descendant nodes. The function
   *               takes a tuple as its input, where the first/second field is the before/after
   *               image of applying the rule on the node's children.
   * @param ruleId is a unique Id for `rule` to prune unnecessary tree traversals. When it is
   *               UnknownRuleId, no pruning happens. Otherwise, if `rule` (with id `ruleId`)
   *               has been marked as in effective on a TreeNode T, skips processing T and its
   *               subtree. Do not pass it if the rule is not purely functional and reads a
   *               varying initial state for different invocations.
   */
  def transformUpWithBeforeAndAfterRuleOnChildren(
      cond: BaseType => Boolean, ruleId: RuleId = UnknownRuleId)(
    rule: PartialFunction[(BaseType, BaseType), BaseType]): BaseType = {
    if (!cond.apply(this) || isRuleIneffective(ruleId)) {
      return this
    }
    val afterRuleOnChildren =
      mapChildren(_.transformUpWithBeforeAndAfterRuleOnChildren(cond, ruleId)(rule))
    val newNode = CurrentOrigin.withOrigin(origin) {
      rule.applyOrElse((this, afterRuleOnChildren), { t: (BaseType, BaseType) => t._2 })
    }
    if (this eq newNode) {
      this.markRuleAsIneffective(ruleId)
      this
    } else {
      // If the transform function replaces this node with a new one, carry over the tags.
      newNode.copyTagsFrom(this)
      newNode
    }
  }

  /**
   * Returns alternative copies of this node where `rule` has been recursively applied to it and all
   * of its children (pre-order).
   *
   * @param rule a function used to generate alternatives for a node
   * @return     the stream of alternatives
   */
  def multiTransformDown(
      rule: PartialFunction[BaseType, Seq[BaseType]]): LazyList[BaseType] = {
    multiTransformDownWithPruning(AlwaysProcess.fn, UnknownRuleId)(rule)
  }

  /**
   * Returns alternative copies of this node where `rule` has been recursively applied to it and all
   * of its children (pre-order).
   *
   * As it is very easy to generate enormous number of alternatives when the input tree is huge or
   * when the rule returns many alternatives for many nodes, this function returns the alternatives
   * as a lazy `LazyList` to be able to limit the number of alternatives generated at the caller
   * side as needed.
   *
   * The purpose of this function to access the returned alternatives by the rule only if they are
   * needed so the rule can return a `LazyList` whose elements are also lazily calculated.
   * E.g. `multiTransform*` calls can be nested with the help of
   * `MultiTransform.generateCartesianProduct()`.
   *
   * The rule should not apply or can return a one element `Seq` of original node to indicate that
   * the original node without any transformation is a valid alternative.
   *
   * The rule can return `Seq.empty` to indicate that the original node should be pruned. In this
   * case `multiTransform()` returns an empty `LazyList`.
   *
   * Please consider the following examples of `input.multiTransformDown(rule)`:
   *
   * We have an input expression:
   *    `Add(a, b)`
   *
   * 1.
   * We have a simple rule:
   *   `a` => `Seq(1, 2)`
   *   `b` => `Seq(10, 20)`
   *   `Add(a, b)` => `Seq(11, 12, 21, 22)`
   *
   * The output is:
   *   `LazyList(11, 12, 21, 22)`
   *
   * 2.
   * In the previous example if we want to generate alternatives of `a` and `b` too then we need to
   * explicitly add the original `Add(a, b)` expression to the rule:
   *   `a` => `Seq(1, 2)`
   *   `b` => `Seq(10, 20)`
   *   `Add(a, b)` => `Seq(11, 12, 21, 22, Add(a, b))`
   *
   * The output is:
   *   `LazyList(11, 12, 21, 22, Add(1, 10), Add(2, 10), Add(1, 20), Add(2, 20))`
   *
   * @param rule   a function used to generate alternatives for a node
   * @param cond   a Lambda expression to prune tree traversals. If `cond.apply` returns false
   *               on a TreeNode T, skips processing T and its subtree; otherwise, processes
   *               T and its subtree recursively.
   * @param ruleId is a unique Id for `rule` to prune unnecessary tree traversals. When it is
   *               UnknownRuleId, no pruning happens. Otherwise, if `rule` (with id `ruleId`)
   *               has been marked as in effective on a TreeNode T, skips processing T and its
   *               subtree. Do not pass it if the rule is not purely functional and reads a
   *               varying initial state for different invocations.
   * @return       the stream of alternatives
   */
  def multiTransformDownWithPruning(
      cond: TreePatternBits => Boolean,
      ruleId: RuleId = UnknownRuleId
    )(rule: PartialFunction[BaseType, Seq[BaseType]]): LazyList[BaseType] = {
    if (!cond.apply(this) || isRuleIneffective(ruleId)) {
      return LazyList(this)
    }

    // We could return `Seq(this)` if the `rule` doesn't apply and handle both
    // - the doesn't apply
    // - and the rule returns a one element `Seq(originalNode)`
    // cases together. The returned `Seq` can be a `LazyList` and unfortunately it doesn't seem like
    // there is a way to match on a one element stream without eagerly computing the tail's head.
    // This contradicts with the purpose of only taking the necessary elements from the
    // alternatives. I.e. the "multiTransformDown is lazy" test case in `TreeNodeSuite` would fail.
    // Please note that this behaviour has a downside as well that we can only mark the rule on the
    // original node ineffective if the rule didn't match.
    var ruleApplied = true
    val afterRules = CurrentOrigin.withOrigin(origin) {
      rule.applyOrElse(this, (_: BaseType) => {
        ruleApplied = false
        Seq.empty
      })
    }

    val afterRulesLazyList = if (afterRules.isEmpty) {
      if (ruleApplied) {
        // If the rule returned with empty alternatives then prune
        LazyList.empty
      } else {
        // If the rule was not applied then keep the original node
        this.markRuleAsIneffective(ruleId)
        LazyList(this)
      }
    } else {
      // If the rule was applied then use the returned alternatives
      afterRules.to(LazyList).map { afterRule =>
        if (this fastEquals afterRule) {
          this
        } else {
          afterRule.copyTagsFrom(this)
          afterRule
        }
      }
    }

    afterRulesLazyList.flatMap { afterRule =>
      if (afterRule.containsChild.nonEmpty) {
        MultiTransform.generateCartesianProduct(
            afterRule.children.map(c => () => c.multiTransformDownWithPruning(cond, ruleId)(rule)))
          .map(afterRule.withNewChildren)
      } else {
        LazyList(afterRule)
      }
    }
  }

  /**
   * Returns a copy of this node where `f` has been applied to all the nodes in `children`.
   */
  def mapChildren(f: BaseType => BaseType): BaseType = {
    if (containsChild.nonEmpty) {
      withNewChildren(children.map(f))
    } else {
      this
    }
  }

  /**
   * Args to the constructor that should be copied, but not transformed.
   * These are appended to the transformed args automatically by makeCopy
   * @return
   */
  protected def otherCopyArgs: Seq[AnyRef] = Nil

  /**
   * Creates a copy of this type of tree node after a transformation.
   * Must be overridden by child classes that have constructor arguments
   * that are not present in the productIterator.
   * @param newArgs the new product arguments.
   */
  def makeCopy(newArgs: Array[AnyRef]): BaseType = makeCopy(newArgs, allowEmptyArgs = false)

  /**
   * Creates a copy of this type of tree node after a transformation.
   * Must be overridden by child classes that have constructor arguments
   * that are not present in the productIterator.
   * @param newArgs the new product arguments.
   * @param allowEmptyArgs whether to allow argument list to be empty.
   */
  private def makeCopy(
      newArgs: Array[AnyRef],
      allowEmptyArgs: Boolean): BaseType = {
    val allCtors = getClass.getConstructors
    if (newArgs.isEmpty && allCtors.isEmpty) {
      // This is a singleton object which doesn't have any constructor. Just return `this` as we
      // can't copy it.
      return this
    }

    // Skip no-arg constructors that are just there for kryo.
    val ctors = allCtors.filter(allowEmptyArgs || _.getParameterCount != 0)
    if (ctors.isEmpty) {
      throw QueryExecutionErrors.constructorNotFoundError(nodeName)
    }
    val allArgs: Array[AnyRef] = if (otherCopyArgs.isEmpty) {
      newArgs
    } else {
      newArgs ++ otherCopyArgs
    }
    val defaultCtor = ctors.find { ctor =>
      if (ctor.getParameterCount != allArgs.length) {
        false
      } else if (allArgs.contains(null)) {
        // if there is a `null`, we can't figure out the class, therefore we should just fallback
        // to older heuristic
        false
      } else {
        val argsArray: Array[Class[_]] = allArgs.map(_.getClass)
        ClassUtils.isAssignable(argsArray, ctor.getParameterTypes, true /* autoboxing */)
      }
    }.getOrElse(ctors.maxBy(_.getParameterCount)) // fall back to older heuristic

    try {
      CurrentOrigin.withOrigin(origin) {
        val res = defaultCtor.newInstance(allArgs.toArray: _*).asInstanceOf[BaseType]
        res.copyTagsFrom(this)
        res
      }
    } catch {
      case e: java.lang.IllegalArgumentException =>
        throw SparkException.internalError(
          s"""
             |Failed to copy node.
             |Is otherCopyArgs specified correctly for $nodeName.
             |Exception message: ${e.getMessage}
             |ctor: $defaultCtor?
             |types: ${newArgs.map(_.getClass).mkString(", ")}
             |args: ${newArgs.mkString(", ")}
           """.stripMargin)
    }
  }

  override def clone(): BaseType = {
    def mapChild(child: Any): Any = child match {
      case arg: TreeNode[_] if containsChild(arg) =>
        arg.asInstanceOf[BaseType].clone()
      case (arg1: TreeNode[_], arg2: TreeNode[_]) =>
        val newChild1 = if (containsChild(arg1)) {
          arg1.asInstanceOf[BaseType].clone()
        } else {
          arg1.asInstanceOf[BaseType]
        }

        val newChild2 = if (containsChild(arg2)) {
          arg2.asInstanceOf[BaseType].clone()
        } else {
          arg2.asInstanceOf[BaseType]
        }
        (newChild1, newChild2)
      case other => other
    }

    @nowarn("cat=deprecation")
    val newArgs = mapProductIterator {
      case arg: TreeNode[_] if containsChild(arg) =>
        arg.asInstanceOf[BaseType].clone()
      case Some(arg: TreeNode[_]) if containsChild(arg) =>
        Some(arg.asInstanceOf[BaseType].clone())
      // `mapValues` is lazy and we need to force it to materialize by converting to Map
      case m: Map[_, _] => m.toMap.transform {
        case (_, arg: TreeNode[_]) if containsChild(arg) =>
          arg.asInstanceOf[BaseType].clone()
        case (_, other) => other
      }
      case d: DataType => d // Avoid unpacking Structs
      case args: Stream[_] => args.map(mapChild).force // Force materialization on stream
      case args: LazyList[_] => args.map(mapChild).force // Force materialization on LazyList
      case args: Iterable[_] => args.map(mapChild)
      case nonChild: AnyRef => nonChild
      case null => null
    }
    makeCopy(newArgs, allowEmptyArgs = true)
  }

  private def simpleClassName: String = Utils.getSimpleName(this.getClass)

  /**
   * Returns the name of this type of TreeNode.  Defaults to the class name.
   * Note that we remove the "Exec" suffix for physical operators here.
   */
  def nodeName: String = simpleClassName.replaceAll("Exec$", "")

  /**
   * The arguments that should be included in the arg string.  Defaults to the `productIterator`.
   */
  protected def stringArgs: Iterator[Any] = productIterator

  private lazy val allChildren: IdentityHashMap[TreeNode[_], Any] = {
    val set = new IdentityHashMap[TreeNode[_], Any]()
    (children ++ innerChildren).foreach {
      set.put(_, null)
    }
    set
  }

  private def redactMapString[K, V](map: Map[K, V], maxFields: Int): List[String] = {
    // For security reason, redact the map value if the key is in certain patterns
    val redactedMap = SQLConf.get.redactOptions(map.toMap)
    // construct the redacted map as strings of the format "key=value"
    val keyValuePairs = redactedMap.toSeq.map { item =>
      s"${item._1}=${item._2}"
    }
    truncatedString(keyValuePairs, "[", ", ", "]", maxFields) :: Nil
  }

  private def formatArg(arg: Any, maxFields: Int): String = arg match {
    case seq: Seq[_] =>
      truncatedString(seq.map(formatArg(_, maxFields)), "[", ", ", "]", maxFields)
    case set: Set[_] =>
      // Sort elements for deterministic behaviours
      truncatedString(set.toSeq.map(formatArg(_, maxFields)).sorted, "{", ", ", "}", maxFields)
    case array: Array[_] =>
      truncatedString(
        array.map(formatArg(_, maxFields)).toImmutableArraySeq, "[", ", ", "]", maxFields)
    case other =>
      other.toString
  }

  /** Returns a string representing the arguments to this node, minus any children */
  def argString(maxFields: Int): String = stringArgs.flatMap {
    case tn: TreeNode[_] if allChildren.containsKey(tn) => Nil
    case Some(tn: TreeNode[_]) if allChildren.containsKey(tn) => Nil
    case Some(tn: TreeNode[_]) => tn.simpleString(maxFields) :: Nil
    case tn: TreeNode[_] => tn.simpleString(maxFields) :: Nil
    case seq: Seq[Any] if seq.forall(allChildren.containsKey) => Nil
    case iter: Iterable[_] if iter.isEmpty => Nil
    case array: Array[_] if array.isEmpty => Nil
    case xs @ (_: Seq[_] | _: Set[_] | _: Array[_]) =>
      formatArg(xs, maxFields) :: Nil
    case null => Nil
    case None => Nil
    case Some(null) => Nil
    case Some(table: CatalogTable) =>
      stringArgsForCatalogTable(table)
    case Some(any) => any :: Nil
    case map: CaseInsensitiveStringMap =>
      redactMapString(map.asCaseSensitiveMap().asScala, maxFields)
    case map: Map[_, _] =>
      redactMapString(map, maxFields)
    case t: TableSpec =>
      t.copy(properties = Utils.redact(t.properties).toMap,
        options = Utils.redact(t.options).toMap) :: Nil
    case table: CatalogTable =>
      stringArgsForCatalogTable(table)

    case other => other :: Nil
  }.mkString(", ")

  private def stringArgsForCatalogTable(table: CatalogTable): Seq[Any] = {
    table.storage.serde match {
      case Some(serde)
        // SPARK-39564: don't print out serde to avoid introducing complicated and error-prone
        // regex magic.
        if !SQLConf.get.getConfString("spark.test.noSerdeInExplain", "false").toBoolean =>
        table.identifier :: serde :: Nil
      case _ => table.identifier :: Nil
    }
  }

  /**
   * ONE line description of this node.
   * @param maxFields Maximum number of fields that will be converted to strings.
   *                  Any elements beyond the limit will be dropped.
   */
  def simpleString(maxFields: Int): String = s"$nodeName ${argString(maxFields)}".trim

  /**
   * ONE line description of this node containing the node identifier.
   * @return
   */
  def simpleStringWithNodeId(): String

  /** ONE line description of this node with more information */
  def verboseString(maxFields: Int): String

  /** ONE line description of this node with some suffix information */
  def verboseStringWithSuffix(maxFields: Int): String = verboseString(maxFields)

  override def toString: String = treeString

  /** Returns a string representation of the nodes in this tree */
  final def treeString: String = treeString(verbose = true)

  final def treeString(
      verbose: Boolean,
      addSuffix: Boolean = false,
      maxFields: Int = SQLConf.get.maxToStringFields,
      printOperatorId: Boolean = false): String = {
    val concat = new PlanStringConcat()
    treeString(concat.append, verbose, addSuffix, maxFields, printOperatorId)
    concat.toString
  }

  def treeString(
      append: String => Unit,
      verbose: Boolean,
      addSuffix: Boolean,
      maxFields: Int,
      printOperatorId: Boolean): Unit = {
    generateTreeString(0, new java.util.ArrayList(), append, verbose, "", addSuffix, maxFields,
      printOperatorId, 0)
  }

  /**
   * Returns a string representation of the nodes in this tree, where each operator is numbered.
   * The numbers can be used with [[TreeNode.apply]] to easily access specific subtrees.
   *
   * The numbers are based on depth-first traversal of the tree (with innerChildren traversed first
   * before children).
   */
  def numberedTreeString: String =
    treeString.split("\n").zipWithIndex.map { case (line, i) => f"$i%02d $line" }.mkString("\n")

  /**
   * Returns the tree node at the specified number, used primarily for interactive debugging.
   * Numbers for each node can be found in the [[numberedTreeString]].
   *
   * Note that this cannot return BaseType because logical plan's plan node might return
   * physical plan for innerChildren, e.g. in-memory relation logical plan node has a reference
   * to the physical plan node it is referencing.
   */
  def apply(number: Int): TreeNode[_] = getNodeNumbered(new MutableInt(number)).orNull

  /**
   * Returns the tree node at the specified number, used primarily for interactive debugging.
   * Numbers for each node can be found in the [[numberedTreeString]].
   *
   * This is a variant of [[apply]] that returns the node as BaseType (if the type matches).
   */
  def p(number: Int): BaseType = apply(number).asInstanceOf[BaseType]

  private def getNodeNumbered(number: MutableInt): Option[TreeNode[_]] = {
    if (number.i < 0) {
      None
    } else if (number.i == 0) {
      Some(this)
    } else {
      number.i -= 1
      // Note that this traversal order must be the same as numberedTreeString.
      innerChildren.map(_.getNodeNumbered(number)).find(_ != None).getOrElse {
        children.map(_.getNodeNumbered(number)).find(_ != None).flatten
      }
    }
  }

  /**
   * All the nodes that should be shown as a inner nested tree of this node.
   * For example, this can be used to show sub-queries.
   */
  def innerChildren: Seq[TreeNode[_]] = Seq.empty

  /**
   * Appends the string representation of this node and its children to the given Writer.
   *
   * The `i`-th element in `lastChildren` indicates whether the ancestor of the current node at
   * depth `i + 1` is the last child of its own parent node.  The depth of the root node is 0, and
   * `lastChildren` for the root node should be empty.
   *
   * Note that this traversal (numbering) order must be the same as [[getNodeNumbered]].
   */
  def generateTreeString(
      depth: Int,
      lastChildren: java.util.ArrayList[Boolean],
      append: String => Unit,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false,
      maxFields: Int,
      printNodeId: Boolean,
      indent: Int = 0): Unit = {
    (0 until indent).foreach(_ => append("   "))
    if (depth > 0) {
      val iter = lastChildren.iterator
      (0 until lastChildren.size - 1).foreach { i =>
        val isLast = iter.next
        append(if (isLast) "   " else ":  ")
      }
      append(if (lastChildren.get(lastChildren.size() - 1)) "+- " else ":- ")
    }

    val str = if (verbose) {
      if (addSuffix) verboseStringWithSuffix(maxFields) else verboseString(maxFields)
    } else if (printNodeId) {
      simpleStringWithNodeId()
    } else {
      simpleString(maxFields)
    }
    append(prefix)
    append(str)
    append("\n")

    val innerChildrenLocal = innerChildren
    if (innerChildrenLocal.nonEmpty) {
      lastChildren.add(children.isEmpty)
      lastChildren.add(false)
      innerChildrenLocal.init.foreach(_.generateTreeString(
        depth + 2, lastChildren, append, verbose,
        addSuffix = addSuffix, maxFields = maxFields, printNodeId = printNodeId, indent = indent))
      lastChildren.remove(lastChildren.size() - 1)
      lastChildren.remove(lastChildren.size() - 1)

      lastChildren.add(children.isEmpty)
      lastChildren.add(true)
      innerChildrenLocal.last.generateTreeString(
        depth + 2, lastChildren, append, verbose,
        addSuffix = addSuffix, maxFields = maxFields, printNodeId = printNodeId, indent = indent)
      lastChildren.remove(lastChildren.size() - 1)
      lastChildren.remove(lastChildren.size() - 1)
    }

    if (children.nonEmpty) {
      lastChildren.add(false)
      children.init.foreach(_.generateTreeString(
        depth + 1, lastChildren, append, verbose, prefix, addSuffix,
        maxFields, printNodeId = printNodeId, indent = indent)
      )
      lastChildren.remove(lastChildren.size() - 1)

      lastChildren.add(true)
      children.last.generateTreeString(
        depth + 1, lastChildren, append, verbose, prefix,
        addSuffix, maxFields, printNodeId = printNodeId, indent = indent)
      lastChildren.remove(lastChildren.size() - 1)
    }
  }

  /**
   * Returns a 'scala code' representation of this `TreeNode` and its children.  Intended for use
   * when debugging where the prettier toString function is obfuscating the actual structure. In the
   * case of 'pure' `TreeNodes` that only contain primitives and other TreeNodes, the result can be
   * pasted in the REPL to build an equivalent Tree.
   */
  def asCode: String = {
    val args = productIterator.map {
      case tn: TreeNode[_] => tn.asCode
      case s: String => "\"" + s + "\""
      case other => other.toString
    }
    s"$nodeName(${args.mkString(",")})"
  }

  def toJSON: String = compact(render(jsonValue))

  def prettyJson: String = pretty(render(jsonValue))

  private def jsonValue: JValue = {
    val jsonValues = scala.collection.mutable.ArrayBuffer.empty[JValue]

    def collectJsonValue(tn: BaseType): Unit = {
      val jsonFields = ("class" -> JString(tn.getClass.getName)) ::
        ("num-children" -> JInt(tn.children.length)) :: tn.jsonFields
      jsonValues += JObject(jsonFields)
      tn.children.foreach(collectJsonValue)
    }

    collectJsonValue(this)
    jsonValues
  }

  protected def jsonFields: List[JField] = {
    val fieldNames = getConstructorParameterNames(getClass)
    val fieldValues = productIterator.toSeq ++ otherCopyArgs
    assert(fieldNames.length == fieldValues.length, s"$simpleClassName fields: " +
      fieldNames.mkString(", ") + s", values: " + fieldValues.mkString(", "))

    fieldNames.zip(fieldValues).map {
      // If the field value is a child, then use an int to encode it, represents the index of
      // this child in all children.
      case (name, value: TreeNode[_]) if containsChild(value) =>
        name -> JInt(children.indexOf(value))
      case (name, value: Seq[BaseType @unchecked]) if value.forall(containsChild) =>
        name -> JArray(
          value.map(v => JInt(children.indexOf(v.asInstanceOf[TreeNode[_]]))).toList
        )
      case (name, value) => name -> parseToJson(value)
    }.toList
  }

  private def parseToJson(obj: Any): JValue = obj match {
    case b: Boolean => JBool(b)
    case b: Byte => JInt(b.toInt)
    case s: Short => JInt(s.toInt)
    case i: Int => JInt(i)
    case l: Long => JInt(l)
    case f: Float => JDouble(f)
    case d: Double => JDouble(d)
    case b: BigInt => JInt(b)
    case null => JNull
    case s: String => JString(s)
    case u: UUID => JString(u.toString)
    case dt: DataType => dt.jsonValue
    // SPARK-17356: In usage of mllib, Metadata may store a huge vector of data, transforming
    // it to JSON may trigger OutOfMemoryError.
    case m: Metadata => Metadata.empty.jsonValue
    case clazz: Class[_] => JString(clazz.getName)
    case s: StorageLevel =>
      ("useDisk" -> s.useDisk) ~ ("useMemory" -> s.useMemory) ~ ("useOffHeap" -> s.useOffHeap) ~
        ("deserialized" -> s.deserialized) ~ ("replication" -> s.replication)
    case n: TreeNode[_] => n.jsonValue
    case o: Option[_] => o.map(parseToJson)
    // Recursive scan Seq[Partitioning], Seq[DataType], Seq[Product]
    case t: Seq[_] if t.forall(_.isInstanceOf[Partitioning]) ||
      t.forall(_.isInstanceOf[DataType]) ||
      t.forall(_.isInstanceOf[Product]) =>
      JArray(t.map(parseToJson).toList)
    case t: Seq[_] if t.length > 0 && t.head.isInstanceOf[String] =>
      JString(truncatedString(t, "[", ", ", "]", SQLConf.get.maxToStringFields))
    case t: Seq[_] => JNull
    case m: Map[_, _] => JNull
    // if it's a scala object, we can simply keep the full class path.
    // TODO: currently if the class name ends with "$", we think it's a scala object, there is
    // probably a better way to check it.
    case obj if obj.getClass.getName.endsWith("$") => "object" -> obj.getClass.getName
    case p: Product if shouldConvertToJson(p) =>
      try {
        val fieldNames = getConstructorParameterNames(p.getClass)
        val fieldValues = {
          if (p.productArity == fieldNames.length) {
            p.productIterator.toSeq
          } else {
            val clazz = p.getClass
            // Fallback to use reflection if length of product elements do not match
            // constructor params.
            fieldNames.map { fieldName =>
              val field = clazz.getDeclaredField(fieldName)
              field.setAccessible(true)
              field.get(p)
            }
          }
        }
        assert(fieldNames.length == fieldValues.length, s"$simpleClassName fields: " +
          fieldNames.mkString(", ") + s", values: " + fieldValues.mkString(", "))
        ("product-class" -> JString(p.getClass.getName)) :: fieldNames.zip(fieldValues).map {
          case (name, value) => name -> parseToJson(value)
        }.toList
      } catch {
        case _: RuntimeException => null
        case _: ReflectiveOperationException => null
      }
    case _ => JNull
  }

  private def shouldConvertToJson(product: Product): Boolean = product match {
    case exprId: ExprId => true
    case field: StructField => true
    case id: CatalystIdentifier => true
    case alias: AliasIdentifier => true
    case join: JoinType => true
    case spec: BucketSpec => true
    case catalog: CatalogTable => true
    case partition: Partitioning => true
    case resource: FunctionResource => true
    case broadcast: BroadcastMode => true
    case table: CatalogTableType => true
    case storage: CatalogStorageFormat => true
    // Write out product that contains TreeNode, since there are some Tuples such as cteRelations
    // in With, branches in CaseWhen which are essential to understand the plan.
    case p if p.productIterator.exists(_.isInstanceOf[TreeNode[_]]) => true
    case _ => false
  }
}

trait LeafLike[T <: TreeNode[T]] { self: TreeNode[T] =>
  override final def children: Seq[T] = Nil
  override final def mapChildren(f: T => T): T = this.asInstanceOf[T]
  // Stateful expressions should override this method to return a new instance.
  override def withNewChildrenInternal(newChildren: IndexedSeq[T]): T = this.asInstanceOf[T]
}

trait UnaryLike[T <: TreeNode[T]] { self: TreeNode[T] =>
  def child: T
  @transient override final lazy val children: Seq[T] = IndexedSeq(child)

  override final def mapChildren(f: T => T): T = {
    val newChild = f(child)
    if (newChild fastEquals child) {
      this.asInstanceOf[T]
    } else {
      CurrentOrigin.withOrigin(origin) {
        val res = withNewChildInternal(newChild)
        res.copyTagsFrom(this.asInstanceOf[T])
        res
      }
    }
  }

  override final def withNewChildrenInternal(newChildren: IndexedSeq[T]): T = {
    assert(newChildren.size == 1, "Incorrect number of children")
    withNewChildInternal(newChildren.head)
  }

  protected def withNewChildInternal(newChild: T): T
}

trait BinaryLike[T <: TreeNode[T]] { self: TreeNode[T] =>
  def left: T
  def right: T
  @transient override final lazy val children: Seq[T] = IndexedSeq(left, right)

  override final def mapChildren(f: T => T): T = {
    var newLeft = f(left)
    newLeft = if (newLeft fastEquals left) left else newLeft
    var newRight = f(right)
    newRight = if (newRight fastEquals right) right else newRight

    if (newLeft.eq(left) && newRight.eq(right)) {
      this.asInstanceOf[T]
    } else {
      CurrentOrigin.withOrigin(origin) {
        val res = withNewChildrenInternal(newLeft, newRight)
        res.copyTagsFrom(this.asInstanceOf[T])
        res
      }
    }
  }

  override final def withNewChildrenInternal(newChildren: IndexedSeq[T]): T = {
    assert(newChildren.size == 2, "Incorrect number of children")
    withNewChildrenInternal(newChildren(0), newChildren(1))
  }

  protected def withNewChildrenInternal(newLeft: T, newRight: T): T
}

trait TernaryLike[T <: TreeNode[T]] { self: TreeNode[T] =>
  def first: T
  def second: T
  def third: T
  @transient override final lazy val children: Seq[T] = IndexedSeq(first, second, third)

  override final def mapChildren(f: T => T): T = {
    var newFirst = f(first)
    newFirst = if (newFirst fastEquals first) first else newFirst
    var newSecond = f(second)
    newSecond = if (newSecond fastEquals second) second else newSecond
    var newThird = f(third)
    newThird = if (newThird fastEquals third) third else newThird

    if (newFirst.eq(first) && newSecond.eq(second) && newThird.eq(third)) {
      this.asInstanceOf[T]
    } else {
      CurrentOrigin.withOrigin(origin) {
        val res = withNewChildrenInternal(newFirst, newSecond, newThird)
        res.copyTagsFrom(this.asInstanceOf[T])
        res
      }
    }
  }

  override final def withNewChildrenInternal(newChildren: IndexedSeq[T]): T = {
    assert(newChildren.size == 3, "Incorrect number of children")
    withNewChildrenInternal(newChildren(0), newChildren(1), newChildren(2))
  }

  protected def withNewChildrenInternal(newFirst: T, newSecond: T, newThird: T): T
}

trait QuaternaryLike[T <: TreeNode[T]] { self: TreeNode[T] =>
  def first: T
  def second: T
  def third: T
  def fourth: T
  @transient override final lazy val children: Seq[T] = IndexedSeq(first, second, third, fourth)

  override final def mapChildren(f: T => T): T = {
    var newFirst = f(first)
    newFirst = if (newFirst fastEquals first) first else newFirst
    var newSecond = f(second)
    newSecond = if (newSecond fastEquals second) second else newSecond
    var newThird = f(third)
    newThird = if (newThird fastEquals third) third else newThird
    var newFourth = f(fourth)
    newFourth = if (newFourth fastEquals fourth) fourth else newFourth

    if (newFirst.eq(first) && newSecond.eq(second) && newThird.eq(third) && newFourth.eq(fourth)) {
      this.asInstanceOf[T]
    } else {
      CurrentOrigin.withOrigin(origin) {
        val res = withNewChildrenInternal(newFirst, newSecond, newThird, newFourth)
        res.copyTagsFrom(this.asInstanceOf[T])
        res
      }
    }
  }

  override final def withNewChildrenInternal(newChildren: IndexedSeq[T]): T = {
    assert(newChildren.size == 4, "Incorrect number of children")
    withNewChildrenInternal(newChildren(0), newChildren(1), newChildren(2), newChildren(3))
  }

  protected def withNewChildrenInternal(newFirst: T, newSecond: T, newThird: T, newFourth: T): T
}

object MultiTransform {

  /**
   * Returns the stream of `Seq` elements by generating the cartesian product of sequences.
   *
   * @param elementSeqs a list of sequences to build the cartesian product from
   * @return            the stream of generated `Seq` elements
   */
  def generateCartesianProduct[T](elementSeqs: Seq[() => Seq[T]]): LazyList[Seq[T]] = {
    elementSeqs.foldRight(LazyList(Seq.empty[T]))((elements, elementTails) =>
      for {
        elementTail <- elementTails
        element <- elements()
      } yield element +: elementTail
    )
  }
}
