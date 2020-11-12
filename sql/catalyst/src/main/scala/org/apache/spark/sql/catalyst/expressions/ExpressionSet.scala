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

import scala.collection.{mutable, GenTraversableOnce}
import scala.collection.mutable.ArrayBuffer

object ExpressionSet {
  /** Constructs a new [[ExpressionSet]] by applying [[Canonicalize]] to `expressions`. */
  def apply(expressions: TraversableOnce[Expression]): ExpressionSet = {
    val set = new ExpressionSet()
    expressions.foreach(set.add)
    set
  }

  def apply(): ExpressionSet = {
    new ExpressionSet()
  }
}

/**
 * A [[Set]] where membership is determined based on determinacy and a canonical representation of
 * an [[Expression]] (i.e. one that attempts to ignore cosmetic differences).
 * See [[Canonicalize]] for more details.
 *
 * Internally this set uses the canonical representation, but keeps also track of the original
 * expressions to ease debugging.  Since different expressions can share the same canonical
 * representation, this means that operations that extract expressions from this set are only
 * guaranteed to see at least one such expression.  For example:
 *
 * {{{
 *   val set = ExpressionSet(a + 1, 1 + a)
 *
 *   set.iterator => Iterator(a + 1)
 *   set.contains(a + 1) => true
 *   set.contains(1 + a) => true
 *   set.contains(a + 2) => false
 * }}}
 *
 * For non-deterministic expressions, they are always considered as not contained in the [[Set]].
 * On adding a non-deterministic expression, simply append it to the original expressions.
 * This is consistent with how we define `semanticEquals` between two expressions.
 */
class ExpressionSet protected(
    private val baseSet: mutable.Set[Expression] = new mutable.HashSet,
    private val originals: mutable.Buffer[Expression] = new ArrayBuffer)
  extends Iterable[Expression] {

  //  Note: this class supports Scala 2.12. A parallel source tree has a 2.13 implementation.

  protected def add(e: Expression): Unit = {
    if (!e.deterministic) {
      originals += e
    } else if (!baseSet.contains(e.canonicalized)) {
      baseSet.add(e.canonicalized)
      originals += e
    }
  }

  protected def remove(e: Expression): Unit = {
    if (e.deterministic) {
      baseSet --= baseSet.filter(_ == e.canonicalized)
      originals --= originals.filter(_.canonicalized == e.canonicalized)
    }
  }

  def contains(elem: Expression): Boolean = baseSet.contains(elem.canonicalized)

  override def filter(p: Expression => Boolean): ExpressionSet = {
    val newBaseSet = baseSet.filter(e => p(e.canonicalized))
    val newOriginals = originals.filter(e => p(e.canonicalized))
    new ExpressionSet(newBaseSet, newOriginals)
  }

  override def filterNot(p: Expression => Boolean): ExpressionSet = {
    val newBaseSet = baseSet.filterNot(e => p(e.canonicalized))
    val newOriginals = originals.filterNot(e => p(e.canonicalized))
    new ExpressionSet(newBaseSet, newOriginals)
  }

  def +(elem: Expression): ExpressionSet = {
    val newSet = clone()
    newSet.add(elem)
    newSet
  }

  def ++(elems: GenTraversableOnce[Expression]): ExpressionSet = {
    val newSet = clone()
    elems.foreach(newSet.add)
    newSet
  }

  def -(elem: Expression): ExpressionSet = {
    val newSet = clone()
    newSet.remove(elem)
    newSet
  }

  def --(elems: GenTraversableOnce[Expression]): ExpressionSet = {
    val newSet = clone()
    elems.foreach(newSet.remove)
    newSet
  }

  def map(f: Expression => Expression): ExpressionSet = {
    val newSet = new ExpressionSet()
    this.iterator.foreach(elem => newSet.add(f(elem)))
    newSet
  }

  def flatMap(f: Expression => Iterable[Expression]): ExpressionSet = {
    val newSet = new ExpressionSet()
    this.iterator.foreach(f(_).foreach(newSet.add))
    newSet
  }

  def iterator: Iterator[Expression] = originals.iterator

  def union(that: ExpressionSet): ExpressionSet = {
    val newSet = clone()
    that.iterator.foreach(newSet.add)
    newSet
  }

  def subsetOf(that: ExpressionSet): Boolean = this.iterator.forall(that.contains)

  def intersect(that: ExpressionSet): ExpressionSet = this.filter(that.contains)

  def diff(that: ExpressionSet): ExpressionSet = this -- that

  def apply(elem: Expression): Boolean = this.contains(elem)

  override def equals(obj: Any): Boolean = obj match {
    case other: ExpressionSet => this.baseSet == other.baseSet
    case _ => false
  }

  override def hashCode(): Int = baseSet.hashCode()

  override def clone(): ExpressionSet = new ExpressionSet(baseSet.clone(), originals.clone())

  /**
   * Returns a string containing both the post [[Canonicalize]] expressions and the original
   * expressions in this set.
   */
  def toDebugString: String =
    s"""
       |baseSet: ${baseSet.mkString(", ")}
       |originals: ${originals.mkString(", ")}
     """.stripMargin
}
