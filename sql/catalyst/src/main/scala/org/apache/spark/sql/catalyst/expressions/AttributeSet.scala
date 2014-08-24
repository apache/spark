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

class AttributeEquals(val a: Attribute) {
  override def hashCode() = a.exprId.hashCode()
  override def equals(other: Any) = other match {
    case otherReference: AttributeEquals => a.exprId == otherReference.a.exprId
    case otherAttribute => false
  }
}

object AttributeSet {
  def apply(baseSet: Seq[Attribute]) = {
    new AttributeSet(baseSet.map(new AttributeEquals(_)).toSet)
  }

//  def apply(baseSet: Set[Attribute]) = {
//    new AttributeSet(baseSet.map(new AttributeEquals(_)))
//  }
}

class AttributeSet(val baseSet: Set[AttributeEquals]) extends Traversable[Attribute] {
  def contains(elem: NamedExpression): Boolean =
    baseSet.contains(new AttributeEquals(elem.toAttribute))

  def +(elem: Attribute): AttributeSet =
    new AttributeSet(baseSet + new AttributeEquals(elem))

  def -(elem: Attribute): AttributeSet =
    new AttributeSet(baseSet - new AttributeEquals(elem))

  def iterator: Iterator[Attribute] = baseSet.map(_.a).iterator

  def subsetOf(other: AttributeSet) = baseSet.subsetOf(other.baseSet)

  def --(other: Traversable[NamedExpression]) =
    new AttributeSet(baseSet -- other.map(a => new AttributeEquals(a.toAttribute)))

  def ++(other: AttributeSet) = new AttributeSet(baseSet ++ other.baseSet)

  override def filter(f: Attribute => Boolean) = new AttributeSet(baseSet.filter(ae => f(ae.a)))

  def intersect(other: AttributeSet) = new AttributeSet(baseSet.intersect(other.baseSet))

  override def nonEmpty = baseSet.nonEmpty

  override def toSeq = baseSet.toSeq.map(_.a)

  override def foreach[U](f: (Attribute) => U): Unit = baseSet.map(_.a).foreach(f)
}