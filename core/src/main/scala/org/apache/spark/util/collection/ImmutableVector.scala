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

package org.apache.spark.util.collection

import scala.reflect.ClassTag

/**
 * An immutable vector that supports efficient point updates. Similarly to
 * scala.collection.immutable.Vector, it is implemented using a 32-ary tree with 32-element arrays
 * at the leaves. Unlike Scala's Vector, it is specialized on the value type, making it much more
 * memory-efficient for primitive values.
 */
private[spark] class ImmutableVector[@specialized(Long, Int) A](val size: Int, root: VectorNode[A])
  extends Serializable {

  def iterator: Iterator[A] = new VectorIterator[A](root)
  def apply(index: Int): A = root(index)
  def updated(index: Int, elem: A): ImmutableVector[A] =
    new ImmutableVector(size, root.updated(index, elem))
}

private[spark] object ImmutableVector {
  def empty[A: ClassTag]: ImmutableVector[A] = new ImmutableVector(0, emptyNode)

  def fromArray[A: ClassTag](array: Array[A]): ImmutableVector[A] = {
    fromArray(array, 0, array.length)
  }

  def fromArray[A: ClassTag](array: Array[A], start: Int, end: Int): ImmutableVector[A] = {
    new ImmutableVector(end - start, nodeFromArray(array, start, end))
  }

  def fill[A: ClassTag](n: Int)(a: A): ImmutableVector[A] = {
    // TODO: Implement this without allocating an extra array
    fromArray(Array.fill(n)(a), 0, n)
  }

  /** Returns the root of a 32-ary tree representing the specified interval into the array. */
  private def nodeFromArray[A: ClassTag](array: Array[A], start: Int, end: Int): VectorNode[A] = {
    val length = end - start
    if (length == 0) {
      emptyNode
    } else {
      val depth = depthOf(length)
      if (depth == 0) {
        new LeafNode(array.slice(start, end))
      } else {
        val shift = 5 * depth
        val numChildren = ((length - 1) >> shift) + 1
        val children = new Array[VectorNode[A]](numChildren)
        var i = 0
        while (i < numChildren) {
          val childStart = start + (i << shift)
          var childEnd = start + ((i + 1) << shift)
          if (end < childEnd) {
            childEnd = end
          }
          children(i) = nodeFromArray(array, childStart, childEnd)
          i += 1
        }
        new InternalNode(children, depth)
      }
    }
  }

  private def emptyNode[A: ClassTag] = new LeafNode(Array.empty)

  /** Returns the required tree depth for an ImmutableVector of the given size. */
  private def depthOf(size: Int): Int = {
    var depth = 0
    var sizeLeft = (size - 1) >> 5
    while (sizeLeft > 0) {
      sizeLeft >>= 5
      depth += 1
    }
    depth
  }
}

/** Trait representing nodes in the vector tree. */
private sealed trait VectorNode[@specialized(Long, Int) A] extends Serializable {
  def apply(index: Int): A
  def updated(index: Int, elem: A): VectorNode[A]
  def numChildren: Int
}

/** An internal node in the vector tree (one containing other nodes rather than vector elements). */
private class InternalNode[@specialized(Long, Int) A: ClassTag](
    children: Array[VectorNode[A]],
    val depth: Int)
  extends VectorNode[A] {

  require(children.length > 0, "InternalNode must have children")
  require(children.length <= 32,
    s"nodes cannot have more than 32 children (got ${children.length})")
  require(depth >= 1, s"InternalNode must have depth >= 1 (got $depth)")

  def childAt(index: Int): VectorNode[A] = children(index)

  override def apply(index: Int): A = {
    var cur: VectorNode[A] = this
    var continue: Boolean = true
    var result: A = null.asInstanceOf[A]
    while (continue) {
      cur match {
        case internal: InternalNode[A] =>
          val shift = 5 * internal.depth
          val localIndex = (index >> shift) & 31
          cur = internal.childAt(localIndex)
        case leaf: LeafNode[A] =>
          continue = false
          result = leaf(index & 31)
      }
    }
    result
  }

  override def updated(index: Int, elem: A) = {
    val shift = 5 * depth
    val localIndex = (index >> shift) & 31
    val childIndex = index & ~(31 << shift)

    val newChildren = new Array[VectorNode[A]](children.length)
    System.arraycopy(children, 0, newChildren, 0, children.length)
    newChildren(localIndex) = children(localIndex).updated(childIndex, elem)
    new InternalNode(newChildren, depth)
  }

  override def numChildren = children.length
}

/** A leaf node in the vector tree containing up to 32 vector elements. */
private class LeafNode[@specialized(Long, Int) A: ClassTag](
    children: Array[A])
  extends VectorNode[A] {

  require(children.length <= 32,
    s"nodes cannot have more than 32 children (got ${children.length})")

  override def apply(index: Int): A = children(index)

  override def updated(index: Int, elem: A) = {
    val newChildren = new Array[A](children.length)
    System.arraycopy(children, 0, newChildren, 0, children.length)
    newChildren(index) = elem
    new LeafNode(newChildren)
  }

  override def numChildren = children.length
}

/** An iterator that walks through the vector tree. */
private class VectorIterator[@specialized(Long, Int) A](v: VectorNode[A]) extends Iterator[A] {
  private[this] val elemStack: Array[VectorNode[A]] = Array.fill(8)(null)
  private[this] val idxStack: Array[Int] = Array.fill(8)(-1)
  private[this] var pos: Int = 0
  private[this] var _hasNext: Boolean = _
  private[this] var numLeafChildren = 0

  elemStack(0) = v
  idxStack(0) = 0
  maybeAdvance()

  override def hasNext = _hasNext

  override def next() = {
    if (_hasNext) {
      val result = elemStack(pos)(idxStack(pos))
      idxStack(pos) += 1
      if (idxStack(pos) >= numLeafChildren) maybeAdvance()
      result
    } else {
      throw new NoSuchElementException("end of iterator")
    }
  }

  private def maybeAdvance() {
    // Pop exhausted nodes
    while (pos >= 0 && idxStack(pos) >= elemStack(pos).numChildren) {
      pos -= 1
    }

    _hasNext = pos >= 0
    if (_hasNext) {
      // Descend to the next leaf node element
      var continue: Boolean = true
      while (continue) {
        elemStack(pos) match {
          case internal: InternalNode[_] =>
            // Get the next child of this InternalNode
            val child = internal.childAt(idxStack(pos))
            idxStack(pos) += 1

            // Push it onto the stack
            pos += 1
            elemStack(pos) = child
            idxStack(pos) = 0
          case leaf: LeafNode[_] =>
            // Done - reached the next element
            numLeafChildren = leaf.numChildren
            continue = false
        }
      }
    }
  }
}
