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

object ImmutableVector {
  private val NIL = new LeafNode(Array.empty)

  def empty[A]: ImmutableVector[A] = NIL

  def fromArray[A](array: Array[A]): ImmutableVector[A] = {
    fromArray(array, 0, array.length)
  }

  def fromArray[A](array: Array[A], start: Int, end: Int): ImmutableVector[A] = {
    val length = end - start
    if (length == 0) {
      // println("fromArray(%d, %d) => empty".format(start, end))
      empty
    } else {
      val depth = depthOf(length)
      if (depth == 0) {
        // println("fromArray(%d, %d) => LeafNode".format(start, end))
        new LeafNode(array.slice(start, end))
      } else {
        val shift = 5 * depth
        val numChildren = ((length - 1) >> shift) + 1
        // println("fromArray(%d, %d) => InternalNode(depth=%d, numChildren=%d)".format(start, end, depth, numChildren))
        val children = new Array[ImmutableVector[A]](numChildren)
        var i = 0
        while (i < numChildren) {
          val childStart = start + (i << shift)
          var childEnd = start + ((i + 1) << shift)
          if (end < childEnd) {
            childEnd = end
          }
          children(i) = fromArray(array, childStart, childEnd)
          i += 1
        }
        new InternalNode(children, depth)
      }
    }
  }

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

private class VectorIterator[@specialized(Long, Int) +A](v: ImmutableVector[A]) extends Iterator[A] {
  private val elemStack: Array[ImmutableVector[_]] = Array.fill(8)(null)
  private val idxStack: Array[Int] = Array.fill(8)(-1)
  private var pos: Int = 0
  private var _hasNext: Boolean = _

  elemStack(0) = v
  idxStack(0) = 0
  maybeAdvance()

  override def hasNext = _hasNext

  override def next() = {
    if (_hasNext) {
      val result = elemStack(pos)(idxStack(pos)).asInstanceOf[A]
      idxStack(pos) += 1
      maybeAdvance()
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
            continue = false
        }
      }
    }
  }
}

sealed trait ImmutableVector[@specialized(Long, Int) +A] extends Serializable {
  def length: Int
  def iterator: Iterator[A] = new VectorIterator[A](this)
  def apply(index: Int): A
  def updated[B >: A : ClassTag](index: Int, elem: B): ImmutableVector[B]
  def numChildren: Int
}

private class InternalNode[@specialized(Long, Int) +A](
    children: Array[ImmutableVector[A]],
    depth: Int)
  extends ImmutableVector[A] {

  require(children.length > 0, "InternalNode must have children")
  require(children.length <= 32, "nodes cannot have more than 32 children (got ${children.length})")
  require(depth >= 1, "InternalNode must have depth >= 1 (got $depth)")

  override def length = (children.length - 1) * 32 + children.last.length

  override def apply(index: Int): A = {
    val shift = 5 * depth
    val localIndex = (index >> shift) & 31
    val childIndex = index & ~(31 << shift)
    // println("InternalNode(depth=%d, numChildren=%d).apply(%d): localIndex=%d, childIndex=%d".format(depth, children.length, index, localIndex, childIndex))
    children(localIndex)(childIndex)
  }

  override def updated[B >: A : ClassTag](index: Int, elem: B) = {
    val shift = 5 * depth
    val localIndex = (index >> shift) & 31
    val childIndex = index & ~(31 << shift)

    val newChildren = new Array[ImmutableVector[B]](children.length)
    System.arraycopy(children, 0, newChildren, 0, children.length)
    newChildren(localIndex) = children(localIndex).updated(childIndex, elem)
    new InternalNode(newChildren, depth)
  }

  override def numChildren = children.length

  def childAt(index: Int): ImmutableVector[A] = children(index)
}

private class LeafNode[@specialized(Long, Int) +A](
    children: Array[A])
  extends ImmutableVector[A] {

  require(children.length <= 32, "nodes cannot have more than 32 children (got ${children.length})")

  override def length = children.length

  override def apply(index: Int): A = children(index)

  override def updated[B >: A : ClassTag](index: Int, elem: B) = {
    val newChildren = new Array[B](children.length)
    System.arraycopy(children, 0, newChildren, 0, children.length)
    newChildren(index) = elem
    new LeafNode(newChildren)
  }

  override def numChildren = children.length
}
