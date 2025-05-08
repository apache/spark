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

package org.apache.spark.sql.catalyst.analysis.resolver

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{
  BinaryExpression,
  Expression,
  QuaternaryExpression,
  TernaryExpression,
  UnaryExpression
}

trait ResolvesExpressionChildren {

  /**
   * Resolves [[UnaryExpression]] children and returns its copy with children resolved.
   */
  protected def withResolvedChildren(
      unresolvedExpression: UnaryExpression,
      resolveChild: Expression => Expression): Expression = {
    val newChildren = Seq(resolveChild(unresolvedExpression.child))
    unresolvedExpression.withNewChildren(newChildren)
  }

  /**
   * Resolves [[BinaryExpression]] children and returns its copy with children resolved.
   */
  protected def withResolvedChildren(
      unresolvedExpression: BinaryExpression,
      resolveChild: Expression => Expression): Expression = {
    val newChildren =
      Seq(resolveChild(unresolvedExpression.left), resolveChild(unresolvedExpression.right))
    unresolvedExpression.withNewChildren(newChildren)
  }

  /**
   * Resolves [[TernaryExpression]] children and returns its copy with children resolved.
   */
  protected def withResolvedChildren(
      unresolvedExpression: TernaryExpression,
      resolveChild: Expression => Expression): Expression = {
    val newChildren = Seq(
      resolveChild(unresolvedExpression.first),
      resolveChild(unresolvedExpression.second),
      resolveChild(unresolvedExpression.third)
    )
    unresolvedExpression.withNewChildren(newChildren)
  }

  /**
   * Resolves [[QuaternaryExpression]] children and returns its copy with children resolved.
   */
  protected def withResolvedChildren(
      unresolvedExpression: QuaternaryExpression,
      resolveChild: Expression => Expression): Expression = {
    val newChildren = Seq(
      resolveChild(unresolvedExpression.first),
      resolveChild(unresolvedExpression.second),
      resolveChild(unresolvedExpression.third),
      resolveChild(unresolvedExpression.fourth)
    )
    unresolvedExpression.withNewChildren(newChildren)
  }

  /**
   * Resolves generic [[Expression]] children and returns its copy with children resolved.
   */
  protected def withResolvedChildren(
      unresolvedExpression: Expression,
      resolveChild: Expression => Expression): Expression = unresolvedExpression match {
    case unaryExpression: UnaryExpression =>
      withResolvedChildren(unaryExpression, resolveChild)
    case binaryExpression: BinaryExpression =>
      withResolvedChildren(binaryExpression, resolveChild)
    case ternaryExpression: TernaryExpression =>
      withResolvedChildren(ternaryExpression, resolveChild)
    case quaternaryExpression: QuaternaryExpression =>
      withResolvedChildren(quaternaryExpression, resolveChild)
    case _ =>
      withResolvedChildrenImpl(unresolvedExpression, resolveChild)
  }

  private def withResolvedChildrenImpl(
      unresolvedExpression: Expression,
      resolveChild: Expression => Expression): Expression = {
    val newChildren = new mutable.ArrayBuffer[Expression](unresolvedExpression.children.size)

    val childrenIterator = unresolvedExpression.children.iterator
    while (childrenIterator.hasNext) {
      newChildren += resolveChild(childrenIterator.next())
    }

    unresolvedExpression.withNewChildren(newChildren.toSeq)
  }
}
