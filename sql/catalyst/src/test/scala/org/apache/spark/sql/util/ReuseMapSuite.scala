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

package org.apache.spark.sql.util

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.types.IntegerType

case class TestNode(children: Seq[TestNode], output: Seq[Attribute]) extends LogicalPlan {
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = copy(children = children)
}
case class TestReuseNode(child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(child = newChild)
}

class ReuseMapSuite extends SparkFunSuite {
  private val leafNode1 = TestNode(Nil, Seq(AttributeReference("a", IntegerType)()))
  private val leafNode2 = TestNode(Nil, Seq(AttributeReference("b", IntegerType)()))
  private val parentNode1 = TestNode(Seq(leafNode1), Seq(AttributeReference("a", IntegerType)()))
  private val parentNode2 = TestNode(Seq(leafNode2), Seq(AttributeReference("b", IntegerType)()))

  private def reuse(testNode: TestNode) = TestReuseNode(testNode)

  test("no reuse if same instance") {
    val reuseMap = new ReuseMap[TestNode, LogicalPlan]()

    reuseMap.reuseOrElseAdd(leafNode1, reuse)
    reuseMap.reuseOrElseAdd(parentNode1, reuse)

    assert(reuseMap.reuseOrElseAdd(leafNode1, reuse) == leafNode1)
    assert(reuseMap.reuseOrElseAdd(parentNode1, reuse) == parentNode1)
  }

  test("reuse if different instance with same canonicalized plan") {
    val reuseMap = new ReuseMap[TestNode, LogicalPlan]()
    reuseMap.reuseOrElseAdd(leafNode1, reuse)
    reuseMap.reuseOrElseAdd(parentNode1, reuse)

    assert(reuseMap.reuseOrElseAdd(leafNode1.clone.asInstanceOf[TestNode], reuse) ==
      reuse(leafNode1))
    assert(reuseMap.reuseOrElseAdd(parentNode1.clone.asInstanceOf[TestNode], reuse) ==
      reuse(parentNode1))
  }

  test("no reuse if different canonicalized plan") {
    val reuseMap = new ReuseMap[TestNode, LogicalPlan]()
    reuseMap.reuseOrElseAdd(leafNode1, reuse)
    reuseMap.reuseOrElseAdd(parentNode1, reuse)

    assert(reuseMap.reuseOrElseAdd(leafNode2, reuse) == leafNode2)
    assert(reuseMap.reuseOrElseAdd(parentNode2, reuse) == parentNode2)
  }
}
