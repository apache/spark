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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._

case class PushDownCount(children: Seq[Expression], pushdown: Boolean) extends CountBase(children) {

  override lazy val updateExpressions = {
    val nullableChildren = children.filter(_.nullable)
    if (nullableChildren.isEmpty) {
      Seq(
        /* count = */ count + children.head
      )
    } else {
      Seq(
        /* count = */ If(nullableChildren.map(IsNull).reduce(Or), count, count + children.head)
      )
    }
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression])
    : PushDownCount =
    copy(children = newChildren)
}

object PushDownCount {
  def apply(child: Expression, pushdown: Boolean): PushDownCount =
    PushDownCount(child :: Nil, pushdown)
}
