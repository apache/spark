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

package org.apache.spark.sql.execution.local

import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Projection}

case class ExpandNode(
    conf: SQLConf,
    projections: Seq[Seq[Expression]],
    output: Seq[Attribute],
    child: LocalNode) extends UnaryLocalNode(conf) {

  assert(projections.size > 0)

  private[this] var result: InternalRow = _
  private[this] var idx: Int = _
  private[this] var input: InternalRow = _
  private[this] var groups: Array[Projection] = _

  override def open(): Unit = {
    child.open()
    idx = -1
    groups = projections.map(ee => newProjection(ee, child.output)).toArray
  }

  override def next(): Boolean = {
    if (idx < 0 || idx >= groups.length) {
      if (child.next()) {
        input = child.fetch()
        result = groups(0)(input)
        idx = 1
        true
      } else {
        false
      }
    } else {
      result = groups(idx)(input)
      idx += 1
      true
    }
  }

  override def fetch(): InternalRow = result

  override def close(): Unit = child.close()
}
