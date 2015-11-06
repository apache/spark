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
import org.apache.spark.sql.catalyst.expressions.Attribute

case class UnionNode(conf: SQLConf, children: Seq[LocalNode]) extends LocalNode(conf) {

  override def output: Seq[Attribute] = children.head.output

  private[this] var currentChild: LocalNode = _

  private[this] var nextChildIndex: Int = _

  override def open(): Unit = {
    currentChild = children.head
    currentChild.open()
    nextChildIndex = 1
  }

  private def advanceToNextChild(): Boolean = {
    var found = false
    var exit = false
    while (!exit && !found) {
      if (currentChild != null) {
        currentChild.close()
      }
      if (nextChildIndex >= children.size) {
        found = false
        exit = true
      } else {
        currentChild = children(nextChildIndex)
        nextChildIndex += 1
        currentChild.open()
        found = currentChild.next()
      }
    }
    found
  }

  override def close(): Unit = {
    if (currentChild != null) {
      currentChild.close()
    }
  }

  override def fetch(): InternalRow = currentChild.fetch()

  override def next(): Boolean = {
    if (currentChild.next()) {
      true
    } else {
      advanceToNextChild()
    }
  }
}
