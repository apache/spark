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
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.CoGroupedIterator
import org.apache.spark.sql.execution.GroupedIterator
import org.apache.spark.util.collection.CompactBuffer

case class CoGroupNode[Key, Left, Right, Result](
    conf: SQLConf,
    left: LocalNode,
    right: LocalNode,
    func: (Key, Iterator[Left], Iterator[Right]) => TraversableOnce[Result],
    keyEnc: ExpressionEncoder[Key],
    leftEnc: ExpressionEncoder[Left],
    rightEnc: ExpressionEncoder[Right],
    resultEnc: ExpressionEncoder[Result],
    output: Seq[Attribute],
    leftGroup: Seq[Attribute],
    rightGroup: Seq[Attribute]) extends BinaryLocalNode(conf) {

  private[this] var currentRow: InternalRow = _

  private[this] var iterator: Iterator[InternalRow] = _

  override def open(): Unit = {
    left.open()
    val leftRelation = new CompactBuffer[InternalRow]
    while (left.next()) {
      leftRelation += left.fetch().copy()
    }
    left.close

    right.open()
    val rightRelation = new CompactBuffer[InternalRow]
    while (right.next()) {
      rightRelation += right.fetch().copy()
    }
    right.close

    val leftGrouped = GroupedIterator(leftRelation.iterator, leftGroup, left.output)
    val rightGrouped = GroupedIterator(rightRelation.iterator, rightGroup, right.output)

    val boundKeyEnc = keyEnc.bind(leftGroup)
    val boundLeftEnc = leftEnc.bind(left.output)
    val boundRightEnc = rightEnc.bind(right.output)

    iterator = new CoGroupedIterator(leftGrouped, rightGrouped, leftGroup).flatMap {
      case (key, leftResult, rightResult) =>
        val results = func(
          boundKeyEnc.fromRow(key),
          leftResult.map(boundLeftEnc.fromRow),
          rightResult.map(boundRightEnc.fromRow))
        results.map(resultEnc.toRow(_))
    }
  }

  override def next(): Boolean = if (iterator.hasNext) {
    currentRow = iterator.next()
    true
  } else {
    false
  }

  override def fetch(): InternalRow = currentRow

  override def close(): Unit = {
    left.close()
    right.close()
  }
}
