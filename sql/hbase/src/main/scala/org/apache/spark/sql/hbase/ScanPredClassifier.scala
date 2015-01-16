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

package org.apache.spark.sql.hbase

import org.apache.spark.sql.catalyst.expressions._

/**
 * Classifies a predicate into a pair of (pushdownable, non-pushdownable) predicates
 * for a Scan; the logic relationship between the two components of the pair is AND
 */
class ScanPredClassifier(relation: HBaseRelation, keyIndex: Int) {
  def apply(pred: Expression): (Option[Expression], Option[Expression]) = {
    // post-order bottom-up traversal
    pred match {
      case And(left, right) =>
        val (ll, lr) = apply(left)
        val (rl, rr) = apply(right)
        (ll, lr, rl, rr) match {
          // All Nones
          case (None, None, None, None) => (None, None)
          // Three Nones
          case (None, None, None, _) => (None, rr)
          case (None, None, _, None) => (rl, None)
          case (None, _, None, None) => (None, lr)
          case (_, None, None, None) => (ll, None)
          // two Nones
          case (None, None, _, _) => (rl, rr)
          case (None, _, None, _) => (None, Some(And(lr.get, rr.get)))
          case (None, _, _, None) => (rl, lr)
          case (_, None, None, _) => (ll, rr)
          case (_, None, _, None) => (Some(And(ll.get, rl.get)), None)
          case (_, _, None, None) => (ll, lr)
          // One None
          case (None, _, _, _) => (rl, Some(And(lr.get, rr.get)))
          case (_, None, _, _) => (Some(And(ll.get, rl.get)), rr)
          case (_, _, None, _) => (ll, Some(And(lr.get, rr.get)))
          case (_, _, _, None) => (Some(And(ll.get, rl.get)), lr)
          // No nones
          case _ => (Some(And(ll.get, rl.get)), Some(And(lr.get, rr.get)))
        }
      case Or(left, right) =>
        val (ll, lr) = apply(left)
        val (rl, rr) = apply(right)
        (ll, lr, rl, rr) match {
          // All Nones
          case (None, None, None, None) => (None, None)
          // Three Nones
          case (None, None, None, _) => (None, rr)
          case (None, None, _, None) => (rl, None)
          case (None, _, None, None) => (None, lr)
          case (_, None, None, None) => (ll, None)
          // two Nones
          case (None, None, _, _) => (rl, rr)
          case (None, _, None, _) => (None, Some(Or(lr.get, rr.get)))
          case (None, _, _, None) => (None, Some(Or(lr.get, rl.get)))
          case (_, None, None, _) => (None, Some(Or(ll.get, rr.get)))
          case (_, None, _, None) => (Some(Or(ll.get, rl.get)), None)
          case (_, _, None, None) => (ll, lr)
          // One None
          case (None, _, _, _) => (None, Some(pred))
          // Accept increased evaluation complexity for improved pushed down
          case (_, None, _, _) => (Some(Or(ll.get, rl.get)), Some(Or(ll.get, rr.get)))
          case (_, _, None, _) => (None, Some(pred))
          // Accept increased evaluation complexity for improved pushed down
          case (_, _, _, None) => (Some(Or(ll.get, rl.get)), Some(Or(lr.get, rl.get)))
          // No nones
          // Accept increased evaluation complexity for improved pushed down
          case _ => (Some(Or(ll.get, rl.get)), Some(And(Or(ll.get, rr.get),
            And(Or(lr.get, rl.get), Or(lr.get, rr.get)))))
        }
      case EqualTo(left, right) => classifyBinary(left, right, pred)
      case LessThan(left, right) => classifyBinary(left, right, pred)
      case LessThanOrEqual(left, right) => classifyBinary(left, right, pred)
      case GreaterThan(left, right) => classifyBinary(left, right, pred)
      case GreaterThanOrEqual(left, right) => classifyBinary(left, right, pred)
      // everything else are treated as non pushdownable
      case _ => (None, Some(pred))
    }
  }

  // returns true if the binary operator of the two args can be pushed down
  private def classifyBinary(left: Expression, right: Expression, pred: Expression)
  : (Option[Expression], Option[Expression]) = {
    (left, right) match {
      case (Literal(_, _), AttributeReference(_, _, _, _)) =>
        if (relation.isNonKey(right.asInstanceOf[AttributeReference])) {
          (Some(pred), None)
        } else {
          val keyIdx = relation.keyIndex(right.asInstanceOf[AttributeReference])
          if (keyIdx == keyIndex) {
            (Some(pred), None)
          } else {
            (None, Some(pred))
          }
        }
      case (AttributeReference(_, _, _, _), Literal(_, _)) =>
        if (relation.isNonKey(left.asInstanceOf[AttributeReference])) {
          (Some(pred), None)
        } else {
          val keyIdx = relation.keyIndex(left.asInstanceOf[AttributeReference])
          if (keyIdx == keyIndex) {
            (Some(pred), None)
          } else {
            (None, Some(pred))
          }
        }
      case _ => (None, Some(pred))
    }
  }
}
