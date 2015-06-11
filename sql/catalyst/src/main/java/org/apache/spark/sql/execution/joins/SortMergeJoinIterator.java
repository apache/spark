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

package org.apache.spark.sql.execution.joins;

import java.util.NoSuchElementException;
import javax.annotation.Nullable;

import scala.Function1;
import scala.collection.Iterator;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import org.apache.spark.sql.AbstractScalaRowIterator;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.JoinedRow5;
import org.apache.spark.sql.catalyst.expressions.RowOrdering;
import org.apache.spark.util.collection.CompactBuffer;

/**
 * Implements the merge step of sort-merge join.
 */
class SortMergeJoinIterator extends AbstractScalaRowIterator {

  private static final ClassTag<Row> ROW_CLASS_TAG = ClassTag$.MODULE$.apply(Row.class);
  private final Iterator<Row> leftIter;
  private final Iterator<Row> rightIter;
  private final Function1<Row, Row> leftKeyGenerator;
  private final Function1<Row, Row> rightKeyGenerator;
  private final RowOrdering keyOrdering;
  private final JoinedRow5 joinRow = new JoinedRow5();

  @Nullable private Row leftElement;
  @Nullable private Row rightElement;
  private Row leftKey;
  private Row rightKey;
  @Nullable private CompactBuffer<Row> rightMatches;
  private int rightPosition = -1;
  private boolean stop = false;
  private Row matchKey;

  public SortMergeJoinIterator(
      Iterator<Row> leftIter,
      Iterator<Row> rightIter,
      Function1<Row, Row> leftKeyGenerator,
      Function1<Row, Row> rightKeyGenerator,
      RowOrdering keyOrdering) {
    this.leftIter = leftIter;
    this.rightIter = rightIter;
    this.leftKeyGenerator = leftKeyGenerator;
    this.rightKeyGenerator = rightKeyGenerator;
    this.keyOrdering = keyOrdering;
    fetchLeft();
    fetchRight();
  }

  private void fetchLeft() {
    if (leftIter.hasNext()) {
      leftElement = leftIter.next();
      leftKey = leftKeyGenerator.apply(leftElement);
    } else {
      leftElement = null;
    }
  }

  private void fetchRight() {
    if (rightIter.hasNext()) {
      rightElement = rightIter.next();
      rightKey = rightKeyGenerator.apply(rightElement);
    } else {
      rightElement = null;
    }
  }

  /**
   * Searches the right iterator for the next rows that have matches in left side, and store
   * them in a buffer.
   *
   * @return true if the search is successful, and false if the right iterator runs out of
   *         tuples.
   */
  private boolean nextMatchingPair() {
    if (!stop && rightElement != null) {
      // run both side to get the first match pair
      while (!stop && leftElement != null && rightElement != null) {
        final int comparing = keyOrdering.compare(leftKey, rightKey);
        // for inner join, we need to filter those null keys
        stop = comparing == 0 && !leftKey.anyNull();
        if (comparing > 0 || rightKey.anyNull()) {
          fetchRight();
        } else if (comparing < 0 || leftKey.anyNull()) {
          fetchLeft();
        }
      }
      rightMatches = new CompactBuffer<Row>(ROW_CLASS_TAG);
      if (stop) {
        stop = false;
        // Iterate the right side to buffer all rows that match.
        // As the records should be ordered, exit when we meet the first record that not match.
        while (!stop && rightElement != null) {
          rightMatches.$plus$eq(rightElement);
          fetchRight();
          stop = keyOrdering.compare(leftKey, rightKey) != 0;
        }
        if (rightMatches.size() > 0) {
          rightPosition = 0;
          matchKey = leftKey;
        }
      }
    }
    return rightMatches != null && rightMatches.size() > 0;
  }

  @Override
  public boolean hasNext() {
    return nextMatchingPair();
  }

  @Override
  public Row next() {
    if (hasNext()) {
      // We are using the buffered right rows and run down left iterator
      final Row joinedRow = joinRow.apply(leftElement, rightMatches.apply(rightPosition));
      rightPosition += 1;
      if (rightPosition >= rightMatches.size()) {
        rightPosition = 0;
        fetchLeft();
        if (leftElement == null || keyOrdering.compare(leftKey, matchKey) != 0) {
          stop = false;
          rightMatches = null;
        }
      }
      return joinedRow;
    } else {
      // No more results
      throw new NoSuchElementException();
    }
  }
}
