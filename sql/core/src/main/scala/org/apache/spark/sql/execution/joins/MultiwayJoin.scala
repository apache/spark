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

package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.catalyst.expressions.{GenericRow, Expression, Row, Projection, Attribute}
import org.apache.spark.sql.catalyst.plans.{JoinType, Inner, LeftOuter, RightOuter, LeftSemi, FullOuter}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.util.collection.{CompactBuffer, BitSet}

case class JoinFilter(joinType: JoinType, filter: Expression)
case class JoinKey(leftKeys: Seq[Expression], rightkeys: Seq[Expression])

/**
 * A mutable wrapper that makes multiple rows appear as a single concatenated row.  Designed to
 * be instantiated once per thread and reused.
 */
private[sql] class MultiJoinedRow(colNums: Int*) extends Row {
  assert(colNums.length >= 2)

  private[this] val cache = new Array[Row](colNums.length)

  private[this] val mapping = {
    val array = new Array[(Int, Int)](colNums.sum)

    var tblIdx = 0
    var passed = 0
    for (i <- 0 until array.length) {
      if (i >= passed + colNums(tblIdx)) {
        passed += colNums(tblIdx)
        tblIdx += 1
      }
      array(i) = (tblIdx, i - passed)
    }

    array
  }

  final def withNewTable(idx: Int, row: Row): MultiJoinedRow = {
    // TODO check the columns count of the row, must equals to associated colNums
    // assert(row == null || row.length == colNums(idx))
    cache(idx) = row

    this
  }

  final def clearTable(idx: Int): MultiJoinedRow = {
    cache(idx) = null
    this
  }

  override def toSeq: Seq[Any] = cache.map(_.toSeq).reduce(_ ++ _)

  override def length = mapping.length

  private def index(pos: Int) = mapping(pos)

  private def eval[T](pos: Int, fun: (Row, Int) => T): T = {
    val idx = index(pos)
    val row = cache(idx._1)

    if (row == null) {
      null.asInstanceOf[T]
    } else {
      fun(row, idx._2)
    }
  }

  override def apply(i: Int) =
    eval(i, (row: Row, pos: Int) => row(pos))

  override def isNullAt(i: Int) =
    eval(i, (row: Row, pos: Int) => row(pos)) == null

  override def getInt(i: Int): Int =
    eval(i, (row: Row, pos: Int) => row.getInt(pos))

  override def getLong(i: Int): Long =
    eval(i, (row: Row, pos: Int) => row.getLong(pos))

  override def getDouble(i: Int): Double =
    eval(i, (row: Row, pos: Int) => row.getDouble(pos))

  override def getBoolean(i: Int): Boolean =
    eval(i, (row: Row, pos: Int) => row.getBoolean(pos))

  override def getShort(i: Int): Short =
    eval(i, (row: Row, pos: Int) => row.getShort(pos))

  override def getByte(i: Int): Byte =
    eval(i, (row: Row, pos: Int) => row.getByte(pos))

  override def getFloat(i: Int): Float =
    eval(i, (row: Row, pos: Int) => row.getFloat(pos))

  override def getString(i: Int): String =
    eval(i, (row: Row, pos: Int) => row.getString(pos))

  override def getAs[T](i: Int): T =
    eval(i, (row: Row, pos: Int) => row.getAs[T](pos))

  override def copy() = {
    val copiedValues = new Array[Any](length)
    var i = 0
    while(i < length) {
      copiedValues(i) = apply(i)
      i += 1
    }
    new GenericRow(copiedValues)
  }

  override def toString() = {
    // Make sure toString never throws NullPointerException.
    if (cache eq null) {
      "[ empty row ]"
    } else {
      cache.mkString("[", ",", "]")
    }
  }
}

trait CompactBufferBuilder extends java.io.Serializable {
  def build: CompactBuffer[Row]

  // TODO HashedRelation should always return a non empty Buffer?
  @inline
  protected[this] def handleNullBuffer(buffer: CompactBuffer[Row]) = {
    if (buffer == null) {
      CompactBufferBuilder.EMPTY_BUFFER
    } else {
      buffer
    }
  }

  def correlated = false
}

object CompactBufferBuilder {
  val EMPTY_BUFFER = CompactBuffer.constantEmpty[Row]()
}

class IteratorBufferBuilder extends CompactBufferBuilder {
  val buffer = CompactBuffer[Row](null)

  def withIterator(input: Row) = {
    buffer.update(0, input)
    this
  }

  def build = buffer
}

class HashedBufferBuilder(relation: HashedRelation) extends CompactBufferBuilder {
  @transient
  var buffer:CompactBuffer[Row] = null

  def withEquiJoinKey(key: Row) = {
    buffer = handleNullBuffer(relation.get(key))

    this
  }

  def build = buffer
}

// Correlated buffer builder is backward-correlated, which mean it requires
// the previous join result entry has been computed
// e.g. SELECT ... FROM a JOIN b ON a.key = b.key JOIN c ON b.value = c.value
// We need the b.value in the entry has been computed, and then we can get the
// buffer for relation "c", when iterating over the whole join series.
class CorrelatedBufferBuilder(key: Projection, relation: HashedRelation) extends CompactBufferBuilder {
  @transient
  var input: Row = _
  def withLeft(input: Row): this.type = {
    this.input = key(input)
    this
  }

  def build: CompactBuffer[Row] = {
    handleNullBuffer(relation.get(input))
  }

  override def correlated = true
}

class ConstantBufferBuilder(row: Row) extends CompactBufferBuilder {
  val buffer: CompactBuffer[Row] = if (row == null) {
    CompactBufferBuilder.EMPTY_BUFFER
  } else {
    CompactBuffer(row)
  }

  def build = buffer
}

trait MultiwayJoin {
  def joinFilters: Seq[JoinFilter]

  def childrenOutputs: Seq[Seq[Attribute]]

  val output = childrenOutputs.reduce(_ ++ _)

  private[this] val NULL_ROWS = Array.tabulate(childrenOutputs.length) { idx =>
    new ConstantBufferBuilder(Row(Array.fill[Any](childrenOutputs(idx).length)(null): _*))
  }

  private[this] val EMPTY_ROW = new ConstantBufferBuilder(null)

  // The output buffer array. The product function returns an iterator that will
  // always return this outputBuffer. Downstream operations need to make sure
  // they are just streaming through the output.
  private[this] val inputBuffer = new MultiJoinedRow(childrenOutputs.map(_.length).toArray: _*)

  @inline
  private[this] final def predicate(row: MultiJoinedRow, pos: Int): Boolean = {
    true == joinFilters(pos).filter.eval(row)
  }

  def product(bufs: Array[CompactBufferBuilder]): Iterator[MultiJoinedRow] = {
    assert(bufs.length == joinFilters.length + 1)

    var i = 0

    var partial: Iterator[MultiJoinedRow] = createBase(bufs(i).build, i)
    while (i < joinFilters.length) {
      val joinCondition = joinFilters(i)
      i += 1

      val currentBuilder = bufs(i)

      partial = joinCondition.joinType match {
        // Only Inner and LeftSemi join can support the correlated buffer builder
        case Inner if currentBuilder.correlated =>
          product2(partial, currentBuilder.asInstanceOf[CorrelatedBufferBuilder], i)

        case Inner =>
          val buffer = currentBuilder.build
          if (buffer.size == 0) {
            createBase(EMPTY_ROW.build, i)
          } else {
            product2(partial, buffer, i)
          }

        case LeftSemi if currentBuilder.correlated =>
          // For semi join, we only need one element from the table on the right
          // to verify a row exists.
          product2LeftSemiJoin(partial, currentBuilder.asInstanceOf[CorrelatedBufferBuilder], i)

        case LeftSemi =>
          // For semi join, we only need one element from the table on the right
          // to verify a row exists.
          val buffer = currentBuilder.build
          if (partial.hasNext == false || buffer.size == 0) {
            createBase(EMPTY_ROW.build, i)
          } else {
            product2LeftSemiJoin(partial, buffer, i)
          }

        case FullOuter =>
          val buffer = currentBuilder.build
          if (partial.hasNext == false && buffer.size == 0) {
            createBase(EMPTY_ROW.build, i)
          } else if (partial.hasNext == false) {
            product2(createBase(NULL_ROWS(i - 1).build, i - 1), buffer, i)
          } else if (buffer.size == 0) {
            product2(partial, NULL_ROWS(i).build, i)
          } else {
            product2FullOuterJoin(partial, buffer, i)
          }

        case LeftOuter =>
          val buffer = currentBuilder.build
          if (partial.hasNext == false) {
            createBase(EMPTY_ROW.build, i)
          } else if (buffer.size == 0) {
            product2(partial, NULL_ROWS(i).build, i)
          } else {
            product2LeftOuterJoin(partial, buffer, i)
          }

        case RightOuter =>
          val buffer = currentBuilder.build
          if (buffer.size == 0) {
            createBase(EMPTY_ROW.build, i)
          } else if (partial.hasNext == false) {
            product2(createBase(NULL_ROWS(i - 1).build, i - 1), buffer, i)
          } else {
            product2RightOuterJoin(partial, buffer, i)
          }

      }
    }
    partial
  }

  @inline
  private def filter(iter: Iterator[MultiJoinedRow], pos: Int)
  : Iterator[MultiJoinedRow] = {
    var occurs = 1
    iter.filter { e =>
      // Per outer join semantic, on more than 1 null table value allowed, we need to filter out
      // the entries from the iterator if it's failed in join filter testing (just keep 1)
      val valid = predicate(e, pos - 1)
      if (valid) {
        true
      } else {
        occurs = occurs - 1
        e.clearTable(pos)
        // if first appearance
        occurs >= 0
      }
    }
  }

  private[this] def product2(left: Iterator[MultiJoinedRow], right: CorrelatedBufferBuilder, pos: Int): Iterator[MultiJoinedRow] = {
    (for (l <- left;
          r <- right.withLeft(l).build.iterator) yield {
      l.withNewTable(pos, r)
    }).filter(predicate(_, pos - 1))
  }

  private[this] def product2(left: Iterator[MultiJoinedRow], right: CompactBuffer[Row], pos: Int): Iterator[MultiJoinedRow] = {
    (for (l <- left; r <- right.iterator) yield {
      l.withNewTable(pos, r)
    }).filter(predicate(_, pos - 1))
  }

  private[this] def product2FullOuterJoin(left: Iterator[MultiJoinedRow], right: CompactBuffer[Row], pos: Int): Iterator[MultiJoinedRow] =
  {
    val bs = new BitSet(right.length)
    var needReset = true

    var idxOuter = -1

    (left.flatMap { l =>
      var idxInner = -1
      val r = right.iterator.map { r =>
        l.withNewTable(pos, r)
      }.filter { e =>
        idxInner += 1
        val filter = predicate(e, pos - 1)
        if (filter) {
          bs.set(idxInner)
        }
        filter
      }
      if (r.hasNext) r else Iterator(l.clearTable(pos))
    }) ++ (right.iterator.filter { _ =>
      // only take those unmatched entry
      idxOuter += 1
      val filter = !bs.get(idxOuter)
      if (filter && needReset) {
        // only reset once
        resetInputBuffer(pos)
        needReset = false
      }

      filter
    }).map(e => inputBuffer.withNewTable(pos, e))
  }

  private[this] def product2LeftOuterJoin(left: Iterator[MultiJoinedRow], right: CompactBuffer[Row], pos: Int)
  : Iterator[MultiJoinedRow] = {
    left.flatMap { l =>
      val r = right.iterator.map { r =>
        l.withNewTable(pos, r)
      }.filter(predicate(_, pos - 1))
      if (r.hasNext) r else Iterator(l.clearTable(pos))
    }
  }

  private[this] def product2LeftSemiJoin(left: Iterator[MultiJoinedRow], right: CompactBuffer[Row], pos: Int)
  : Iterator[MultiJoinedRow] = {
    (left.filter { l =>
      right.exists { r =>
        predicate(l.withNewTable(pos, r), pos - 1)
      }
    }).map(_.clearTable(pos))
  }

  private[this] def product2LeftSemiJoin(left: Iterator[MultiJoinedRow], right: CorrelatedBufferBuilder, pos: Int)
  : Iterator[MultiJoinedRow] = {
    (left.filter { l =>
      right.withLeft(l).build.exists { r =>
        predicate(l.withNewTable(pos, r), pos - 1)
      }
    }).map(_.clearTable(pos))
  }

  private[this] def product2RightOuterJoin(left: Iterator[MultiJoinedRow], right: CompactBuffer[Row], pos: Int)
  : Iterator[MultiJoinedRow] = {
    val bs = new BitSet(right.length)
    var needReset = true

    var idxOuter = -1
    (left.flatMap { l =>
      var idxInner = -1
      right.iterator.flatMap { r =>
        idxInner += 1
        if (predicate(l.withNewTable(pos, r), pos - 1)) {
          bs.set(idxInner)
          Iterator(l)
        } else {
          Iterator.empty
        }
      }
    }) ++ (right.iterator.filter { _ =>
      // only take those unmatched entry
      idxOuter += 1
      val filter = !bs.get(idxOuter)
      if (filter && needReset) {
        // only reset once
        resetInputBuffer(pos)
        needReset = false
      }

      filter
    }).map(e => inputBuffer.withNewTable(pos, e))
  }

  @inline
  private[this] def resetInputBuffer(pos: Int): MultiJoinedRow = {
    var i = 0
    while (i <= pos) {
      // reset the buffer
      inputBuffer.clearTable(i)
      i += 1
    }
    inputBuffer
  }

  private[this] def createBase(left: CompactBuffer[Row], pos: Int): Iterator[MultiJoinedRow] = {
    resetInputBuffer(pos)

    left.iterator.map { l =>
      inputBuffer.withNewTable(pos, l)
    }
  }
}
