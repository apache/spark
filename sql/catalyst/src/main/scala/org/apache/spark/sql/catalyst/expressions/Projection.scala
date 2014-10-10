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

package org.apache.spark.sql.catalyst.expressions


/**
 * A [[Projection]] that is calculated by calling the `eval` of each of the specified expressions.
 * @param expressions a sequence of expressions that determine the value of each column of the
 *                    output row.
 */
class InterpretedProjection(expressions: Seq[Expression]) extends Projection {
  def this(expressions: Seq[Expression], inputSchema: Seq[Attribute]) =
    this(expressions.map(BindReferences.bindReference(_, inputSchema)))

  // null check is required for when Kryo invokes the no-arg constructor.
  protected val exprArray = if (expressions != null) expressions.toArray else null

  def apply(input: Row): Row = {
    val outputArray = new Array[Any](exprArray.length)
    var i = 0
    while (i < exprArray.length) {
      outputArray(i) = exprArray(i).eval(input)
      i += 1
    }
    new GenericRow(outputArray)
  }
}

/**
 * A [[MutableProjection]] that is calculated by calling `eval` on each of the specified
 * expressions.
 * @param expressions a sequence of expressions that determine the value of each column of the
 *                    output row.
 */
case class InterpretedMutableProjection(expressions: Seq[Expression]) extends MutableProjection {
  def this(expressions: Seq[Expression], inputSchema: Seq[Attribute]) =
    this(expressions.map(BindReferences.bindReference(_, inputSchema)))

  private[this] val exprArray = expressions.toArray
  private[this] var mutableRow: MutableRow = new GenericMutableRow(exprArray.size)
  def currentValue: Row = mutableRow

  override def target(row: MutableRow): MutableProjection = {
    mutableRow = row
    this
  }

  override def apply(input: Row): Row = {
    var i = 0
    while (i < exprArray.length) {
      mutableRow(i) = exprArray(i).eval(input)
      i += 1
    }
    mutableRow
  }
}

/**
 * A mutable wrapper that makes two rows appear as a single concatenated row.  Designed to
 * be instantiated once per thread and reused.
 */
class JoinedRow extends Row {
  private[this] var row1: Row = _
  private[this] var row2: Row = _

  def this(left: Row, right: Row) = {
    this()
    row1 = left
    row2 = right
  }

  /** Updates this JoinedRow to used point at two new base rows.  Returns itself. */
  def apply(r1: Row, r2: Row): Row = {
    row1 = r1
    row2 = r2
    this
  }

  /** Updates this JoinedRow by updating its left base row.  Returns itself. */
  def withLeft(newLeft: Row): Row = {
    row1 = newLeft
    this
  }

  /** Updates this JoinedRow by updating its right base row.  Returns itself. */
  def withRight(newRight: Row): Row = {
    row2 = newRight
    this
  }

  def iterator = row1.iterator ++ row2.iterator

  def length = row1.length + row2.length

  def apply(i: Int) =
    if (i < row1.size) row1(i) else row2(i - row1.size)

  def isNullAt(i: Int) =
    if (i < row1.size) row1.isNullAt(i) else row2.isNullAt(i - row1.size)

  def getInt(i: Int): Int =
    if (i < row1.size) row1.getInt(i) else row2.getInt(i - row1.size)

  def getLong(i: Int): Long =
    if (i < row1.size) row1.getLong(i) else row2.getLong(i - row1.size)

  def getDouble(i: Int): Double =
    if (i < row1.size) row1.getDouble(i) else row2.getDouble(i - row1.size)

  def getBoolean(i: Int): Boolean =
    if (i < row1.size) row1.getBoolean(i) else row2.getBoolean(i - row1.size)

  def getShort(i: Int): Short =
    if (i < row1.size) row1.getShort(i) else row2.getShort(i - row1.size)

  def getByte(i: Int): Byte =
    if (i < row1.size) row1.getByte(i) else row2.getByte(i - row1.size)

  def getFloat(i: Int): Float =
    if (i < row1.size) row1.getFloat(i) else row2.getFloat(i - row1.size)

  def getString(i: Int): String =
    if (i < row1.size) row1.getString(i) else row2.getString(i - row1.size)

  override def getAs[T](i: Int): T =
    if (i < row1.size) row1.getAs[T](i) else row2.getAs[T](i - row1.size)

  def copy() = {
    val totalSize = row1.size + row2.size
    val copiedValues = new Array[Any](totalSize)
    var i = 0
    while(i < totalSize) {
      copiedValues(i) = apply(i)
      i += 1
    }
    new GenericRow(copiedValues)
  }

  override def toString() = {
    val row = (if (row1 != null) row1 else Seq[Any]()) ++ (if (row2 != null) row2 else Seq[Any]())
    s"[${row.mkString(",")}]"
  }
}

/**
 * JIT HACK: Replace with macros
 * The `JoinedRow` class is used in many performance critical situation.  Unfortunately, since there
 * are multiple different types of `Rows` that could be stored as `row1` and `row2` most of the
 * calls in the critical path are polymorphic.  By creating special versions of this class that are
 * used in only a single location of the code, we increase the chance that only a single type of
 * Row will be referenced, increasing the opportunity for the JIT to play tricks.  This sounds
 * crazy but in benchmarks it had noticeable effects.
 */
class JoinedRow2 extends Row {
  private[this] var row1: Row = _
  private[this] var row2: Row = _

  def this(left: Row, right: Row) = {
    this()
    row1 = left
    row2 = right
  }

  /** Updates this JoinedRow to used point at two new base rows.  Returns itself. */
  def apply(r1: Row, r2: Row): Row = {
    row1 = r1
    row2 = r2
    this
  }

  /** Updates this JoinedRow by updating its left base row.  Returns itself. */
  def withLeft(newLeft: Row): Row = {
    row1 = newLeft
    this
  }

  /** Updates this JoinedRow by updating its right base row.  Returns itself. */
  def withRight(newRight: Row): Row = {
    row2 = newRight
    this
  }

  def iterator = row1.iterator ++ row2.iterator

  def length = row1.length + row2.length

  def apply(i: Int) =
    if (i < row1.size) row1(i) else row2(i - row1.size)

  def isNullAt(i: Int) =
    if (i < row1.size) row1.isNullAt(i) else row2.isNullAt(i - row1.size)

  def getInt(i: Int): Int =
    if (i < row1.size) row1.getInt(i) else row2.getInt(i - row1.size)

  def getLong(i: Int): Long =
    if (i < row1.size) row1.getLong(i) else row2.getLong(i - row1.size)

  def getDouble(i: Int): Double =
    if (i < row1.size) row1.getDouble(i) else row2.getDouble(i - row1.size)

  def getBoolean(i: Int): Boolean =
    if (i < row1.size) row1.getBoolean(i) else row2.getBoolean(i - row1.size)

  def getShort(i: Int): Short =
    if (i < row1.size) row1.getShort(i) else row2.getShort(i - row1.size)

  def getByte(i: Int): Byte =
    if (i < row1.size) row1.getByte(i) else row2.getByte(i - row1.size)

  def getFloat(i: Int): Float =
    if (i < row1.size) row1.getFloat(i) else row2.getFloat(i - row1.size)

  def getString(i: Int): String =
    if (i < row1.size) row1.getString(i) else row2.getString(i - row1.size)

  override def getAs[T](i: Int): T =
    if (i < row1.size) row1.getAs[T](i) else row2.getAs[T](i - row1.size)

  def copy() = {
    val totalSize = row1.size + row2.size
    val copiedValues = new Array[Any](totalSize)
    var i = 0
    while(i < totalSize) {
      copiedValues(i) = apply(i)
      i += 1
    }
    new GenericRow(copiedValues)
  }

  override def toString() = {
    val row = (if (row1 != null) row1 else Seq[Any]()) ++ (if (row2 != null) row2 else Seq[Any]())
    s"[${row.mkString(",")}]"
  }
}

/**
 * JIT HACK: Replace with macros
 */
class JoinedRow3 extends Row {
  private[this] var row1: Row = _
  private[this] var row2: Row = _

  def this(left: Row, right: Row) = {
    this()
    row1 = left
    row2 = right
  }

  /** Updates this JoinedRow to used point at two new base rows.  Returns itself. */
  def apply(r1: Row, r2: Row): Row = {
    row1 = r1
    row2 = r2
    this
  }

  /** Updates this JoinedRow by updating its left base row.  Returns itself. */
  def withLeft(newLeft: Row): Row = {
    row1 = newLeft
    this
  }

  /** Updates this JoinedRow by updating its right base row.  Returns itself. */
  def withRight(newRight: Row): Row = {
    row2 = newRight
    this
  }

  def iterator = row1.iterator ++ row2.iterator

  def length = row1.length + row2.length

  def apply(i: Int) =
    if (i < row1.size) row1(i) else row2(i - row1.size)

  def isNullAt(i: Int) =
    if (i < row1.size) row1.isNullAt(i) else row2.isNullAt(i - row1.size)

  def getInt(i: Int): Int =
    if (i < row1.size) row1.getInt(i) else row2.getInt(i - row1.size)

  def getLong(i: Int): Long =
    if (i < row1.size) row1.getLong(i) else row2.getLong(i - row1.size)

  def getDouble(i: Int): Double =
    if (i < row1.size) row1.getDouble(i) else row2.getDouble(i - row1.size)

  def getBoolean(i: Int): Boolean =
    if (i < row1.size) row1.getBoolean(i) else row2.getBoolean(i - row1.size)

  def getShort(i: Int): Short =
    if (i < row1.size) row1.getShort(i) else row2.getShort(i - row1.size)

  def getByte(i: Int): Byte =
    if (i < row1.size) row1.getByte(i) else row2.getByte(i - row1.size)

  def getFloat(i: Int): Float =
    if (i < row1.size) row1.getFloat(i) else row2.getFloat(i - row1.size)

  def getString(i: Int): String =
    if (i < row1.size) row1.getString(i) else row2.getString(i - row1.size)

  override def getAs[T](i: Int): T =
    if (i < row1.size) row1.getAs[T](i) else row2.getAs[T](i - row1.size)

  def copy() = {
    val totalSize = row1.size + row2.size
    val copiedValues = new Array[Any](totalSize)
    var i = 0
    while(i < totalSize) {
      copiedValues(i) = apply(i)
      i += 1
    }
    new GenericRow(copiedValues)
  }

  override def toString() = {
    val row = (if (row1 != null) row1 else Seq[Any]()) ++ (if (row2 != null) row2 else Seq[Any]())
    s"[${row.mkString(",")}]"
  }
}

/**
 * JIT HACK: Replace with macros
 */
class JoinedRow4 extends Row {
  private[this] var row1: Row = _
  private[this] var row2: Row = _

  def this(left: Row, right: Row) = {
    this()
    row1 = left
    row2 = right
  }

  /** Updates this JoinedRow to used point at two new base rows.  Returns itself. */
  def apply(r1: Row, r2: Row): Row = {
    row1 = r1
    row2 = r2
    this
  }

  /** Updates this JoinedRow by updating its left base row.  Returns itself. */
  def withLeft(newLeft: Row): Row = {
    row1 = newLeft
    this
  }

  /** Updates this JoinedRow by updating its right base row.  Returns itself. */
  def withRight(newRight: Row): Row = {
    row2 = newRight
    this
  }

  def iterator = row1.iterator ++ row2.iterator

  def length = row1.length + row2.length

  def apply(i: Int) =
    if (i < row1.size) row1(i) else row2(i - row1.size)

  def isNullAt(i: Int) =
    if (i < row1.size) row1.isNullAt(i) else row2.isNullAt(i - row1.size)

  def getInt(i: Int): Int =
    if (i < row1.size) row1.getInt(i) else row2.getInt(i - row1.size)

  def getLong(i: Int): Long =
    if (i < row1.size) row1.getLong(i) else row2.getLong(i - row1.size)

  def getDouble(i: Int): Double =
    if (i < row1.size) row1.getDouble(i) else row2.getDouble(i - row1.size)

  def getBoolean(i: Int): Boolean =
    if (i < row1.size) row1.getBoolean(i) else row2.getBoolean(i - row1.size)

  def getShort(i: Int): Short =
    if (i < row1.size) row1.getShort(i) else row2.getShort(i - row1.size)

  def getByte(i: Int): Byte =
    if (i < row1.size) row1.getByte(i) else row2.getByte(i - row1.size)

  def getFloat(i: Int): Float =
    if (i < row1.size) row1.getFloat(i) else row2.getFloat(i - row1.size)

  def getString(i: Int): String =
    if (i < row1.size) row1.getString(i) else row2.getString(i - row1.size)

  override def getAs[T](i: Int): T =
    if (i < row1.size) row1.getAs[T](i) else row2.getAs[T](i - row1.size)

  def copy() = {
    val totalSize = row1.size + row2.size
    val copiedValues = new Array[Any](totalSize)
    var i = 0
    while(i < totalSize) {
      copiedValues(i) = apply(i)
      i += 1
    }
    new GenericRow(copiedValues)
  }

  override def toString() = {
    val row = (if (row1 != null) row1 else Seq[Any]()) ++ (if (row2 != null) row2 else Seq[Any]())
    s"[${row.mkString(",")}]"
  }
}

/**
 * JIT HACK: Replace with macros
 */
class JoinedRow5 extends Row {
  private[this] var row1: Row = _
  private[this] var row2: Row = _

  def this(left: Row, right: Row) = {
    this()
    row1 = left
    row2 = right
  }

  /** Updates this JoinedRow to used point at two new base rows.  Returns itself. */
  def apply(r1: Row, r2: Row): Row = {
    row1 = r1
    row2 = r2
    this
  }

  /** Updates this JoinedRow by updating its left base row.  Returns itself. */
  def withLeft(newLeft: Row): Row = {
    row1 = newLeft
    this
  }

  /** Updates this JoinedRow by updating its right base row.  Returns itself. */
  def withRight(newRight: Row): Row = {
    row2 = newRight
    this
  }

  def iterator = row1.iterator ++ row2.iterator

  def length = row1.length + row2.length

  def apply(i: Int) =
    if (i < row1.size) row1(i) else row2(i - row1.size)

  def isNullAt(i: Int) =
    if (i < row1.size) row1.isNullAt(i) else row2.isNullAt(i - row1.size)

  def getInt(i: Int): Int =
    if (i < row1.size) row1.getInt(i) else row2.getInt(i - row1.size)

  def getLong(i: Int): Long =
    if (i < row1.size) row1.getLong(i) else row2.getLong(i - row1.size)

  def getDouble(i: Int): Double =
    if (i < row1.size) row1.getDouble(i) else row2.getDouble(i - row1.size)

  def getBoolean(i: Int): Boolean =
    if (i < row1.size) row1.getBoolean(i) else row2.getBoolean(i - row1.size)

  def getShort(i: Int): Short =
    if (i < row1.size) row1.getShort(i) else row2.getShort(i - row1.size)

  def getByte(i: Int): Byte =
    if (i < row1.size) row1.getByte(i) else row2.getByte(i - row1.size)

  def getFloat(i: Int): Float =
    if (i < row1.size) row1.getFloat(i) else row2.getFloat(i - row1.size)

  def getString(i: Int): String =
    if (i < row1.size) row1.getString(i) else row2.getString(i - row1.size)

  override def getAs[T](i: Int): T =
    if (i < row1.size) row1.getAs[T](i) else row2.getAs[T](i - row1.size)

  def copy() = {
    val totalSize = row1.size + row2.size
    val copiedValues = new Array[Any](totalSize)
    var i = 0
    while(i < totalSize) {
      copiedValues(i) = apply(i)
      i += 1
    }
    new GenericRow(copiedValues)
  }

  override def toString() = {
    val row = (if (row1 != null) row1 else Seq[Any]()) ++ (if (row2 != null) row2 else Seq[Any]())
    s"[${row.mkString(",")}]"
  }
}
