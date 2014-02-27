package org.apache.spark.sql.catalyst
package expressions

/**
 * Converts a Row to another Row given a set of expressions.
 *
 * If the schema of the input row is specified, then the given expression will be bound to that
 * schema.
 */
class Projection(expressions: Seq[Expression]) extends (Row => Row) {
  def this(expressions: Seq[Expression], inputSchema: Seq[Attribute]) =
    this(expressions.map(BindReferences.bindReference(_, inputSchema)))

  protected val exprArray = expressions.toArray
  def apply(input: Row): Row = {
    val outputArray = new Array[Any](exprArray.size)
    var i = 0
    while (i < exprArray.size) {
      outputArray(i) = exprArray(i).apply(input)
      i += 1
    }
    new GenericRow(outputArray)
  }
}

/**
 * A mutable wrapper that makes two rows appear appear as a single concatenated row.  Designed to
 * be instantiated once per thread and reused.
 */
class JoinedRow extends Row {
  private var row1: Row = _
  private var row2: Row = _

  def apply(r1: Row, r2: Row): Row = {
    row1 = r1
    row2 = r2
    this
  }

  def iterator = row1.iterator ++ row2.iterator

  def length = row1.length + row2.length

  def apply(i: Int) =
    if (i < row1.size) row1(i) else row2(i - row1.size)

  def isNullAt(i: Int) = apply(i) == null

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
}
