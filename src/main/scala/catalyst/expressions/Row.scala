package catalyst
package expressions

import types._

/**
 * Represents one row of output from a relational operator.  Allows both generic access by ordinal,
 * which will incur boxing overhead for primitives, as well as native primitive access.
 *
 * It is invalid to use the native primitive interface to retrieve a value that is null, instead a
 * user must check [[isNullAt]] before attempting to retrieve a value that might be null.
 */
abstract class Row extends Seq[Any] with Serializable {
  def apply(i: Int): Any

  def isNullAt(i: Int): Boolean

  def getInt(i: Int): Int
  def getLong(i: Int): Long
  def getDouble(i: Int): Double
  def getBoolean(i: Int): Boolean
  def getShort(i: Int): Short
  def getByte(i: Int): Byte
}

/**
 * A row with no data.  Calling any methods will result in an error.  Can be used as a placeholder.
 */
object EmptyRow extends Row {
  def apply(i: Int): Any = throw new UnsupportedOperationException

  def iterator = Iterator.empty
  def length = 0
  def isNullAt(i: Int): Boolean = throw new UnsupportedOperationException

  def getInt(i: Int): Int = throw new UnsupportedOperationException
  def getLong(i: Int): Long = throw new UnsupportedOperationException
  def getDouble(i: Int): Double = throw new UnsupportedOperationException
  def getBoolean(i: Int): Boolean = throw new UnsupportedOperationException
  def getShort(i: Int): Short = throw new UnsupportedOperationException
  def getByte(i: Int): Byte = throw new UnsupportedOperationException
}

/**
 * A row implementation that uses an array of objects as the underlying storage.
 */
class GenericRow(input: Seq[Any]) extends Row {
  val values = input.toIndexedSeq

  def iterator = values.iterator
  def length = values.length

  def apply(i: Int) = values(i)

  def isNullAt(i: Int) = values(i) == null

  def getInt(i: Int): Int = {
    if (values(i) == null) sys.error("Failed to check null bit for primitive int value.")
    values(i).asInstanceOf[Int]
  }
  def getLong(i: Int): Long = {
    if (values(i) == null) sys.error("Failed to check null bit for primitive long value.")
    values(i).asInstanceOf[Long]
  }
  def getDouble(i: Int): Double = {
    if (values(i) == null) sys.error("Failed to check null bit for primitive double value.")
    values(i).asInstanceOf[Double]
  }
  def getBoolean(i: Int): Boolean = {
    if (values(i) == null) sys.error("Failed to check null bit for primitive boolean value.")
    values(i).asInstanceOf[Boolean]
  }
  def getShort(i: Int): Short = {
    if (values(i) == null) sys.error("Failed to check null bit for primitive short value.")
    values(i).asInstanceOf[Short]
  }
  def getByte(i: Int): Byte = {
    if (values(i) == null) sys.error("Failed to check null bit for primitive byte value.")
    values(i).asInstanceOf[Byte]
  }
}

object OrderedRow {
  def apply(ordering: Seq[SortOrder], input: Iterator[Row]): Iterator[(OrderedRow, Row)] =  {
    val expressions = ordering.map(_.child)
    val orderingObjects = ordering.map { o =>
      o.dataType match {
        case nativeType: NativeType =>
          if(o.direction == Ascending)
            nativeType.ordering.asInstanceOf[Ordering[Any]]
          else
            nativeType.ordering.asInstanceOf[Ordering[Any]].reverse
        case _ => sys.error(s"No ordering available for ${o.dataType}")
      }
    }
    val directions = ordering.map(_.direction)

    input.map { row =>
      (new OrderedRow(orderingObjects, directions, expressions.map(Evaluate(_, Vector(row)))), row)
    }
  }
}

class OrderedRow(ordering: Seq[Ordering[Any]], directions: Seq[SortDirection], input: Seq[Any])
    extends GenericRow(input) with Ordered[OrderedRow] {

  def compare(other: OrderedRow): Int = {
    var i = 0
    while (i < values.size) {
      val left = values(i)
      val right = other.values(i)

      val comparison =
        if (left == null && right == null) {
          0
        } else if (left == null) {
          if (directions(i) == Ascending) -1 else 1
        } else if (right == null) {
          if (directions(i) == Ascending) 1 else -1
        } else {
          ordering(i).compare(left, right)
        }
      if (comparison != 0) return comparison
      i += 1
    }
    return 0
  }
}