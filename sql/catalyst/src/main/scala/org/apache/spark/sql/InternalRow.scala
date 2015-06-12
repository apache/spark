package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.GenericRow

/**
 * An abstract class for row used internal in Spark SQL, which only contain the columns as
 * internal types.
 */
abstract class InternalRow extends Row {
  // A default implementation to change the return type
  override def copy(): InternalRow = {this}
}

object InternalRow {
  def unapplySeq(row: InternalRow): Some[Seq[Any]] = Some(row.toSeq)

  /**
   * This method can be used to construct a [[Row]] with the given values.
   */
  def apply(values: Any*): InternalRow = new GenericRow(values.toArray)

  /**
   * This method can be used to construct a [[Row]] from a [[Seq]] of values.
   */
  def fromSeq(values: Seq[Any]): InternalRow = new GenericRow(values.toArray)

  def fromTuple(tuple: Product): InternalRow = fromSeq(tuple.productIterator.toSeq)

  /**
   * Merge multiple rows into a single row, one after another.
   */
  def merge(rows: InternalRow*): InternalRow = {
    // TODO: Improve the performance of this if used in performance critical part.
    new GenericRow(rows.flatMap(_.toSeq).toArray)
  }

  /** Returns an empty row. */
  val empty = apply()
}