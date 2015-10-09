package org.apache.spark.sql.execution.local

import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute

/**
 * An operator that scans data from an Scala Iterator.
 */
case class IteratorScanNode(
    conf: SQLConf,
    output: Seq[Attribute])
  extends LeafLocalNode(conf) {

  private[this] var iterator: Iterator[InternalRow] = _
  private[this] var currentRow: InternalRow = _

  def withIterator(inputIterator: Iterator[InternalRow]) = {
    iterator = inputIterator
  }

  override def open(): Unit = {
    // Do nothing
  }

  override def next(): Boolean = {
    if (iterator.hasNext) {
      currentRow = iterator.next()
      true
    } else {
      false
    }
  }

  override def fetch(): InternalRow = currentRow

  override def close(): Unit = {
    // Do nothing
  }
}
