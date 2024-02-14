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

package org.apache.spark.sql.execution.python

import scala.collection.mutable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.UnaryExecNode

object EvalPythonExec {
  /**
   * Metadata for arguments of Python UDTF.
   *
   * @param offset the offset of the argument
   * @param name the name of the argument if it's a `NamedArgumentExpression`
   */
  case class ArgumentMetadata(offset: Int, name: Option[String])

  /**
   * Represents one row sent as input to a Python UDF.
   * The [[forwardedHiddenValues]] is an optional array of other values forwarded from the input
   * row to the output row unchanged.
   */
  abstract class InputRow(val forwardedHiddenValues: Array[Any])

  /** Represents one row sent as input to a Python UDF comprising discrete input row values. */
  case class InternalInputRow(row: InternalRow, override val forwardedHiddenValues: Array[Any])
    extends InputRow(forwardedHiddenValues)

  /** Represents one row sent as input to a Python UDF comprising serialized input row bytes. */
  case class SerializedInputRow(bytes: Array[Byte], override val forwardedHiddenValues: Array[Any])
    extends InputRow(forwardedHiddenValues)

  /**
   * Convenience method to convert an iterator of iterators of rows to the above [[InputRow]].
   * No forwarded hidden values are used here, so the second element of the tuple is an empty array.
   */
  def toInternalInputRows(iter: Iterator[Iterator[InternalRow]]): Iterator[Iterator[InputRow]] =
    iter.map { rowIter: Iterator[InternalRow] =>
      rowIter.map { row: InternalRow =>
        EvalPythonExec.InternalInputRow(row, Array.empty)
      }
    }

  /**
   * This is a wrapper over an iterator of [[InputRow]] that keeps track of the most recent
   * forwarded hidden values. This is used to insert these values into the output row iterator to
   * implement the forwarding feature by skipping sending their values to the Python interpreter.
   */
  case class InputRowIteratorWithForwardedHiddenValues(iter: Iterator[InputRow])
    extends Iterator[InputRow] {
    override def hasNext: Boolean = iter.hasNext
    override def next(): InputRow = {
      val result: InputRow = iter.next()
      mostRecentValues = result.forwardedHiddenValues
      result
    }
    def forwardedHiddenValues: Array[Any] = mostRecentValues
    private var mostRecentValues: Array[Any] = Array.empty
  }

  /**
   * This is a wrapper over an iterator of [[InternalRow]] that acts as a wrapper over
   * [[internalRowIterator]], while also assigning the most recent forwarded hidden values from the
   * provided [[inputIterator]]. By doing so, we implement the forwarding feature by skipping
   * sending their values to the Python interpreter.
   */
  case class OutputRowIteratorWithForwardedHiddenValues(
      udtf: PythonUDTF,
      internalRowIterator: Iterator[InternalRow],
      inputIterator: InputRowIteratorWithForwardedHiddenValues)
    extends Iterator[InternalRow] {
    override def hasNext: Boolean = internalRowIterator.hasNext
    override def next(): InternalRow = {
      val result: InternalRow = internalRowIterator.next()
      udtf.outputTableForwardedHiddenColumnIndexes.foreach { index: Int =>
        result.update(index, inputIterator.forwardedHiddenValues(index))
      }
      result
    }
  }

  /**
   * This method looks up values from a row by the child indexes of the [[PythonUDTFColumnIndexes]].
   * It returns a [[LookupFromRowResult]] containing the indexed values and the remaining values.
   * This is useful for separating forwarded hidden column values from rows so that we can avoid
   * sending them to/from the JVM and Python worker, for efficiency.
   */
  case class LookupFromRowResult(
      indexedValues: Array[Any],
      nonIndexedValues: Array[Any])
  def lookupIndexedColumnValuesFromRow(
      indexes: Option[PythonUDTFColumnIndexes],
      rowValues: Array[Any]): LookupFromRowResult = {
    indexes.map { p =>
      val indexedBuffer = mutable.ArrayBuffer.empty[Any]
      val remainingBuffer = mutable.ArrayBuffer.empty[Any]
      rowValues.zipWithIndex.foreach { case (value: Any, index: Int) =>
        if (p.childIndexes.contains(index)) {
          indexedBuffer += value
        } else {
          remainingBuffer += value
        }
      }
      LookupFromRowResult(
        indexedBuffer.toArray,
        remainingBuffer.toArray)
    }.getOrElse {
      LookupFromRowResult(
        indexedValues = Array.empty,
        nonIndexedValues = rowValues)
    }
  }
}

/**
 * A physical plan that evaluates a [[PythonUDF]], one partition of tuples at a time.
 *
 * Python evaluation works by sending the necessary (projected) input data via a socket to an
 * external Python process, and combine the result from the Python process with the original row.
 *
 * For each row we send to Python, we also put it in a queue first. For each output row from Python,
 * we drain the queue to find the original input row. Note that if the Python process is way too
 * slow, this could lead to the queue growing unbounded and spill into disk when run out of memory.
 *
 * Here is a diagram to show how this works:
 *
 *            Downstream (for parent)
 *             /      \
 *            /     socket  (output of UDF)
 *           /         \
 *        RowQueue    Python
 *           \         /
 *            \     socket  (input of UDF)
 *             \     /
 *          upstream (from child)
 *
 * The rows sent to and received from Python are packed into batches (100 rows) and serialized,
 * there should be always some rows buffered in the socket or Python process, so the pulling from
 * RowQueue ALWAYS happened after pushing into it.
 */
trait EvalPythonExec extends UnaryExecNode {
  def udfs: Seq[PythonUDF]
  def resultAttrs: Seq[Attribute]

  protected def evaluatorFactory: EvalPythonEvaluatorFactory

  override def output: Seq[Attribute] = child.output ++ resultAttrs

  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)

  protected override def doExecute(): RDD[InternalRow] = {
    val inputRDD = child.execute().map(_.copy())
    if (conf.usePartitionEvaluator) {
      inputRDD.mapPartitionsWithEvaluator(evaluatorFactory)
    } else {
      inputRDD.mapPartitionsWithIndexInternal { (index, iter) =>
        evaluatorFactory.createEvaluator().eval(index, iter)
      }
    }
  }
}
