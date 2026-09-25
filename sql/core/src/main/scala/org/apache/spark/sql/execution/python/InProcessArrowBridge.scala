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

import scala.jdk.CollectionConverters._

import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}
import org.apache.arrow.memory.util.MemoryUtil
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.types.pojo.Field

import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ArrowColumnVector
import org.apache.spark.util.Utils

/**
 * Bridges JVM Arrow column buffers with Python PyArrow arrays for in-process UDF execution.
 *
 * Both input and output paths use the Arrow C Data Interface (CDI) for zero-copy transfer.
 *
 * Input path (JVM to Python, zero-copy via CDI):
 *   JVM pre-allocates [[ArrowArray]] and [[ArrowSchema]] C structs and exports each input
 *   [[FieldVector]] into them via [[Data.exportVector]]. The native addresses are passed to
 *   Python. Python calls ``pa.Array._import_from_c(array_ptr, schema_ptr)`` to wrap the
 *   same Arrow buffers as a PyArrow array -- no memcpy. When Python GCs the array, the CDI
 *   release callback decrements the buffer reference counts; the JVM [[FieldVector]] retains
 *   its own reference. Each batch uses new vectors; closing the old vectors releases only the
 *   JVM's references, leaving any arrays retained by Python valid and unchanged.
 *
 * Output path (Python to JVM, zero-copy via CDI):
 *   JVM pre-allocates [[ArrowArray]] and [[ArrowSchema]] C structs. Python calls
 *   ``arr._export_to_c(array_ptr, schema_ptr)`` to fill those structs in-place. The JVM
 *   calls [[Data.importIntoVector]] to reconstruct the [[FieldVector]] without copying. When the
 *   imported [[FieldVector]] is closed, Arrow Java invokes PyArrow's CDI release callback,
 *   decrementing the Python array refcount and allowing garbage collection.
 *
 * The runtime validates the returned schema before ArrowColumnVector reads the buffers.
 */
private[python] object InProcessArrowBridge {

  /**
   * Export a [[FieldVector]] to pre-allocated Arrow C Data Interface structs.
   *
   * Fills ``outArray`` and ``outSchema`` with the CDI representation of ``vector``.
   * The export is zero-copy: ``outArray``'s buffer pointers reference the same off-heap
   * memory as ``vector``. The CDI release callback (invoked when the Python-side imported
   * array is GC'd) decrements the buffer reference counts; the [[FieldVector]] continues
   * to hold its own reference.
   *
   * Caller must release any unconsumed exports and close both structs on every exit path.
   */
  def exportColumn(vector: FieldVector, outArray: ArrowArray, outSchema: ArrowSchema): Unit =
    Data.exportVector(ArrowUtils.rootAllocator, vector, null, outArray, outSchema)

  /**
   * Reconstruct an [[ArrowColumnVector]] from JVM-allocated Arrow C Data Interface structs.
   *
   * The JVM pre-allocates [[ArrowArray]] and [[ArrowSchema]] before invoking Python.
   * Python fills them via ``arr._export_to_c(array_ptr, schema_ptr)``. This method
   * calls [[Data.importIntoVector]] to wrap Python's Arrow buffers (zero-copy).
   *
   * Lifecycle:
   *  - [[Data.importIntoVector]] internally calls ``ArrayImporter.importArray()``, which
   *    moves the struct snapshot through a non-owning wrapper, leaving the caller's struct
   *    storage alive for cleanup, and wraps the data buffers via
   *    ``ReferenceCountedArrowArray`` (ForeignAllocation, zero-copy).
   *  - Data.importField releases and closes a non-owning schema wrapper too.
   *    The caller closes the original struct storage.
   *  - When the returned [[ArrowColumnVector]] is closed, the reference count drops to
   *    zero, PyArrow's C ``release`` callback is invoked, and the Python array is GC'd.
   */
  private def checkOffsets(array: ArrowArray): Unit = {
    val snapshot = array.snapshot()
    require(snapshot.offset == 0L, "In-process UDF returned an unsupported Arrow CDI offset")
    (0L until snapshot.n_children).foreach { i =>
      checkOffsets(ArrowArray.wrap(MemoryUtil.getLong(snapshot.children + i * 8L)))
    }
    if (snapshot.dictionary != 0L) checkOffsets(ArrowArray.wrap(snapshot.dictionary))
  }

  private def sameLayout(actual: Field, expected: Field): Boolean = {
    actual.getType == expected.getType && actual.getDictionary == expected.getDictionary &&
      actual.getChildren.size == expected.getChildren.size &&
      actual.getChildren.asScala.zip(expected.getChildren.asScala).forall { case (a, e) =>
        a.getName == e.getName && a.isNullable == e.isNullable && sameLayout(a, e)
      }
  }

  def cdiToColumn(
      arrowArray: ArrowArray,
      arrowSchema: ArrowSchema,
      expected: Option[Field] = None): ArrowColumnVector = {
    checkOffsets(arrowArray)
    val field = Data.importField(
      ArrowUtils.rootAllocator, ArrowSchema.wrap(arrowSchema.memoryAddress()), null)
    expected.foreach { declared =>
      require(sameLayout(field, declared),
        s"In-process UDF returned Arrow field $field; expected $declared")
    }
    // Array CDI has no top-level field metadata. Nested JSON metadata can also differ in
    // whitespace. After checking the physical layout, use the declared field to retain Spark
    // precision and logical type metadata; Python has already validated logical nullability.
    val vector = expected.getOrElse(field).createVector(ArrowUtils.rootAllocator)
    try {
      Data.importIntoVector(
        ArrowUtils.rootAllocator, ArrowArray.wrap(arrowArray.memoryAddress()), vector, null)
      new ArrowColumnVector(vector)
    } catch {
      case t: Throwable =>
        Utils.tryWithSafeFinally { throw t } { vector.close() }
    }
  }
}
