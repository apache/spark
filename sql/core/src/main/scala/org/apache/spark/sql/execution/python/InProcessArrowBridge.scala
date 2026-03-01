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

import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}
import org.apache.arrow.vector.FieldVector

import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ArrowColumnVector

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
 *   its own reference, so buffers remain live until [[ArrowWriter]] resets for the next batch.
 *
 * Output path (Python to JVM, zero-copy via CDI):
 *   JVM pre-allocates [[ArrowArray]] and [[ArrowSchema]] C structs. Python calls
 *   ``arr._export_to_c(array_ptr, schema_ptr)`` to fill those structs in-place. The JVM
 *   calls [[Data.importVector]] to reconstruct the [[FieldVector]] without copying. When the
 *   imported [[FieldVector]] is closed, Arrow Java invokes PyArrow's CDI release callback,
 *   decrementing the Python array refcount and allowing garbage collection.
 *
 * Because CDI carries the full Arrow schema, all Arrow types are supported on both paths.
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
   * Caller must close ``outArray`` and ``outSchema`` after [[InProcessPythonRuntime.invoke]]
   * returns (by which time Python's ``_import_from_c`` has already consumed the structs).
   */
  def exportColumn(vector: FieldVector, outArray: ArrowArray, outSchema: ArrowSchema): Unit =
    Data.exportVector(ArrowUtils.rootAllocator, vector, null, outArray, outSchema)

  /**
   * Reconstruct an [[ArrowColumnVector]] from JVM-allocated Arrow C Data Interface structs.
   *
   * The JVM pre-allocates [[ArrowArray]] and [[ArrowSchema]] before invoking Python.
   * Python fills them via ``arr._export_to_c(array_ptr, schema_ptr)``. This method
   * calls [[Data.importVector]] to wrap Python's Arrow buffers (zero-copy).
   *
   * Lifecycle:
   *  - [[Data.importVector]] internally calls ``ArrayImporter.importArray()``, which
   *    copies the struct snapshot, calls ``markReleased()`` + ``close()`` on ``arrowArray``
   *    (idempotent -- caller's try-finally close is safe), and wraps the data buffers via
   *    ``ReferenceCountedArrowArray`` (ForeignAllocation, zero-copy).
   *  - [[ArrowSchema]] is NOT closed by importVector; the caller must close it.
   *  - When the returned [[ArrowColumnVector]] is closed, the reference count drops to
   *    zero, PyArrow's C ``release`` callback is invoked, and the Python array is GC'd.
   */
  def cdiToColumn(arrowArray: ArrowArray, arrowSchema: ArrowSchema): ArrowColumnVector = {
    val vector = Data.importVector(ArrowUtils.rootAllocator, arrowArray, arrowSchema, null)
    new ArrowColumnVector(vector)
  }
}
