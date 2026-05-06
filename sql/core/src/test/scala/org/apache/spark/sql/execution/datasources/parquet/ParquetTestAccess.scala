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

package org.apache.spark.sql.execution.datasources.parquet

import java.lang.reflect.{InvocationTargetException, Method}
import java.time.ZoneId
import java.util.PrimitiveIterator

import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.schema.LogicalTypeAnnotation

import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.util.SparkClassUtils

/**
 * Reflective bridge to package-private classes in
 * `org.apache.spark.sql.execution.datasources.parquet`. Under `spark-submit --jars`, test
 * and main classes load from different classloaders, blocking package-private access despite
 * the matching package name. Reflection with `setAccessible(true)` sidesteps the check
 * without widening production visibility.
 *
 * Currently bridges:
 *   - `ParquetReadState` (constructor + `resetForNewBatch` + `resetForNewPage`)
 *   - `VectorizedRleValuesReader.readBatch` (5-arg overload not exposed publicly)
 *   - `ParquetVectorUpdaterFactory` (constructor)
 *   - `VectorizedDeltaByteArrayReader` (no-arg constructor)
 *   - `VectorizedDeltaLengthByteArrayReader` (no-arg constructor)
 */
object ParquetTestAccess {

  // -------- ParquetReadState --------

  private val stateCls = SparkClassUtils.classForName[Any](
    "org.apache.spark.sql.execution.datasources.parquet.ParquetReadState")

  private val stateCtor = {
    val c = stateCls.getDeclaredConstructor(
      classOf[ColumnDescriptor],
      java.lang.Boolean.TYPE,
      classOf[PrimitiveIterator.OfLong])
    c.setAccessible(true)
    c
  }

  private val resetForNewBatchMethod = {
    val m = stateCls.getDeclaredMethod("resetForNewBatch", Integer.TYPE)
    m.setAccessible(true)
    m
  }

  private val resetForNewPageMethod = {
    val m = stateCls.getDeclaredMethod(
      "resetForNewPage", Integer.TYPE, java.lang.Long.TYPE)
    m.setAccessible(true)
    m
  }

  private val readBatchMethod: Method =
    classOf[VectorizedRleValuesReader].getMethods
      .find(m =>
        m.getName == "readBatch"
          && m.getParameterCount == 5
          && m.getParameterTypes()(0) == stateCls)
      .getOrElse(throw new NoSuchMethodException(
        "VectorizedRleValuesReader.readBatch/5"))

  def newState(
      descriptor: ColumnDescriptor,
      isRequired: Boolean,
      rowIndexes: PrimitiveIterator.OfLong = null): AnyRef = {
    try {
      stateCtor.newInstance(
        descriptor,
        Boolean.box(isRequired),
        rowIndexes).asInstanceOf[AnyRef]
    } catch {
      case e: ReflectiveOperationException => throw rethrow(e)
    }
  }

  def resetForNewBatch(state: AnyRef, batchSize: Int): Unit =
    try { resetForNewBatchMethod.invoke(state, Int.box(batchSize)) }
    catch { case e: ReflectiveOperationException => throw rethrow(e) }

  def resetForNewPage(
      state: AnyRef,
      totalValuesInPage: Int,
      pageFirstRowIndex: Long): Unit =
    try {
      resetForNewPageMethod.invoke(
        state, Int.box(totalValuesInPage), Long.box(pageFirstRowIndex))
    } catch { case e: ReflectiveOperationException => throw rethrow(e) }

  def readBatch(
      reader: VectorizedRleValuesReader,
      state: AnyRef,
      values: WritableColumnVector,
      defLevels: WritableColumnVector,
      valueReader: VectorizedValuesReader,
      updater: ParquetVectorUpdater): Unit =
    try {
      readBatchMethod.invoke(
        reader, state, values, defLevels, valueReader, updater)
    } catch { case e: ReflectiveOperationException => throw rethrow(e) }

  // -------- ParquetVectorUpdaterFactory --------

  private val factoryCtor = {
    val cls = SparkClassUtils.classForName[Any](
      "org.apache.spark.sql.execution.datasources.parquet.ParquetVectorUpdaterFactory")
    val c = cls.getDeclaredConstructor(
      classOf[LogicalTypeAnnotation],
      classOf[ZoneId],
      classOf[String],
      classOf[String],
      classOf[String],
      classOf[String])
    c.setAccessible(true)
    c
  }

  def newFactory(
      logicalTypeAnnotation: LogicalTypeAnnotation,
      convertTz: ZoneId,
      datetimeRebaseMode: String,
      datetimeRebaseTz: String,
      int96RebaseMode: String,
      int96RebaseTz: String): ParquetVectorUpdaterFactory = {
    try {
      factoryCtor.newInstance(
        logicalTypeAnnotation, convertTz,
        datetimeRebaseMode, datetimeRebaseTz,
        int96RebaseMode, int96RebaseTz).asInstanceOf[ParquetVectorUpdaterFactory]
    } catch {
      case e: ReflectiveOperationException => throw rethrow(e)
    }
  }

  // -------- VectorizedDeltaByteArrayReader / VectorizedDeltaLengthByteArrayReader --------

  private val deltaByteArrayCtor = {
    val c = classOf[VectorizedDeltaByteArrayReader].getDeclaredConstructor()
    c.setAccessible(true)
    c
  }

  private val deltaLengthByteArrayCtor = {
    val c = classOf[VectorizedDeltaLengthByteArrayReader].getDeclaredConstructor()
    c.setAccessible(true)
    c
  }

  def newDeltaByteArrayReader(): VectorizedDeltaByteArrayReader =
    try { deltaByteArrayCtor.newInstance() }
    catch { case e: ReflectiveOperationException => throw rethrow(e) }

  def newDeltaLengthByteArrayReader(): VectorizedDeltaLengthByteArrayReader =
    try { deltaLengthByteArrayCtor.newInstance() }
    catch { case e: ReflectiveOperationException => throw rethrow(e) }

  // -------- shared helper --------

  private def rethrow(e: ReflectiveOperationException): RuntimeException = {
    val cause = e match {
      case ite: InvocationTargetException => ite.getCause
      case other => other
    }
    cause match {
      case re: RuntimeException => throw re
      case er: Error => throw er
      case _ => throw new RuntimeException(cause)
    }
  }
}
