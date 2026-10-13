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

import scala.jdk.CollectionConverters._

import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.filter2.columnindex.RowRanges
import org.apache.parquet.schema.LogicalTypeAnnotation

import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.util.SparkClassUtils

/**
 * Reflective bridge to package-private classes in
 * `org.apache.spark.sql.execution.datasources.parquet`, and to private state of its readers.
 * Under `spark-submit --jars`, test and main classes load from different classloaders, blocking
 * package-private access despite the matching package name. Reflection with
 * `setAccessible(true)` sidesteps the check without widening production visibility. So a
 * package-private class is looked up by name here, never named in a `classOf` or a cast.
 *
 * Currently bridges:
 *   - `ParquetReadState` (its factory + `resetForNewBatch` + `resetForNewPage`)
 *   - `VectorizedRleValuesReader.readBatch` (5-arg overload not exposed publicly)
 *   - `ParquetVectorUpdaterFactory` (constructor)
 *   - `VectorizedDeltaByteArrayReader` (no-arg constructor)
 *   - `VectorizedDeltaLengthByteArrayReader` (no-arg constructor)
 *   - `LateMaterializationParquetRecordReader`'s scratch vectors and queued survivor sets, the
 *     column readers and row indexes a reader still holds, and a vector's byte-child capacity
 *     (private state a test checks the reader's memory by)
 */
object ParquetTestAccess {

  // -------- ParquetReadState --------

  private val stateCls = SparkClassUtils.classForName[Any](
    "org.apache.spark.sql.execution.datasources.parquet.ParquetReadState")

  private val stateForRead = {
    val m = stateCls.getDeclaredMethod(
      "forRead",
      classOf[ColumnDescriptor],
      java.lang.Boolean.TYPE,
      classOf[RowRanges],
      classOf[PrimitiveIterator.OfLong])
    m.setAccessible(true)
    m
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

  /**
   * A read state over `descriptor`, told which rows to include the same way production tells it, as
   * ranges when the caller holds them, else as the row indexes a page store handed out, else not at
   * all, which means every row of the chunk. Ranges come with the row indexes, as they do whenever
   * production reads ranges out of a store that indexes its rows, and then the ranges are what it
   * walks. Ranges alone are refused.
   */
  def newState(
      descriptor: ColumnDescriptor,
      isRequired: Boolean,
      rowIndexes: PrimitiveIterator.OfLong = null,
      rowRanges: RowRanges = null): AnyRef = {
    try {
      stateForRead.invoke(null, descriptor, Boolean.box(isRequired), rowRanges, rowIndexes)
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

  // -------- LateMaterializationParquetRecordReader --------

  // Lazy, so a renamed member fails only the tests that reach it, not every user of this
  // object at its initialization.

  private lazy val keyScratchVectorsField = {
    val f = classOf[LateMaterializationParquetRecordReader].getDeclaredField("keyScratchVectors")
    f.setAccessible(true)
    f
  }

  /** The vectors phase 1 decodes key pages into. */
  def keyScratchVectors(reader: LateMaterializationParquetRecordReader): Seq[WritableColumnVector] =
    keyScratchVectorsField.get(reader).asInstanceOf[Array[WritableColumnVector]].toSeq

  private lazy val survivorBatchesField = {
    val f = classOf[LateMaterializationParquetRecordReader].getDeclaredField("survivorBatches")
    f.setAccessible(true)
    f
  }

  /** The survivor sets phase 1 queued and the reader has not emitted yet, oldest first. */
  def queuedSurvivorSets(
      reader: LateMaterializationParquetRecordReader): Seq[Seq[WritableColumnVector]] =
    survivorBatchesField.get(reader)
      .asInstanceOf[java.util.ArrayDeque[Array[WritableColumnVector]]].asScala.map(_.toSeq).toSeq

  private lazy val columnVectorsField = {
    val f = classOf[VectorizedParquetRecordReader].getDeclaredField("columnVectors")
    f.setAccessible(true)
    f
  }

  private lazy val columnVectorCls = SparkClassUtils.classForName[Any](
    "org.apache.spark.sql.execution.datasources.parquet.ParquetColumnVector")

  private lazy val getLeavesMethod = {
    val m = columnVectorCls.getDeclaredMethod("getLeaves")
    m.setAccessible(true)
    m
  }

  private lazy val getColumnReaderMethod = {
    val m = columnVectorCls.getDeclaredMethod("getColumnReader")
    m.setAccessible(true)
    m
  }

  /** How many leaf columns of the reader's batch still hold a column reader. */
  def columnReadersHeld(reader: VectorizedParquetRecordReader): Int =
    columnVectorsField.get(reader).asInstanceOf[Array[AnyRef]].iterator
      .flatMap(v => getLeavesMethod.invoke(v).asInstanceOf[java.util.List[AnyRef]].asScala)
      .count(leaf => getColumnReaderMethod.invoke(leaf) != null)

  private lazy val rowIndexGeneratorField = {
    val f = classOf[VectorizedParquetRecordReader].getDeclaredField("rowIndexGenerator")
    f.setAccessible(true)
    f
  }

  /** Whether the reader's row-index generator still holds the row indexes of a row group. */
  def rowIndexesHeld(reader: VectorizedParquetRecordReader): Boolean = {
    val generator = rowIndexGeneratorField.get(reader)
      .asInstanceOf[ParquetRowIndexUtil.RowIndexGenerator]
    require(generator != null, "the reader must generate row indexes")
    generator.rowIndexIterator != null
  }

  private lazy val capacityField = {
    val f = classOf[WritableColumnVector].getDeclaredField("capacity")
    f.setAccessible(true)
    f
  }

  /** How many bytes a variable-length vector's byte child holds before it has to grow. */
  def byteChildCapacity(vector: WritableColumnVector): Int =
    capacityField.getInt(vector.arrayData())

  // -------- shared helper --------

  // What the reflected call itself threw, checked or not, so a test sees the method's own error.
  private def rethrow(e: ReflectiveOperationException): Throwable = e match {
    case ite: InvocationTargetException => ite.getCause
    case other => other
  }
}
