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

package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{UnsafeArrayData, UnsafeRow}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.unsafe.Platform

class BufferHolderSuite extends SparkFunSuite {

  test("SPARK-16071 Check the size limit to avoid integer overflow") {
    var e = intercept[UnsupportedOperationException] {
      new BufferHolder(new UnsafeRow(Int.MaxValue / 8))
    }
    assert(e.getMessage.contains("too many fields"))

    val holder = new BufferHolder(new UnsafeRow(1000))
    holder.reset()
    holder.grow(1000)
    e = intercept[UnsupportedOperationException] {
      holder.grow(Integer.MAX_VALUE)
    }
    assert(e.getMessage.contains("exceeds size limitation"))
  }

  def performUnsafeArrayWriter(length: Int, elementSize: Int, f: (UnsafeArrayWriter) => Unit):
  UnsafeArrayData = {
    val unsafeRow = new UnsafeRow(1)
    val unsafeArrayWriter = new UnsafeArrayWriter
    val bufferHolder = new BufferHolder(unsafeRow, 32)
    bufferHolder.reset()
    val cursor = bufferHolder.cursor
    unsafeArrayWriter.initialize(bufferHolder, length, elementSize)
    // execute UnsafeArrayWriter.foo() in f()
    f(unsafeArrayWriter)

    val unsafeArray = new UnsafeArrayData
    unsafeArray.pointTo(bufferHolder.buffer, cursor.toLong, bufferHolder.cursor - cursor)
    assert(unsafeArray.numElements() == length)
    unsafeArray
  }

  def initializeUnsafeArrayData(data: Seq[Any], elementSize: Int):
  UnsafeArrayData = {
    val length = data.length
    val unsafeArray = new UnsafeArrayData
    val headerSize = UnsafeArrayData.calculateHeaderPortionInBytes(length)
    val size = headerSize + elementSize * length
    val buffer = new Array[Byte](size)
    Platform.putInt(buffer, Platform.BYTE_ARRAY_OFFSET, length)
    unsafeArray.pointTo(buffer, Platform.BYTE_ARRAY_OFFSET, size)
    assert(unsafeArray.numElements == length)
    data.zipWithIndex.map { case (e, i) =>
      val offset = Platform.BYTE_ARRAY_OFFSET + headerSize + elementSize * i
      e match {
        case _ : Boolean => Platform.putBoolean(buffer, offset, e.asInstanceOf[Boolean])
        case _ : Byte => Platform.putByte(buffer, offset, e.asInstanceOf[Byte])
        case _ : Short => Platform.putShort(buffer, offset, e.asInstanceOf[Short])
        case _ : Int => Platform.putInt(buffer, offset, e.asInstanceOf[Int])
        case _ : Long => Platform.putLong(buffer, offset, e.asInstanceOf[Long])
        case _ : Float => Platform.putFloat(buffer, offset, e.asInstanceOf[Float])
        case _ : Double => Platform.putDouble(buffer, offset, e.asInstanceOf[Double])
        case _ => throw new UnsupportedOperationException()
      }
    }
    unsafeArray
  }

  val booleanData = Seq(true, false)
  val byteData = Seq(0.toByte, 1.toByte, Byte.MaxValue, Byte.MinValue)
  val shortData = Seq(0.toShort, 1.toShort, Short.MaxValue, Short.MinValue)
  val intData = Seq(0, 1, -1, Int.MaxValue, Int.MinValue)
  val longData = Seq(0.toLong, 1.toLong, -1.toLong, Long.MaxValue, Long.MinValue)
  val floatData = Seq(0.toFloat, 1.1.toFloat, -1.1.toFloat, Float.MaxValue, Float.MinValue)
  val doubleData = Seq(0.toDouble, 1.1.toDouble, -1.1.toDouble, Double.MaxValue, Double.MinValue)

  test("UnsafeArrayDataWriter write") {
    val boolUnsafeArray = performUnsafeArrayWriter(booleanData.length, 1,
      (writer: UnsafeArrayWriter) => booleanData.zipWithIndex.map {
        case (e, i) => writer.write(i, e) })
    booleanData.zipWithIndex.map { case (e, i) => assert(boolUnsafeArray.getBoolean(i) == e) }

    val byteUnsafeArray = performUnsafeArrayWriter(byteData.length, 1,
      (writer: UnsafeArrayWriter) => byteData.zipWithIndex.map {
        case (e, i) => writer.write(i, e) })
    byteData.zipWithIndex.map { case (e, i) => assert(byteUnsafeArray.getByte(i) == e) }

    val shortUnsafeArray = performUnsafeArrayWriter(shortData.length, 2,
      (writer: UnsafeArrayWriter) => shortData.zipWithIndex.map {
        case (e, i) => writer.write(i, e) })
    shortData.zipWithIndex.map { case (e, i) => assert(shortUnsafeArray.getShort(i) == e) }

    val intUnsafeArray = performUnsafeArrayWriter(intData.length, 4,
      (writer: UnsafeArrayWriter) => intData.zipWithIndex.map {
        case (e, i) => writer.write(i, e) })
    intData.zipWithIndex.map { case (e, i) => assert(intUnsafeArray.getInt(i) == e) }

    val longUnsafeArray = performUnsafeArrayWriter(longData.length, 8,
      (writer: UnsafeArrayWriter) => longData.zipWithIndex.map {
        case (e, i) => writer.write(i, e) })
    longData.zipWithIndex.map { case (e, i) => assert(longUnsafeArray.getLong(i) == e) }

    val floatUnsafeArray = performUnsafeArrayWriter(floatData.length, 8,
      (writer: UnsafeArrayWriter) => floatData.zipWithIndex.map {
        case (e, i) => writer.write(i, e) })
    floatData.zipWithIndex.map { case (e, i) => assert(floatUnsafeArray.getFloat(i) == e) }

    val doubleUnsafeArray = performUnsafeArrayWriter(doubleData.length, 8,
      (writer: UnsafeArrayWriter) => doubleData.zipWithIndex.map {
        case (e, i) => writer.write(i, e) })
    doubleData.zipWithIndex.map { case (e, i) => assert(doubleUnsafeArray.getDouble(i) == e) }
  }

  test("toPrimitiveArray") {
    val booleanUnsafeArray = initializeUnsafeArrayData(booleanData, 1)
    booleanUnsafeArray.toBooleanArray().
      zipWithIndex.map { case (e, i) => assert(e == booleanData(i)) }

    val byteUnsafeArray = initializeUnsafeArrayData(byteData, 1)
    byteUnsafeArray.toByteArray().zipWithIndex.map { case (e, i) => assert(e == byteData(i)) }

    val shortUnsafeArray = initializeUnsafeArrayData(shortData, 2)
    shortUnsafeArray.toShortArray().zipWithIndex.map { case (e, i) => assert(e == shortData(i)) }

    val intUnsafeArray = initializeUnsafeArrayData(intData, 4)
    intUnsafeArray.toIntArray().zipWithIndex.map { case (e, i) => assert(e == intData(i)) }

    val longUnsafeArray = initializeUnsafeArrayData(longData, 8)
    longUnsafeArray.toLongArray().zipWithIndex.map { case (e, i) => assert(e == longData(i)) }

    val floatUnsafeArray = initializeUnsafeArrayData(floatData, 4)
    floatUnsafeArray.toFloatArray().zipWithIndex.map { case (e, i) => assert(e == floatData(i)) }

    val doubleUnsafeArray = initializeUnsafeArrayData(doubleData, 8)
    doubleUnsafeArray.toDoubleArray().
      zipWithIndex.map { case (e, i) => assert(e == doubleData(i)) }
  }

  test("fromPrimitiveArray") {
    val booleanArray = booleanData.toArray
    val booleanUnsafeArray = UnsafeArrayData.fromPrimitiveArray(booleanArray)
    booleanArray.zipWithIndex.map { case (e, i) => assert(booleanUnsafeArray.getBoolean(i) == e) }

    val byteArray = byteData.toArray
    val byteUnsafeArray = UnsafeArrayData.fromPrimitiveArray(byteArray)
    byteArray.zipWithIndex.map { case (e, i) => assert(byteUnsafeArray.getByte(i) == e) }

    val shortArray = shortData.toArray
    val shortUnsafeArray = UnsafeArrayData.fromPrimitiveArray(shortArray)
    shortArray.zipWithIndex.map { case (e, i) => assert(shortUnsafeArray.getShort(i) == e) }

    val intArray = intData.toArray
    val intUnsafeArray = UnsafeArrayData.fromPrimitiveArray(intArray)
    intArray.zipWithIndex.map { case (e, i) => assert(intUnsafeArray.getInt(i) == e) }

    val longArray = longData.toArray
    val longUnsafeArray = UnsafeArrayData.fromPrimitiveArray(longArray)
    longArray.zipWithIndex.map { case (e, i) => assert(longUnsafeArray.getLong(i) == e) }

    val floatArray = floatData.toArray
    val floatUnsafeArray = UnsafeArrayData.fromPrimitiveArray(floatArray)
    floatArray.zipWithIndex.map { case (e, i) => assert(floatUnsafeArray.getFloat(i) == e) }

    val doubleArray = doubleData.toArray
    val doubleUnsafeArray = UnsafeArrayData.fromPrimitiveArray(doubleArray)
    doubleArray.zipWithIndex.map { case (e, i) => assert(doubleUnsafeArray.getDouble(i) == e) }
  }

  test("writePrimitiveArray") {
    val booleanArray = booleanData.toArray
    val booleanUnsafeArray = performUnsafeArrayWriter(booleanArray.length, 4,
      (writer: UnsafeArrayWriter) =>
        writer.writePrimitiveBooleanArray(new GenericArrayData(booleanArray)))
    booleanArray.zipWithIndex.map { case (e, i) => assert(booleanUnsafeArray.getBoolean(i) == e) }

    val byteArray = byteData.toArray
    val byteUnsafeArray = performUnsafeArrayWriter(byteArray.length, 4,
      (writer: UnsafeArrayWriter) =>
        writer.writePrimitiveByteArray(new GenericArrayData(byteArray)))
    byteArray.zipWithIndex.map { case (e, i) => assert(byteUnsafeArray.getByte(i) == e) }

    val shortArray = shortData.toArray
    val shortUnsafeArray = performUnsafeArrayWriter(shortArray.length, 4,
      (writer: UnsafeArrayWriter) =>
        writer.writePrimitiveShortArray(new GenericArrayData(shortArray)))
    shortArray.zipWithIndex.map { case (e, i) => assert(shortUnsafeArray.getShort(i) == e) }

    val intArray = intData.toArray
    val intUnsafeArray = performUnsafeArrayWriter(intArray.length, 4,
      (writer: UnsafeArrayWriter) => writer.writePrimitiveIntArray(new GenericArrayData(intArray)))
    intArray.zipWithIndex.map { case (e, i) => assert(intUnsafeArray.getInt(i) == e) }

    val longArray = longData.toArray
    val longUnsafeArray = performUnsafeArrayWriter(longArray.length, 8,
      (writer: UnsafeArrayWriter) =>
        writer.writePrimitiveLongArray(new GenericArrayData(longArray)))
    longArray.zipWithIndex.map { case (e, i) => assert(longUnsafeArray.getLong(i) == e) }

    val floatArray = floatData.toArray
    val floatUnsafeArray = performUnsafeArrayWriter(floatArray.length, 4,
      (writer: UnsafeArrayWriter) =>
        writer.writePrimitiveFloatArray(new GenericArrayData(floatArray)))
    floatArray.zipWithIndex.map { case (e, i) => assert(floatUnsafeArray.getFloat(i) == e) }

    val doubleArray = doubleData.toArray
    val doubleUnsafeArray = performUnsafeArrayWriter(doubleArray.length, 8,
      (writer: UnsafeArrayWriter) =>
        writer.writePrimitiveDoubleArray(new GenericArrayData(doubleArray)))
    doubleArray.zipWithIndex.map { case (e, i) => assert(doubleUnsafeArray.getDouble(i) == e) }
  }
}
