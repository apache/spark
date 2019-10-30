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

package org.apache.spark.sql.thriftserver.cli

import java.nio.ByteBuffer

import scala.collection.JavaConverters._

import com.google.common.primitives._
import java.util._

import org.apache.spark.sql.thriftserver.cli.Type._
import org.apache.spark.sql.thriftserver.cli.thrift._

/**
 * ColumnBuffer
 */
object ColumnBuffer {
  private val DEFAULT_SIZE = 100
  private val MASKS = Array[Byte](0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80.toByte)

  private def toBitset(nulls: Array[Byte]) = {
    val bitset = new BitSet
    val bits = nulls.length * 8
    var i = 0
    while (i < bits) {
      bitset.set(i, (nulls(i / 8) & MASKS(i % 8)) != 0)
      i = i + 1
    }
    bitset
  }

  private val EMPTY_BINARY = ByteBuffer.allocate(0)
  private val EMPTY_STRING = ""
}

class ColumnBuffer() {

  import ColumnBuffer._

  final private var `type`: Type = null
  private var nulls: BitSet = null
  private var _size: Int = 0
  private var _boolVars: Array[Boolean] = null
  private var _byteVars: Array[Byte] = null
  private var _shortVars: Array[Short] = null
  private var _intVars: Array[Int] = null
  private var _longVars: Array[Long] = null
  private var _doubleVars: Array[Double] = null
  private var _stringVars: Array[String] = null
  private var _binaryVars: Array[ByteBuffer] = null


  def this(colValues: TColumn) {
    this()
    if (colValues.isSetBoolVal) {
      `type` = Type.BOOLEAN
      nulls = toBitset(colValues.getBoolVal.getNulls)
      _boolVars = Booleans.toArray(colValues.getBoolVal.getValues)
      _size = boolVars.length
    }
    else if (colValues.isSetByteVal) {
      `type` = Type.BYTE
      nulls = toBitset(colValues.getByteVal.getNulls)
      _byteVars = Bytes.toArray(colValues.getByteVal.getValues)
      _size = byteVars.length
    }
    else if (colValues.isSetI16Val) {
      `type` = Type.SHORT
      nulls = toBitset(colValues.getI16Val.getNulls)
      _shortVars = Shorts.toArray(colValues.getI16Val.getValues)
      _size = shortVars.length
    }
    else if (colValues.isSetI32Val) {
      `type` = Type.INT
      nulls = toBitset(colValues.getI32Val.getNulls)
      _intVars = Ints.toArray(colValues.getI32Val.getValues)
      _size = intVars.length
    }
    else if (colValues.isSetI64Val) {
      `type` = Type.LONG
      nulls = toBitset(colValues.getI64Val.getNulls)
      _longVars = Longs.toArray(colValues.getI64Val.getValues)
      _size = longVars.length
    }
    else if (colValues.isSetDoubleVal) {
      `type` = Type.DOUBLE
      nulls = toBitset(colValues.getDoubleVal.getNulls)
      _doubleVars = Doubles.toArray(colValues.getDoubleVal.getValues)
      _size = doubleVars.length
    }
    else if (colValues.isSetBinaryVal) {
      `type` = Type.BINARY
      nulls = toBitset(colValues.getBinaryVal.getNulls)
      _binaryVars = colValues.getBinaryVal.getValues.asScala.toArray
      _size = _binaryVars.size
    }
    else if (colValues.isSetStringVal) {
      `type` = Type.STRING
      nulls = toBitset(colValues.getStringVal.getNulls)
      _stringVars = colValues.getStringVal.getValues.asScala.toArray
      _size = _stringVars.size
    } else {
      throw new IllegalStateException("invalid union object")
    }
  }

  private[cli] def getNulls = nulls

  def getType: Type = `type`

  def get(index: Int): Any = {
    if (nulls.get(index)) {
      return null
    }
    `type` match {
      case BOOLEAN =>
        return _boolVars(index)
      case BYTE =>
        return _byteVars(index)
      case SHORT =>
        return _shortVars(index)
      case INT =>
        return _intVars(index)
      case LONG =>
        return _longVars(index)
      case FLOAT =>
      case DOUBLE =>
        return _doubleVars(index)
      case STRING =>
        return _stringVars(index)
      case BINARY =>
        return _binaryVars(index).array
    }
    null
  }

  def size: Int = _size

  private def boolVars: Array[Boolean] = {
    if (_boolVars.length == _size) {
      val newVars = new Array[Boolean](_size << 1)
      System.arraycopy(_boolVars, 0, newVars, 0, _size)
      _boolVars = newVars
    }
    _boolVars
  }

  private def byteVars: Array[Byte] = {
    if (_byteVars.length == _size) {
      val newVars = new Array[Byte](_size << 1)
      System.arraycopy(_byteVars, 0, newVars, 0, _size)
      _byteVars = newVars
    }
    _byteVars
  }

  private def shortVars: Array[Short] = {
    if (_shortVars.length == _size) {
      val newVars = new Array[Short](_size << 1)
      System.arraycopy(_shortVars, 0, newVars, 0, _size)
      _shortVars = newVars
    }
    _shortVars
  }

  private def intVars: Array[Int] = {
    if (_intVars.length == _size) {
      val newVars = new Array[Int](_size << 1)
      System.arraycopy(_intVars, 0, newVars, 0, _size)
      _intVars = newVars
    }
    _intVars
  }

  private def longVars: Array[Long] = {
    if (_longVars.length == _size) {
      val newVars = new Array[Long](_size << 1)
      System.arraycopy(_longVars, 0, newVars, 0, _size)
      _longVars = newVars
    }
    _longVars
  }

  private def doubleVars: Array[Double] = {
    if (_doubleVars.length == _size) {
      val newVars = new Array[Double](_size << 1)
      System.arraycopy(_doubleVars, 0, newVars, 0, _size)
      _doubleVars = newVars
    }
    _doubleVars
  }
}
