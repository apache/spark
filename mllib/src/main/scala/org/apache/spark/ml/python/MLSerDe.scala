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

package org.apache.spark.ml.python

import java.io.OutputStream
import java.nio.{ByteBuffer, ByteOrder}
import java.util.{ArrayList => JArrayList}

import scala.collection.JavaConverters._

import net.razorvine.pickle._

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.python.SerDeUtil
import org.apache.spark.ml.linalg._
import org.apache.spark.mllib.api.python.SerDeBase
import org.apache.spark.rdd.RDD

/**
 * SerDe utility functions for pyspark.ml.
 */
private[spark] object MLSerDe extends SerDeBase with Serializable {

  override val PYSPARK_PACKAGE = "pyspark.ml"

  // Pickler for DenseVector
  private[python] class DenseVectorPickler extends BasePickler[DenseVector] {

    def saveState(obj: Object, out: OutputStream, pickler: Pickler): Unit = {
      val vector: DenseVector = obj.asInstanceOf[DenseVector]
      val bytes = new Array[Byte](8 * vector.size)
      val bb = ByteBuffer.wrap(bytes)
      bb.order(ByteOrder.nativeOrder())
      val db = bb.asDoubleBuffer()
      db.put(vector.values)

      out.write(Opcodes.BINSTRING)
      out.write(PickleUtils.integer_to_bytes(bytes.length))
      out.write(bytes)
      out.write(Opcodes.TUPLE1)
    }

    def construct(args: Array[Object]): Object = {
      if (args.length != 1) {
        throw new PickleException("should be 1")
      }
      val bytes = getBytes(args(0))
      val bb = ByteBuffer.wrap(bytes, 0, bytes.length)
      bb.order(ByteOrder.nativeOrder())
      val db = bb.asDoubleBuffer()
      val ans = new Array[Double](bytes.length / 8)
      db.get(ans)
      Vectors.dense(ans)
    }
  }

  // Pickler for DenseMatrix
  private[python] class DenseMatrixPickler extends BasePickler[DenseMatrix] {

    def saveState(obj: Object, out: OutputStream, pickler: Pickler): Unit = {
      val m: DenseMatrix = obj.asInstanceOf[DenseMatrix]
      val bytes = new Array[Byte](8 * m.values.length)
      val order = ByteOrder.nativeOrder()
      val isTransposed = if (m.isTransposed) 1 else 0
      ByteBuffer.wrap(bytes).order(order).asDoubleBuffer().put(m.values)

      out.write(Opcodes.MARK)
      out.write(Opcodes.BININT)
      out.write(PickleUtils.integer_to_bytes(m.numRows))
      out.write(Opcodes.BININT)
      out.write(PickleUtils.integer_to_bytes(m.numCols))
      out.write(Opcodes.BINSTRING)
      out.write(PickleUtils.integer_to_bytes(bytes.length))
      out.write(bytes)
      out.write(Opcodes.BININT)
      out.write(PickleUtils.integer_to_bytes(isTransposed))
      out.write(Opcodes.TUPLE)
    }

    def construct(args: Array[Object]): Object = {
      if (args.length != 4) {
        throw new PickleException("should be 4")
      }
      val bytes = getBytes(args(2))
      val n = bytes.length / 8
      val values = new Array[Double](n)
      val order = ByteOrder.nativeOrder()
      ByteBuffer.wrap(bytes).order(order).asDoubleBuffer().get(values)
      val isTransposed = args(3).asInstanceOf[Int] == 1
      new DenseMatrix(args(0).asInstanceOf[Int], args(1).asInstanceOf[Int], values, isTransposed)
    }
  }

  // Pickler for SparseMatrix
  private[python] class SparseMatrixPickler extends BasePickler[SparseMatrix] {

    def saveState(obj: Object, out: OutputStream, pickler: Pickler): Unit = {
      val s = obj.asInstanceOf[SparseMatrix]
      val order = ByteOrder.nativeOrder()

      val colPtrsBytes = new Array[Byte](4 * s.colPtrs.length)
      val indicesBytes = new Array[Byte](4 * s.rowIndices.length)
      val valuesBytes = new Array[Byte](8 * s.values.length)
      val isTransposed = if (s.isTransposed) 1 else 0
      ByteBuffer.wrap(colPtrsBytes).order(order).asIntBuffer().put(s.colPtrs)
      ByteBuffer.wrap(indicesBytes).order(order).asIntBuffer().put(s.rowIndices)
      ByteBuffer.wrap(valuesBytes).order(order).asDoubleBuffer().put(s.values)

      out.write(Opcodes.MARK)
      out.write(Opcodes.BININT)
      out.write(PickleUtils.integer_to_bytes(s.numRows))
      out.write(Opcodes.BININT)
      out.write(PickleUtils.integer_to_bytes(s.numCols))
      out.write(Opcodes.BINSTRING)
      out.write(PickleUtils.integer_to_bytes(colPtrsBytes.length))
      out.write(colPtrsBytes)
      out.write(Opcodes.BINSTRING)
      out.write(PickleUtils.integer_to_bytes(indicesBytes.length))
      out.write(indicesBytes)
      out.write(Opcodes.BINSTRING)
      out.write(PickleUtils.integer_to_bytes(valuesBytes.length))
      out.write(valuesBytes)
      out.write(Opcodes.BININT)
      out.write(PickleUtils.integer_to_bytes(isTransposed))
      out.write(Opcodes.TUPLE)
    }

    def construct(args: Array[Object]): Object = {
      if (args.length != 6) {
        throw new PickleException("should be 6")
      }
      val order = ByteOrder.nativeOrder()
      val colPtrsBytes = getBytes(args(2))
      val indicesBytes = getBytes(args(3))
      val valuesBytes = getBytes(args(4))
      val colPtrs = new Array[Int](colPtrsBytes.length / 4)
      val rowIndices = new Array[Int](indicesBytes.length / 4)
      val values = new Array[Double](valuesBytes.length / 8)
      ByteBuffer.wrap(colPtrsBytes).order(order).asIntBuffer().get(colPtrs)
      ByteBuffer.wrap(indicesBytes).order(order).asIntBuffer().get(rowIndices)
      ByteBuffer.wrap(valuesBytes).order(order).asDoubleBuffer().get(values)
      val isTransposed = args(5).asInstanceOf[Int] == 1
      new SparseMatrix(
        args(0).asInstanceOf[Int], args(1).asInstanceOf[Int], colPtrs, rowIndices, values,
        isTransposed)
    }
  }

  // Pickler for SparseVector
  private[python] class SparseVectorPickler extends BasePickler[SparseVector] {

    def saveState(obj: Object, out: OutputStream, pickler: Pickler): Unit = {
      val v: SparseVector = obj.asInstanceOf[SparseVector]
      val n = v.indices.length
      val indiceBytes = new Array[Byte](4 * n)
      val order = ByteOrder.nativeOrder()
      ByteBuffer.wrap(indiceBytes).order(order).asIntBuffer().put(v.indices)
      val valueBytes = new Array[Byte](8 * n)
      ByteBuffer.wrap(valueBytes).order(order).asDoubleBuffer().put(v.values)

      out.write(Opcodes.BININT)
      out.write(PickleUtils.integer_to_bytes(v.size))
      out.write(Opcodes.BINSTRING)
      out.write(PickleUtils.integer_to_bytes(indiceBytes.length))
      out.write(indiceBytes)
      out.write(Opcodes.BINSTRING)
      out.write(PickleUtils.integer_to_bytes(valueBytes.length))
      out.write(valueBytes)
      out.write(Opcodes.TUPLE3)
    }

    def construct(args: Array[Object]): Object = {
      if (args.length != 3) {
        throw new PickleException("should be 3")
      }
      val size = args(0).asInstanceOf[Int]
      val indiceBytes = getBytes(args(1))
      val valueBytes = getBytes(args(2))
      val n = indiceBytes.length / 4
      val indices = new Array[Int](n)
      val values = new Array[Double](n)
      if (n > 0) {
        val order = ByteOrder.nativeOrder()
        ByteBuffer.wrap(indiceBytes).order(order).asIntBuffer().get(indices)
        ByteBuffer.wrap(valueBytes).order(order).asDoubleBuffer().get(values)
      }
      new SparseVector(size, indices, values)
    }
  }

  var initialized = false
  // This should be called before trying to serialize any above classes
  // In cluster mode, this should be put in the closure
  override def initialize(): Unit = {
    SerDeUtil.initialize()
    synchronized {
      if (!initialized) {
        new DenseVectorPickler().register()
        new DenseMatrixPickler().register()
        new SparseMatrixPickler().register()
        new SparseVectorPickler().register()
        initialized = true
      }
    }
  }
  // will not called in Executor automatically
  initialize()
}
