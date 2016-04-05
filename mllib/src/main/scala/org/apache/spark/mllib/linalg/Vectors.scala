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

package org.apache.spark.mllib.linalg

import java.lang.{Double => JavaDouble, Integer => JavaInteger, Iterable => JavaIterable}

import scala.annotation.varargs
import scala.language.implicitConversions

import breeze.linalg.{Vector => BV}

import org.apache.spark.annotation.{Since, AlphaComponent}
import org.apache.spark.ml.linalg.{DenseVector => MLDenseVector, SparseVector => MLSparseVector}
import org.apache.spark.ml.linalg.{Vector => MLVector, Vectors => MLVectors}
import org.apache.spark.mllib.linalg.ImplicitVectorCasts._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._

/**
 * Represents a numeric vector, whose index type is Int and value type is Double.
 *
 * Note: Users should not implement this interface.
 */
@SQLUserDefinedType(udt = classOf[VectorUDT])
@Since("1.0.0")
sealed trait Vector extends Serializable {

  override def toString: String = this.asML.toString

  /**
   * Size of the vector.
   */
  @Since("1.0.0")
  def size: Int

  /**
   * Converts the instance to a double array.
   */
  @Since("1.0.0")
  def toArray: Array[Double] = this.asML.toArray

  override def equals(other: Any): Boolean = {
    other match {
      case v2: Vector =>
        this.asML.equals(v2.asML)
      case _ => false
    }
  }

  /**
   * Returns a hash code value for the vector. The hash code is based on its size and its first 128
   * nonzero entries, using a hash algorithm similar to [[java.util.Arrays.hashCode]].
   */
  override def hashCode(): Int = this.asML.hashCode()

  /**
   * Casts the instance to a breeze vector.
   * Note that the underneath data structure is shared, so changing
   * the data in one vector will reflect the other one
   */
  private[spark] def asBreeze: BV[Double] = this.asML.asBreeze

  /**
   * Casts the instance to a standalone ml vector.
   * Note that the underneath data structure is shared, so changing
   * the data in one vector will reflect the other one.
   */
  private[spark] def asML: MLVector

  /**
   * Gets the value of the ith element.
   * @param i index
   */
  @Since("1.1.0")
  def apply(i: Int): Double = this.asML(i)

  /**
   * Makes a deep copy of this vector.
   */
  @Since("1.1.0")
  def copy: Vector = {
    throw new NotImplementedError(s"copy is not implemented for ${this.getClass}.")
  }

  /**
   * Applies a function `f` to all the active elements of dense and sparse vector.
   *
   * @param f the function takes two parameters where the first parameter is the index of
   *          the vector with type `Int`, and the second parameter is the corresponding value
   *          with type `Double`.
   */
  @Since("1.6.0")
  def foreachActive(f: (Int, Double) => Unit): Unit = this.asML.foreachActive(f)

  /**
   * Number of active entries.  An "active entry" is an element which is explicitly stored,
   * regardless of its value.  Note that inactive entries have value 0.
   */
  @Since("1.4.0")
  def numActives: Int = this.asML.numActives

  /**
   * Number of nonzero elements. This scans all active values and count nonzeros.
   */
  @Since("1.4.0")
  def numNonzeros: Int = this.asML.numNonzeros

  /**
   * Converts this vector to a sparse vector with all explicit zeros removed.
   */
  @Since("1.4.0")
  def toSparse: SparseVector = this.asML.toSparse.asMLLib

  /**
   * Converts this vector to a dense vector.
   */
  @Since("1.4.0")
  def toDense: DenseVector = new DenseVector(this.toArray)

  /**
   * Returns a vector in either dense or sparse format, whichever uses less storage.
   */
  @Since("1.4.0")
  def compressed: Vector = this.asML.compressed.asMLLib

  /**
   * Find the index of a maximal element.  Returns the first maximal element in case of a tie.
   * Returns -1 if vector has length 0.
   */
  @Since("1.5.0")
  def argmax: Int = this.asML.argmax

  /**
   * Converts the vector to a JSON string.
   */
  @Since("1.6.0")
  def toJson: String = this.asML.toJson
}

/**
 * :: AlphaComponent ::
 *
 * User-defined type for [[Vector]] which allows easy interaction with SQL
 * via [[org.apache.spark.sql.DataFrame]].
 */
@AlphaComponent
class VectorUDT extends UserDefinedType[Vector] {

  override def sqlType: StructType = {
    // type: 0 = sparse, 1 = dense
    // We only use "values" for dense vectors, and "size", "indices", and "values" for sparse
    // vectors. The "values" field is nullable because we might want to add binary vectors later,
    // which uses "size" and "indices", but not "values".
    StructType(Seq(
      StructField("type", ByteType, nullable = false),
      StructField("size", IntegerType, nullable = true),
      StructField("indices", ArrayType(IntegerType, containsNull = false), nullable = true),
      StructField("values", ArrayType(DoubleType, containsNull = false), nullable = true)))
  }

  override def serialize(obj: Vector): InternalRow = {
    obj match {
      case SparseVector(size, indices, values) =>
        val row = new GenericMutableRow(4)
        row.setByte(0, 0)
        row.setInt(1, size)
        row.update(2, new GenericArrayData(indices.map(_.asInstanceOf[Any])))
        row.update(3, new GenericArrayData(values.map(_.asInstanceOf[Any])))
        row
      case DenseVector(values) =>
        val row = new GenericMutableRow(4)
        row.setByte(0, 1)
        row.setNullAt(1)
        row.setNullAt(2)
        row.update(3, new GenericArrayData(values.map(_.asInstanceOf[Any])))
        row
    }
  }

  override def deserialize(datum: Any): Vector = {
    datum match {
      case row: InternalRow =>
        require(row.numFields == 4,
          s"VectorUDT.deserialize given row with length ${row.numFields} but requires length == 4")
        val tpe = row.getByte(0)
        tpe match {
          case 0 =>
            val size = row.getInt(1)
            val indices = row.getArray(2).toIntArray()
            val values = row.getArray(3).toDoubleArray()
            new SparseVector(size, indices, values)
          case 1 =>
            val values = row.getArray(3).toDoubleArray()
            new DenseVector(values)
        }
    }
  }

  override def pyUDT: String = "pyspark.mllib.linalg.VectorUDT"

  override def userClass: Class[Vector] = classOf[Vector]

  override def equals(o: Any): Boolean = {
    o match {
      case v: VectorUDT => true
      case _ => false
    }
  }

  // see [SPARK-8647], this achieves the needed constant hash code without constant no.
  override def hashCode(): Int = classOf[VectorUDT].getName.hashCode()

  override def typeName: String = "vector"

  private[spark] override def asNullable: VectorUDT = this
}

/**
 * Factory methods for [[org.apache.spark.mllib.linalg.Vector]].
 * We don't use the name `Vector` because Scala imports
 * [[scala.collection.immutable.Vector]] by default.
 */
@Since("1.0.0")
object Vectors {

  /**
   * Creates a dense vector from its values.
   */
  @Since("1.0.0")
  @varargs
  def dense(firstValue: Double, otherValues: Double*): Vector =
    MLVectors.dense(firstValue, otherValues:_*).asMLLib

  // A dummy implicit is used to avoid signature collision with the one generated by @varargs.
  /**
   * Creates a dense vector from a double array.
   */
  @Since("1.0.0")
  def dense(values: Array[Double]): Vector = MLVectors.dense(values).asMLLib

  /**
   * Creates a sparse vector providing its index array and value array.
   *
   * @param size vector size.
   * @param indices index array, must be strictly increasing.
   * @param values value array, must have the same length as indices.
   */
  @Since("1.0.0")
  def sparse(size: Int, indices: Array[Int], values: Array[Double]): Vector =
    MLVectors.sparse(size, indices, values).asMLLib

  /**
   * Creates a sparse vector using unordered (index, value) pairs.
   *
   * @param size vector size.
   * @param elements vector elements in (index, value) pairs.
   */
  @Since("1.0.0")
  def sparse(size: Int, elements: Seq[(Int, Double)]): Vector =
    MLVectors.sparse(size, elements).asMLLib

  /**
   * Creates a sparse vector using unordered (index, value) pairs in a Java friendly way.
   *
   * @param size vector size.
   * @param elements vector elements in (index, value) pairs.
   */
  @Since("1.0.0")
  def sparse(size: Int, elements: JavaIterable[(JavaInteger, JavaDouble)]): Vector =
    MLVectors.sparse(size, elements).asMLLib

  /**
   * Creates a vector of all zeros.
   *
   * @param size vector size
   * @return a zero vector
   */
  @Since("1.1.0")
  def zeros(size: Int): Vector = MLVectors.zeros(size).asMLLib

  private[mllib] def parseNumeric(any: Any): Vector = MLVectors.parseNumeric(any).asMLLib

  /**
   * Parses a string resulted from [[Vector.toString]] into a [[Vector]].
   */
  @Since("1.1.0")
  def parse(s: String): Vector = MLVectors.parse(s).asMLLib

  /**
   * Parses the JSON representation of a vector into a [[Vector]].
   */
  @Since("1.6.0")
  def fromJson(json: String): Vector = MLVectors.fromJson(json).asMLLib

  /**
   * Creates a vector instance from a breeze vector.
   */
  private[spark] def fromBreeze(breezeVector: BV[Double]): Vector =
    MLVectors.fromBreeze(breezeVector).asMLLib

  /**
   * Returns the p-norm of this vector.
   * @param vector input vector.
   * @param p norm.
   * @return norm in L^p^ space.
   */
  @Since("1.3.0")
  def norm(vector: Vector, p: Double): Double = MLVectors.norm(vector.asML, p)

  /**
   * Returns the squared distance between two Vectors.
   * @param v1 first Vector.
   * @param v2 second Vector.
   * @return squared distance between two Vectors.
   */
  @Since("1.3.0")
  def sqdist(v1: Vector, v2: Vector): Double = MLVectors.sqdist(v1.asML, v2.asML)

  /**
   * Returns the squared distance between DenseVector and SparseVector.
   */
  private[mllib] def sqdist(v1: SparseVector, v2: DenseVector): Double =
    MLVectors.sqdist(v1.asML, v2.asML)

  /**
   * Check equality between sparse/dense vectors
   */
  private[mllib] def equals(
      v1Indices: IndexedSeq[Int],
      v1Values: Array[Double],
      v2Indices: IndexedSeq[Int],
      v2Values: Array[Double]): Boolean =
    MLVectors.equals(v1Indices, v1Values, v2Indices, v2Values)
}

/**
 * A dense vector represented by a value array.
 */
@Since("1.0.0")
@SQLUserDefinedType(udt = classOf[VectorUDT])
class DenseVector private[linalg] (vectorImpl: MLDenseVector) extends Vector {

  @Since("1.0.0")
  override val size: Int = vectorImpl.size

  @Since("1.0.0")
  def this(values: Array[Double]) = this(new MLDenseVector(values))

  @Since("1.0.0")
  val values = vectorImpl.values

  @Since("1.1.0")
  override def copy: DenseVector = {
    new DenseVector(this.asML.copy)
  }

  private[spark] override def asML: MLDenseVector = vectorImpl
}


@Since("1.3.0")
object DenseVector {

  /** Extracts the value array from a dense vector. */
  @Since("1.3.0")
  def unapply(dv: DenseVector): Option[Array[Double]] = MLDenseVector.unapply(dv.asML)
}

/**
 * A sparse vector represented by an index array and an value array.
 */
@Since("1.0.0")
@SQLUserDefinedType(udt = classOf[VectorUDT])
class SparseVector (vectorImpl: MLSparseVector) extends Vector {

  @Since("1.0.0")
  override val size: Int = vectorImpl.size

  @Since("1.0.0")
  val indices: Array[Int] = vectorImpl.indices
  @Since("1.0.0")
  val values: Array[Double] = vectorImpl.values

  /**
   * @param size size of the vector.
   * @param indices index array, assume to be strictly increasing.
   * @param values value array, must have the same length as the index array.
   */
  @Since("1.0.0")
  def this(size: Int, indices: Array[Int], values: Array[Double]) =
    this(new MLSparseVector(size, indices, values))

  @Since("1.1.0")
  override def copy: SparseVector =
    new SparseVector(this.asML.copy)

  private[spark] override def asML: MLSparseVector = vectorImpl

  /**
   * Create a slice of this vector based on the given indices.
   * @param selectedIndices Unsorted list of indices into the vector.
   *                        This does NOT do bound checking.
   * @return  New SparseVector with values in the order specified by the given indices.
   *
   * NOTE: The API needs to be discussed before making this public.
   *       Also, if we have a version assuming indices are sorted, we should optimize it.
   */
  private[spark] def slice(selectedIndices: Array[Int]): SparseVector =
    this.asML.slice(selectedIndices).asMLLib
}

@Since("1.3.0")
object SparseVector {
  @Since("1.3.0")
  def unapply(sv: SparseVector): Option[(Int, Array[Int], Array[Double])] =
    MLSparseVector.unapply(sv.asML)
}

private[spark] object ImplicitVectorCasts {

  class MLVectorWithCast(val v: MLVector) {
    /**
     * Casts the instance to a mllib vector.
     * Note that the underneath data structure is shared, so changing
     * the data in one vector will reflect the other one.
     */
    def asMLLib: Vector = {
      v match {
        case v: MLDenseVector =>
          new DenseVector(v)
        case v: MLSparseVector =>
          new SparseVector(v)
        case v =>
          sys.error("Unsupported ML vector type: " + v.getClass.getName)
      }
    }
  }

  class MLDenseVectorWithCast(v: MLDenseVector) {
    /**
     * Casts a ml dense vector to a mllib dense vector.
     * Note that the underneath data structure is shared, so changing
     * the data in one vector will reflect the other one.
     */
    def asMLLib: DenseVector = new DenseVector(v)
  }

  class MLSparseVectorWithCast(v: MLSparseVector) {
    /**
     * Casts a ml sparse vector to a mllib sparse vector.
     * Note that the underneath data structure is shared, so changing
     * the data in one vector will reflect the other one.
     */
    def asMLLib: SparseVector = new SparseVector(v)
  }

  implicit def MLVectorAsMLVectorWithCast(v: MLVector): MLVectorWithCast =
    new MLVectorWithCast(v)

  implicit def MLDenseVectorAsMLDenseVectorWithCast(v: MLDenseVector): MLDenseVectorWithCast =
    new MLDenseVectorWithCast(v)

  implicit def MLSparseVectorAsMLSparseVectorWithCast(v: MLSparseVector): MLSparseVectorWithCast =
    new MLSparseVectorWithCast(v)
}