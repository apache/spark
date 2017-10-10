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

package org.apache.spark.sql.catalyst.expressions.aggregate

import java.nio.ByteBuffer
import java.util.HashMap

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult._
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ImplicitCastInputTypes, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.NGrams.NGramBuffer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Return the top-k n-grams in rows that consist of sequences of strings.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(expr, n, k, accuracy) - Estimates the top-k n-grams in rows that consist of sequences
      of strings, represented as arrays of strings, or arrays of arrays of strings. 'accuracy' is an
      optional precision factor that controls memory usage.
      The parameter 'n' specifies what type of n-grams are being estimated. Unigrams are n = 1, and
      bigrams are n = 2. Generally, n will not be greater than about 5. The 'k' parameter specifies
      how many of the highest-frequency n-grams will be returned by the UDAF. The optional precision
      factor 'accuracy' specifies how much memory to use for estimation; more memory will give
      more accurate frequency counts, but could crash the JVM. The value will be the max between
      'accuracy'(0 if it's not specified) and 1000/k, which indicates the max number of n-grams
      which are kept in the internal HashMap.
      The output is an array of maps with the top-k n-grams and corresponding frequency.
  """,
  extended = """
    Examples:
        > SELECT ngrams(array("abc", "abc", "bcd", "abc", "bcd"), 2, 4);
       [{["abc","bcd"]:2.0},
       {["abc","abc"]:1.0},
       {["bcd","abc"]:1.0}]
  """)
case class NGrams(
    child: Expression,
    nExpression: Expression,
    kExpression: Expression,
    accuracyExpression: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
  extends TypedImperativeAggregate[NGramBuffer] with ImplicitCastInputTypes  {

  def this(child: Expression, nExpression: Expression, kExpression: Expression,
           accuracyExpression: Expression) = {
    this(child, nExpression, kExpression, NGrams.getAccuracy(kExpression, accuracyExpression), 0, 0)
  }

  def this(child: Expression, nExpression: Expression, kExpression: Expression) = {
    this(child, nExpression, kExpression, Literal(0))
  }

  private lazy val n: Int = nExpression.eval().asInstanceOf[Int]
  private lazy val k: Int = kExpression.eval().asInstanceOf[Int]
  private lazy val accuracy: Int = accuracyExpression.eval().asInstanceOf[Int]

  override def inputTypes: Seq[AbstractDataType] = {
    Seq(TypeCollection(ArrayType(StringType, false), ArrayType(ArrayType(StringType, false))),
      IntegerType, IntegerType, IntegerType)
  }

  val isArrayOfString = child.dataType == ArrayType(StringType, false) ||
    child.dataType == ArrayType(StringType, true)

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (!nExpression.foldable || !kExpression.foldable || !accuracyExpression.foldable) {
      TypeCheckFailure(s"The accuracy or percentage provided must be a foldable integer expression")
    } else if (n <= 0) {
      TypeCheckFailure(s"The n provided must be a positive integer (current value = $n)")
    } else if (k <= 0) {
      TypeCheckFailure(s"The k provided must be a positive integer (current value = $k)")
    } else if (accuracy <= 0) {
      TypeCheckFailure(
        s"The accuracy provided must be a positive integer (current value = $accuracy)")
    } else {
      TypeCheckSuccess
    }
  }

  override def createAggregationBuffer(): NGramBuffer = {
    new NGramBuffer(n, k, accuracy)
  }

  def updateArray(genericArrayData: GenericArrayData, buffer: NGramBuffer, inputRow: InternalRow) {
    val values = (0 until genericArrayData.numElements()).map(genericArrayData.get(_, StringType).
      asInstanceOf[UTF8String]).toVector
    val nGrams = getNGrams(values, n)
    nGrams.foreach(buffer.add(_))
    buffer.trim()
  }
  override def update(buffer: NGramBuffer, inputRow: InternalRow): NGramBuffer = {
    if (isArrayOfString) {
      updateArray(child.eval(inputRow).asInstanceOf[GenericArrayData], buffer, inputRow)
    } else {
      val arrayOfArray = child.eval(inputRow).asInstanceOf[GenericArrayData]
      for (i <- 0 until arrayOfArray.numElements()) {
        updateArray(arrayOfArray.getArray(i).asInstanceOf[GenericArrayData], buffer, inputRow)
      }
    }
    buffer
  }

  override def merge(buffer: NGramBuffer, input: NGramBuffer): NGramBuffer = {
    buffer.merge(input)
    buffer.trim()
    buffer
  }

  override def eval(buffer: NGramBuffer): Any = {
    val topKNGrams = buffer.getTopKNGrams().map((keyValuePair: (Vector[UTF8String], Double)) => {
      val arrayKey = new GenericArrayData(Vector(new GenericArrayData(keyValuePair._1)))
      val arrayValue = new GenericArrayData(Vector(keyValuePair._2))
      new ArrayBasedMapData(arrayKey, arrayValue)
    }).toVector
    new GenericArrayData(topKNGrams)
  }

  private def getNGrams(values: Vector[UTF8String], n: Int): Vector[Vector[UTF8String]] = {
    if (values.length >= n) {
      values.sliding(n).toVector
    } else {
      Vector()
    }
  }

  override def withNewMutableAggBufferOffset(newOffset: Int): NGrams =
    copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(newOffset: Int): NGrams =
    copy(inputAggBufferOffset = newOffset)

  override def children: Seq[Expression] = Seq(child, nExpression, kExpression, accuracyExpression)

  // Returns null for empty inputs
  override def nullable: Boolean = true

  override def dataType: DataType = ArrayType(MapType(ArrayType(StringType), DoubleType))

  override def prettyName: String = "ngrams"

  override def serialize(obj: NGramBuffer): Array[Byte] = {
    NGrams.serializer.serialize(obj)
  }

  override def deserialize(bytes: Array[Byte]): NGramBuffer = {
    NGrams.serializer.deserialize(bytes)
  }
}

object NGrams {

  val DEFAULT_ACCURACY: Int = 1000

  private def getAccuracy(kExpression: Expression, accuracyExpression: Expression): Expression = {
    val accuracy = accuracyExpression.eval().asInstanceOf[Int]
    val k = kExpression.eval().asInstanceOf[Int]
    Literal(accuracy.max(DEFAULT_ACCURACY / k))
  }

  val kryoSerializer: KryoSerializer = new KryoSerializer(new SparkConf())

  import collection.JavaConverters._

  class NGramBuffer(val n: Int,
                    val k: Int,
                    val precisionFactor: Int) {

    val frequencyMap = new HashMap[Vector[UTF8String], Double]()

    def add(ng: Vector[UTF8String]): Unit = {
      frequencyMap.put(ng, frequencyMap.getOrDefault(ng, 0.0D) + 1)
    }

    def merge(other: NGramBuffer): Unit = {
      other.frequencyMap.asScala.foreach((keyValuePair: (Vector[UTF8String], Double)) => {
        val (key, value) = keyValuePair
        frequencyMap.put(key, frequencyMap.getOrDefault(key, 0.0D) + value)
      })
    }

    def sortWithTwoFields(frequencyDescend: Boolean)
                         (keyWithFrequency: Tuple2[Vector[UTF8String], Double],
                          keyWithFrequency2: Tuple2[Vector[UTF8String], Double]): Boolean = {
      if (keyWithFrequency._2 != keyWithFrequency2._2) {
        (keyWithFrequency._2 < keyWithFrequency2._2) ^ frequencyDescend
      } else {
        val keyVector = keyWithFrequency._1
        val keyVector2 = keyWithFrequency2._1
        for (i <- 0 until keyVector.length) {
          if (keyVector(i) != keyVector2(i)) {
            return (keyVector(i).compare(keyVector2(i))) < 0
          }
        }
        true
      }
    }

    def trim(): Unit = {
      if (frequencyMap.size() > 2 * k * precisionFactor) {
        frequencyMap.asScala.iterator.toVector.sortWith(sortWithTwoFields(
          frequencyDescend = false)).take(frequencyMap.size() - k * precisionFactor).
          foreach(keyValuePair => frequencyMap.remove(keyValuePair._1))
      }
    }

    def getTopKNGrams(): Seq[(Vector[UTF8String], Double)] = {
      frequencyMap.asScala.iterator.toVector.sortWith(sortWithTwoFields(frequencyDescend = true)).
        take(k)
    }

  }

  class NGramBufferSerializer {

    final def serialize(obj: NGramBuffer): Array[Byte] = {
      NGrams.kryoSerializer.newInstance().serialize[NGramBuffer](obj).array()
    }

    final def deserialize(bytes: Array[Byte]): NGramBuffer = {
      NGrams.kryoSerializer.newInstance().deserialize[NGramBuffer](ByteBuffer.wrap(bytes))
    }
  }

  val serializer: NGramBufferSerializer = new NGramBufferSerializer
}
