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

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult._
import org.apache.spark.sql.catalyst.expressions.aggregate.NGrams.NGramBuffer
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ImplicitCastInputTypes, Literal}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.unsafe.types.UTF8String


@ExpressionDescription(
  usage = """
      _FUNC_(expr, n, k, pf) - Estimates the top-k n-grams in rows that consist of sequences of
      strings, represented as arrays of strings, or arrays of arrays of strings. 'pf' is an
      optional precision factor that controls memory usage.
      The parameter 'n' specifies what type of n-grams are being estimated. Unigrams are n = 1,
      and bigrams are n = 2. Generally, n will not be greater than about 5. The 'k' parameter
      specifies how many of the highest-frequency n-grams will be returned by the UDAF. The optional
      precision factor 'pf' specifies how much memory to use for estimation; more memory will give
      more accurate frequency counts, but could crash the JVM. The default value is 20, which
      internally maintains 20*k n-grams, but only returns the k highest frequency ones. The output
      is an array of structs with the top-k n-grams. It might be convenient to explode() the output
      of this UDAF.
          """,
  extended = """
    Examples:
             """)
case class NGrams(child: Expression,
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
    Seq(ArrayType(StringType), IntegerType, IntegerType, IntegerType)
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (!nExpression.foldable || !kExpression.foldable || !accuracyExpression.foldable) {
      TypeCheckFailure(s"The accuracy or percentage provided must be a constant literal")
    } else if (accuracy <= 0) {
      TypeCheckFailure(
        s"The accuracy provided must be a positive integer literal (current value = $accuracy)")
    }
    else {
      TypeCheckSuccess
    }
  }

  override def createAggregationBuffer(): NGramBuffer = {
    new NGramBuffer(n, k, accuracy, new HashMap[List[UTF8String], Double]())
  }

  override def update(buffer: NGramBuffer, inputRow: InternalRow): NGramBuffer = {
    val genericArrayData: GenericArrayData = child.eval(inputRow).asInstanceOf[GenericArrayData]
    val values = (0 until genericArrayData.numElements()).map(genericArrayData.get(_, StringType).asInstanceOf[UTF8String]).toList
    if (values != null) {
      val nGrams = getNGrams(values, n)
      nGrams.foreach(buffer.add(_))
    }
    buffer.trim()
    buffer
  }

  override def merge(buffer: NGramBuffer, input: NGramBuffer): NGramBuffer = {
    buffer.merge(input)
    buffer.trim()
    buffer
  }

  override def eval(buffer: NGramBuffer): Any = {
    buffer.getTopKNGrams()
  }

  private def getNGrams(values: List[UTF8String], n: Int): List[List[UTF8String]] = {
    values.sliding(n).toList
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
    Literal(Math.max(accuracy, DEFAULT_ACCURACY / k))
  }

  val kryoSerializer: KryoSerializer = new KryoSerializer(new SparkConf())

  import collection.JavaConverters._

  class NGramBuffer(val n: Int,
                    val k: Int,
                    val precisionFactor: Int,
                    val frequencyMap: HashMap[List[UTF8String], Double]) {
    def add(ng: List[UTF8String]): Unit = {
      var currentFrequency: Double = frequencyMap.get(ng)
      if (currentFrequency == null.asInstanceOf[Double]) {
        currentFrequency = 1.0D
      } else {
        currentFrequency += 1
      }
      frequencyMap.put(ng, currentFrequency)
    }

    def merge(other: NGramBuffer): Unit = {
      other.frequencyMap.asScala.foreach((keyValuePair: (List[UTF8String], Double)) => {
        val key = keyValuePair._1
        val value = keyValuePair._2
        val originalValue = frequencyMap.getOrDefault(key, 0.0D)
        frequencyMap.put(key, originalValue + value)
      })
    }

    def trim(): Unit = {
      if (frequencyMap.size() > 2 * k * precisionFactor) {
        val orderedWithIndex = frequencyMap.asScala.iterator.toList.sortWith(_._2 < _._2).zipWithIndex
        orderedWithIndex.takeWhile(_._2 < frequencyMap.size() - k * precisionFactor).map(_._1).
          foreach(keyValuePair => frequencyMap.remove(keyValuePair._1))
      }
    }

    def getTopKNGrams() = {
      frequencyMap.asScala.iterator.toList.sortWith(_._2 > _._2).zipWithIndex.takeWhile(_._2 < k).map(_._1).toList
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
