package org.apache.spark.sql.catalyst.expressions.aggregate

import java.nio.ByteBuffer

import com.google.common.primitives.{Chars, Doubles, Ints}
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.aggregate.NGrams.NGramBuffer
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ImplicitCastInputTypes, Literal}
import org.apache.spark.sql.catalyst.util.{GenericArrayData, QuantileSummaries}
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.OpenHashMap
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
    this(child, nExpression, kExpression, accuracyExpression, 0, 0)
  }

  def this(child: Expression, nExpression: Expression, kExpression: Expression) = {
    this(child, nExpression, kExpression, Literal(NGrams.DEFAULT_NGRAM_ACCURACY))
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
    new NGramBuffer(n, k, accuracy, new OpenHashMap[List[UTF8String], Double]())
  }

  override def update(buffer: NGramBuffer, inputRow: InternalRow): NGramBuffer = {
    val genericArrayData: GenericArrayData = child.eval(inputRow).asInstanceOf[GenericArrayData]
    // Ignore empty rows, for example: percentile_approx(null)
    val values = (0 until genericArrayData.numElements()).map(genericArrayData.get(_, StringType).asInstanceOf[UTF8String]).toList
    if (values != null) {
      val nGrams = getNGrams(values, n)
      nGrams.foreach(buffer.add(_))
    }
    buffer
  }

  private def getNGrams(values: List[UTF8String], n: Int): List[List[UTF8String]] = {
    values.sliding(n).toList
  }

  override def merge(buffer: NGramBuffer, input: NGramBuffer): NGramBuffer = {
    buffer.merge(input)
    buffer
  }

  override def eval(buffer: NGramBuffer): Any = {
    buffer.getTopKNGrams()
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

  val DEFAULT_NGRAM_ACCURACY: Int = 10000

  val kryoSerializer: KryoSerializer = new KryoSerializer(new SparkConf())

  class NGramBuffer(val n: Int,
                    val k: Int,
                    val accuracy: Int,
                    val ngramFrequencyMap: OpenHashMap[List[UTF8String], Double]) {

    def add(ng: List[UTF8String]): Unit = {
      var currentFrequency: Double = ngramFrequencyMap(ng)
      if (currentFrequency == null.asInstanceOf[Double]) {
        currentFrequency = 1.0D
      } else {
        currentFrequency += 1
      }
      ngramFrequencyMap.update(ng, currentFrequency)
      //    if (ngramMap.size > k * accuracy * 2) {
      //      trim(false)
      //    }
    }

    def merge(other: NGramBuffer): Unit = {
      other.ngramFrequencyMap.iterator.foreach(mergeSingleKVPair)
    }

    def mergeSingleKVPair(keyValuePair: (List[UTF8String], Double)): Unit = {
      val key = keyValuePair._1
      val value = keyValuePair._2
      ngramFrequencyMap.changeValue(key, value, _ + value)
    }

    def getTopKNGrams() = {
      ngramFrequencyMap.iterator.toList.sortWith(_._2 > _._2).zipWithIndex.takeWhile(_._2 < k).map(_._1).toList
    }

  }
  class NGramBufferSerializer {

//    private final def length(nGramBuffer: NGramBuffer): Int = {
//      Ints.BYTES * 3 + nGramBuffer.ngramFrequencyMap.iterator.map(_._1).reduce(_.length + _.length) *
//        Chars.BYTES + nGramBuffer.ngramFrequencyMap.size * Doubles.BYTES
//
//    }

    final def serialize(obj: NGramBuffer): Array[Byte] = {
//      val buffer = ByteBuffer.wrap(new Array(length(obj)))
//      buffer.putInt(obj.n)
//      buffer.putInt(obj.k)
//      buffer.putInt(obj.accuracy)
//
//      var i = (List[String], Double)
//      obj.ngramFrequencyMap.iterator.foreach((keyValuePair: (List[String], Double)) => {
//        val key = keyValuePair._1
//        val value = keyValuePair._2
//        key.foreach(str => buffer.put(str.getBytes()))
//        buffer.putDouble(value)
//      })
//      buffer.array()
      NGrams.kryoSerializer.newInstance().serialize[NGramBuffer](obj).array()
    }

    final def deserialize(bytes: Array[Byte]): NGramBuffer = {
      NGrams.kryoSerializer.newInstance().deserialize[NGramBuffer](ByteBuffer.wrap(bytes))

      //      val buffer = ByteBuffer.wrap(bytes)
//      val n = buffer.getInt()
//      val k = buffer.getInt()
//      val accuracy = buffer.getInt()
//
//      buffer.get()
//      var i = 0
//      while (i < sampledLength) {
//        val value = buffer.getDouble()
//        val g = buffer.getInt()
//        val delta = buffer.getInt()
//        sampled(i) = Stats(value, g, delta)
//        i += 1
//      }
//      val summary = new QuantileSummaries(compressThreshold, relativeError, sampled, count)
//      new PercentileDigest(summary, isCompressed = true)
    }
  }

  val serializer: NGramBufferSerializer = new NGramBufferSerializer
}
