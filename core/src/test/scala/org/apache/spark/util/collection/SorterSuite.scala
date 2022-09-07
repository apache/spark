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

package org.apache.spark.util.collection

import java.lang.{Float => JFloat}
import java.util.Arrays
import java.util.concurrent.TimeUnit

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.Utils.timeIt
import org.apache.spark.util.random.XORShiftRandom

class SorterSuite extends SparkFunSuite {

  test("equivalent to Arrays.sort") {
    val rand = new XORShiftRandom(123)
    val data0 = Array.tabulate[Int](10000) { i => rand.nextInt() }
    val data1 = data0.clone()
    val data2 = data0.clone()

    Arrays.sort(data0)
    new Sorter(new IntArraySortDataFormat).sort(data1, 0, data1.length, Ordering.Int)
    new Sorter(new KeyReuseIntArraySortDataFormat)
      .sort(data2, 0, data2.length, Ordering[IntWrapper])

    assert(data0 === data1)
    assert(data0 === data2)
  }

  test("KVArraySorter") {
    val rand = new XORShiftRandom(456)

    // Construct an array of keys (to Java sort) and an array where the keys and values
    // alternate. Keys are random doubles, values are ordinals from 0 to length.
    val keys = Array.tabulate[Double](5000) { i => rand.nextDouble() }
    val keyValueArray = Array.tabulate[Number](10000) { i =>
      if (i % 2 == 0) keys(i / 2) else Integer.valueOf(i / 2)
    }

    // Map from generated keys to values, to verify correctness later
    val kvMap =
      keyValueArray.grouped(2).map { case Array(k, v) => k.doubleValue() -> v.intValue() }.toMap

    Arrays.sort(keys)
    new Sorter(new KVArraySortDataFormat[Double, Number])
      .sort(keyValueArray, 0, keys.length, (x, y) => java.lang.Double.compare(x, y))

    keys.zipWithIndex.foreach { case (k, i) =>
      assert(k === keyValueArray(2 * i))
      assert(kvMap(k) === keyValueArray(2 * i + 1))
    }
  }

  // http://www.envisage-project.eu/timsort-specification-and-verification/
  test("SPARK-5984 TimSort bug") {
    val data = TestTimSort.getTimSortBugTestSet(67108864)
    new Sorter(new IntArraySortDataFormat).sort(data, 0, data.length, Ordering.Int)
    (0 to data.length - 2).foreach(i => assert(data(i) <= data(i + 1)))
  }

  test("java.lang.ArrayIndexOutOfBoundsException in TimSort") {
    // scalastyle:off
    val runLengths = Array(76405736, 74830360, 1181532, 787688, 1575376, 2363064, 3938440, 6301504,
      1181532, 393844, 15753760, 1575376, 787688, 393844, 1969220, 3150752, 1181532,787688, 5513816, 3938440,
      1181532, 787688, 1575376, 18116824, 1181532, 787688, 1575376, 2363064, 3938440,787688, 26781392, 1181532,
      787688, 1575376, 2363064, 393844, 4332284, 1181532, 787688, 1575376, 12209164,1181532, 787688, 1575376,
      2363064, 787688, 393844, 4726128, 1575376, 787688, 1969220, 76405758, 53168940,1181532, 787688, 1575376,
      2363064, 3938440, 1575376, 787688, 393844, 10633788, 1181532, 787688, 1575376,2363064, 4332284, 1181532,
      787688, 1575376, 12996852, 1181532, 787688, 1575376, 2363064, 393844, 17329136,1575376, 787688, 393844,
      1969220, 3150752, 1181532, 393844, 7483036, 1575376, 787688, 1969220, 2756908,1181532, 787600, 76405780,
      38202802, 114608494, 66, 44, 88, 176, 352, 704, 1408, 2816, 5632, 11264, 22528,45056, 90112, 180224,
      360448, 720896, 1441792, 2883584, 5767168, 11387222, 22495132, 319836, 213224,426448, 639672, 1066120,
      1705792, 426448, 213224, 106612, 4584316, 426448, 213224, 106612, 533060,106612, 852896, 426448, 213224,
      1599180, 1172732, 319836, 213224, 426448, 5223988, 319836, 213224, 426448,639672, 1066120, 319836, 213224,
      7782676, 426448, 213224, 533060, 746284, 213224, 1705792, 319836, 213224,426448, 639672, 2238852, 426448,
      213224, 106612, 2345464, 426448, 213224, 106612, 533060, 106612, 852896,426448, 213224, 106612, 22921602,
      15245516, 319836, 213224, 426448, 639672, 1172732, 319836, 213224, 426448,3304972, 319836, 213224, 426448,
      639672, 213224, 1279344, 426448, 213224, 533060, 3838032, 319836, 213224,426448, 639672, 213224, 106612,
      5330600, 319836, 213224, 426448, 639672, 1066120, 213224, 2345464, 426448,213224, 106612, 533060, 106612,
      852896, 426448, 213224, 106524, 22921624, 11460724, 34382260, 66, 44, 88, 176,352, 704, 1408, 2816, 5632,
      11264, 22528, 45056, 90112, 180224, 360448, 720896, 1001792, 1783584, 2649020,6739370, 102630, 68420,
      136840, 205260, 342100, 547360, 102630, 68420, 1436820, 102630, 68420, 136840,205260, 342100, 547360,
      102630, 68420, 136840, 205260, 68420, 34210, 1607870, 102630, 68420, 136840,205260, 342100, 68420, 34210,
      2428910, 102630, 68420, 136840, 205260, 34210, 410520, 102630, 68420, 136840,1094720, 102630, 68420,
      136840, 205260, 68420, 34210, 444730, 136840, 68420, 34210, 171050, 34210,6876232, 4618350, 102630, 68420,
      136840, 205260, 34210, 342100, 136840, 68420, 34210, 992090, 102630, 68420,136840, 205260, 68420, 342100,
      205260, 102630, 68420, 1163140, 102630, 68420, 136840, 205260, 68420, 1607870,102630, 68420, 136840,
      205260, 342100, 34210, 684200, 136840, 68420, 171050, 239470, 102630, 68332,6876254, 3438028, 10314194, 66,
      44, 88, 176, 352, 704, 1408, 2816, 5632, 11264, 22528, 45056, 90112, 180224,360448, 500896, 840554,
      2018720, 32736, 21824, 43648, 65472, 21824, 10912, 141856, 43648, 21824, 10912,54560, 10912, 425568, 43648,
      21824, 54560, 76384, 21824, 10912, 185504, 32736, 21824, 43648, 65472, 21824,10912, 491040, 32736, 21824,
      43648, 65472, 109120, 10912, 731104, 32736, 21824, 43648, 65472, 120032, 32736,21824, 43648, 327360, 32736,
      21824, 43648, 65472, 21824, 130944, 43648, 21824, 54560, 2062390, 1396736,32736, 21824, 43648, 65472,
      109120, 43648, 21824, 10912, 294624, 32736, 21824, 43648, 65472, 120032, 32736,21824, 43648, 360096, 32736,
      21824, 43648, 65472, 10912, 480128, 32736, 21824, 43648, 65472, 109120, 196416,65472, 32736, 10912, 76384,
      21824, 10824, 2062412, 1031118, 3093442, 66, 44, 88, 176, 352, 704, 1408, 2816,5632, 11264, 22528, 45056,
      90112, 180224, 258170, 605616, 9768, 6512, 13024, 19536, 6512, 3256, 42328,13024, 6512, 3256, 16280, 3256,
      126984, 13024, 6512, 16280, 22792, 6512, 3256, 55352, 9768, 6512, 13024, 19536,6512, 3256, 146520, 9768,
      6512, 13024, 19536, 32560, 3256, 218152, 9768, 6512, 13024, 19536, 35816, 9768,6512, 13024, 100936, 9768,
      6512, 13024, 19536, 6512, 3256, 39072, 13024, 6512, 16280, 618662, 416768,9768, 6512, 13024, 19536, 32560,
      13024, 6512, 3256, 87912, 9768, 6512, 13024, 19536, 35816, 9768, 6512, 13024,107448, 9768, 6512, 13024,
      19536, 3256, 143264, 13024, 6512, 3256, 16280, 26048, 9768, 3256, 61864, 13024,6512, 16280, 22792, 9768,
      3168, 618684, 309254, 927850, 66, 44, 88, 176, 352, 704, 1408, 2816, 5632,11264, 22440, 23056, 45056,
      72314, 181632, 2838, 1892, 3784, 5676, 9460, 15136, 2838, 946, 37840, 3784,1892, 946, 4730, 7568, 2838,
      1892, 13244, 9460, 2838, 1892, 3784, 43516, 2838, 1892, 3784, 5676, 9460, 1892,65274, 2838, 1892, 3784,
      5676, 946, 10406, 2838, 1892, 3784, 30272, 2838, 1892, 3784, 5676, 1892, 946,12298, 3784, 1892, 946, 4730,
      185438, 127710, 2838, 1892, 3784, 5676, 9460, 3784, 1892, 946, 26488, 2838,1892, 3784, 5676, 946, 10406,
      2838, 1892, 3784, 31218, 2838, 1892, 3784, 5676, 946, 42570, 2838, 1892, 3784,5676, 9460, 17974, 3784,
      1892, 4730, 6622, 2838, 1804, 185460, 92642, 278014, 66, 44, 88, 176, 352, 704,1408, 2816, 5632, 9064,
      11528, 23606, 54340, 858, 572, 1144, 1716, 2860, 4576, 858, 286, 11440, 1144,572, 286, 1430, 2288, 858,
      572, 4004, 2860, 858, 572, 1144, 13156, 858, 572, 1144, 1716, 2860, 572, 19448,858, 572, 1144, 1716, 286,
      3146, 858, 572, 1144, 8866, 858, 572, 1144, 1716, 572, 286, 3432, 1144, 572,1430, 55506, 38610, 858, 572,
      1144, 1716, 2860, 1144, 572, 286, 7722, 858, 572, 1144, 1716, 3146, 858, 572,1144, 9438, 858, 572, 1144,
      1716, 286, 12584, 1144, 572, 286, 1430, 2288, 858, 286, 5434, 1144, 572, 1430,2002, 858, 484, 55528, 27676,
      83116, 66, 44, 88, 176, 352, 704, 1408, 1716, 3872, 8118, 16192, 264, 176, 352,528, 176, 88, 1144, 352,
      176, 88, 440, 88, 3432, 352, 176, 440, 616, 176, 88, 1408, 264, 176, 352, 528,176, 88, 3960, 264, 176, 352,
      528, 880, 88, 5808, 264, 176, 352, 528, 968, 264, 176, 352, 2640, 264, 176,352, 528, 176, 1056, 352, 176,
      440, 16566, 11264, 264, 176, 352, 528, 880, 352, 176, 2376, 264, 176, 352, 528,968, 264, 176, 352, 2816,
      264, 176, 352, 528, 88, 3872, 264, 176, 352, 528, 880, 1584, 528, 264, 88, 616,176, 16588, 8206, 24706, 66,
      44, 88, 176, 352, 704, 1408, 2090, 4708, 66, 44, 88, 132, 220, 352, 66, 44, 88,990, 66, 44, 88, 132, 220,
      418, 88, 44, 110, 154, 66, 44, 1122, 66, 44, 88, 132, 220, 88, 44, 22, 1716,88, 44, 110, 154, 44, 352, 66,
      44, 88, 132, 44, 22, 462, 110, 44, 22, 528, 88, 44, 22, 110, 22, 176, 88, 44,22, 4950, 3256, 66, 44, 88,
      132, 22, 242, 66, 44, 88, 704, 66, 44, 88, 132, 44, 22, 264, 88, 44, 110, 814,88, 44, 110, 154, 22, 1144,
      66, 44, 88, 132, 220, 44, 22, 506, 88, 44, 22, 110, 22, 176, 88, 44, 22, 4972,2398, 7282, 66, 44, 88, 176,
      242, 418, 660, 1496, 66, 44, 88, 132, 242, 66, 44, 88, 682, 66, 44, 88, 132,44, 22, 264, 88, 44, 110, 1716,
      858, 88, 44, 110, 154, 44, 22, 352, 66, 44, 88, 132, 44, 22, 1738, 198, 1760,175, 156, 18, 17, 19, 36, 65,
      21, 20, 22, 18, 452, 114, 95, 18, 17, 21, 36, 18, 17, 115, 76, 144, 44, 38, 61,20, 19, 21, 17)
    // scalastyle:on
    val arrayToSortSize = 1091482190
    val arrayToSort = new Array[Byte](arrayToSortSize)
    var sum: Int = -1
    for (i <- runLengths) {
      sum += i
      arrayToSort(sum) = 1
    }
    val sorter = new Sorter(new ByteArraySortDataFormat())
    sorter.sort(arrayToSort, 0, arrayToSort.length, Ordering.Byte)
    // The sort must finish without ArrayIndexOutOfBoundsException
    // The arrayToSort contains runLengths.length elements of 1, and others are 0.
    // Those 1 must be placed at the end of arrayToSort after sorting.
    /*
    var i: Int = 0
    sum = 0
    val amountOfZeros = arrayToSort.length - runLengths.length
    do {
      sum += arrayToSort(i)
      i += 1
    } while (i < amountOfZeros)
    assert(sum === 0)

    val sizeOfArrayToSort = arrayToSort.length
    do {
      sum += arrayToSort(i)
      i += 1
    } while (i < sizeOfArrayToSort)
    assert(sum === runLengths.length)
    */
  }

  /** Runs an experiment several times. */
  def runExperiment(name: String, skip: Boolean = false)(f: => Unit, prepare: () => Unit): Unit = {
    if (skip) {
      logInfo(s"Skipped experiment $name.")
      return
    }

    val firstTry = TimeUnit.NANOSECONDS.toMillis(timeIt(1)(f, Some(prepare)))
    System.gc()

    var i = 0
    var next10: Long = 0
    while (i < 10) {
      val time = TimeUnit.NANOSECONDS.toMillis(timeIt(1)(f, Some(prepare)))
      next10 += time
      logInfo(s"$name: Took $time ms")
      i += 1
    }

    logInfo(s"$name: ($firstTry ms first try, ${next10 / 10} ms average)")
  }

  /**
   * This provides a simple benchmark for comparing the Sorter with Java internal sorting.
   * Ideally these would be executed one at a time, each in their own JVM, so their listing
   * here is mainly to have the code. Running multiple tests within the same JVM session would
   * prevent JIT inlining overridden methods and hence hurt the performance.
   *
   * The goal of this code is to sort an array of key-value pairs, where the array physically
   * has the keys and values alternating. The basic Java sorts work only on the keys, so the
   * real Java solution is to make Tuple2s to store the keys and values and sort an array of
   * those, while the Sorter approach can work directly on the input data format.
   */
  ignore("Sorter benchmark for key-value pairs") {
    val numElements = 25000000 // 25 mil
    val rand = new XORShiftRandom(123)

    // Test our key-value pairs where each element is a Tuple2[Float, Integer].

    val kvTuples = Array.tabulate(numElements) { i =>
      (JFloat.valueOf(rand.nextFloat()), Integer.valueOf(i))
    }

    val kvTupleArray = new Array[AnyRef](numElements)
    val prepareKvTupleArray = () => {
      System.arraycopy(kvTuples, 0, kvTupleArray, 0, numElements)
    }
    runExperiment("Tuple-sort using Arrays.sort()")({
      Arrays.sort(kvTupleArray, (x: AnyRef, y: AnyRef) =>
        x.asInstanceOf[(JFloat, _)]._1.compareTo(y.asInstanceOf[(JFloat, _)]._1))
    }, prepareKvTupleArray)

    // Test our Sorter where each element alternates between Float and Integer, non-primitive

    val keyValues = {
      val data = new Array[AnyRef](numElements * 2)
      var i = 0
      while (i < numElements) {
        data(2 * i) = kvTuples(i)._1
        data(2 * i + 1) = kvTuples(i)._2
        i += 1
      }
      data
    }

    val keyValueArray = new Array[AnyRef](numElements * 2)
    val prepareKeyValueArray = () => {
      System.arraycopy(keyValues, 0, keyValueArray, 0, numElements * 2)
    }

    val sorter = new Sorter(new KVArraySortDataFormat[JFloat, AnyRef])
    runExperiment("KV-sort using Sorter")({
      sorter.sort(keyValueArray, 0, numElements, (x: JFloat, y: JFloat) => x.compareTo(y))
    }, prepareKeyValueArray)
  }

  /**
   * Tests for sorting with primitive keys with/without key reuse. Java's Arrays.sort is used as
   * reference, which is expected to be faster but it can only sort a single array. Sorter can be
   * used to sort parallel arrays.
   *
   * Ideally these would be executed one at a time, each in their own JVM, so their listing
   * here is mainly to have the code. Running multiple tests within the same JVM session would
   * prevent JIT inlining overridden methods and hence hurt the performance.
   */
  ignore("Sorter benchmark for primitive int array") {
    val numElements = 25000000 // 25 mil
    val rand = new XORShiftRandom(123)

    val ints = Array.fill(numElements)(rand.nextInt())
    val intObjects = {
      val data = new Array[Integer](numElements)
      var i = 0
      while (i < numElements) {
        data(i) = Integer.valueOf(ints(i))
        i += 1
      }
      data
    }

    val intObjectArray = new Array[Integer](numElements)
    val prepareIntObjectArray = () => {
      System.arraycopy(intObjects, 0, intObjectArray, 0, numElements)
    }

    runExperiment("Java Arrays.sort() on non-primitive int array")(
      Arrays.sort(intObjectArray, (x: Integer, y: Integer) => x.compareTo(y)),
      prepareIntObjectArray)

    val intPrimitiveArray = new Array[Int](numElements)
    val prepareIntPrimitiveArray = () => {
      System.arraycopy(ints, 0, intPrimitiveArray, 0, numElements)
    }

    runExperiment("Java Arrays.sort() on primitive int array")({
      Arrays.sort(intPrimitiveArray)
    }, prepareIntPrimitiveArray)

    val sorterWithoutKeyReuse = new Sorter(new IntArraySortDataFormat)
    runExperiment("Sorter without key reuse on primitive int array")({
      sorterWithoutKeyReuse.sort(intPrimitiveArray, 0, numElements, Ordering[Int])
    }, prepareIntPrimitiveArray)

    val sorterWithKeyReuse = new Sorter(new KeyReuseIntArraySortDataFormat)
    runExperiment("Sorter with key reuse on primitive int array")({
      sorterWithKeyReuse.sort(intPrimitiveArray, 0, numElements, Ordering[IntWrapper])
    }, prepareIntPrimitiveArray)
  }
}

abstract class AbstractIntArraySortDataFormat[K] extends SortDataFormat[K, Array[Int]] {

  override def swap(data: Array[Int], pos0: Int, pos1: Int): Unit = {
    val tmp = data(pos0)
    data(pos0) = data(pos1)
    data(pos1) = tmp
  }

  override def copyElement(src: Array[Int], srcPos: Int, dst: Array[Int], dstPos: Int): Unit = {
    dst(dstPos) = src(srcPos)
  }

  /** Copy a range of elements starting at src(srcPos) to dest, starting at destPos. */
  override def copyRange(src: Array[Int], srcPos: Int,
      dst: Array[Int], dstPos: Int, length: Int): Unit = {
    System.arraycopy(src, srcPos, dst, dstPos, length)
  }

  /** Allocates a new structure that can hold up to 'length' elements. */
  override def allocate(length: Int): Array[Int] = {
    new Array[Int](length)
  }
}

abstract class AbstractByteArraySortDataFormat[K] extends SortDataFormat[K, Array[Byte]] {

  override def swap(data: Array[Byte], pos0: Int, pos1: Int): Unit = {
    val tmp = data(pos0)
    data(pos0) = data(pos1)
    data(pos1) = tmp
  }

  override def copyElement(src: Array[Byte], srcPos: Int, dst: Array[Byte], dstPos: Int): Unit = {
    dst(dstPos) = src(srcPos)
  }

  /** Copy a range of elements starting at src(srcPos) to dest, starting at destPos. */
  override def copyRange(src: Array[Byte],
                         srcPos: Int, dst: Array[Byte], dstPos: Int, length: Int): Unit = {
    System.arraycopy(src, srcPos, dst, dstPos, length)
  }

  /** Allocates a new structure that can hold up to 'length' elements. */
  override def allocate(length: Int): Array[Byte] = {
    new Array[Byte](length)
  }
}


/** Format to sort a simple Array[Int]. Could be easily generified and specialized. */
class IntArraySortDataFormat extends AbstractIntArraySortDataFormat[Int] {

  override protected def getKey(data: Array[Int], pos: Int): Int = {
    data(pos)
  }
}

class ByteArraySortDataFormat extends AbstractByteArraySortDataFormat[Byte] {

  override protected def getKey(data: Array[Byte], pos: Int): Byte = {
    data(pos)
  }
}

/** Wrapper of Int for key reuse. */
class IntWrapper(var key: Int = 0) extends Ordered[IntWrapper] {

  override def compare(that: IntWrapper): Int = {
    Ordering.Int.compare(key, that.key)
  }
}

/** SortDataFormat for Array[Int] with reused keys. */
class KeyReuseIntArraySortDataFormat extends AbstractIntArraySortDataFormat[IntWrapper] {

  override def newKey(): IntWrapper = {
    new IntWrapper()
  }

  override def getKey(data: Array[Int], pos: Int, reuse: IntWrapper): IntWrapper = {
    if (reuse == null) {
      new IntWrapper(data(pos))
    } else {
      reuse.key = data(pos)
      reuse
    }
  }

  override protected def getKey(data: Array[Int], pos: Int): IntWrapper = {
    getKey(data, pos, null)
  }
}
