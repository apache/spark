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

package org.apache.spark.ml.linalg

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * In many machine learning algorithms, we have to treeAggregate large vectors/arrays
 * due to the large number of features. Unfortunately, the treeAggregate operation of RDD
 * well be low efficiency when the dimension of vectors/arrays is bigger than million.
 * Because high dimension of vector/array always occupy more than 100MB Memory, transferring
 * a 100MB element among executors is pretty low efficiency in Spark.
 *
 * ElementwiseSlicing slices a vector/array into n pieces low dimension vector/array before
 * transferred among executors. And slices will compose into a high dimension vector/array in
 * driver.
 *
 */
trait ElementwiseSlicing[T] extends Serializable {
  /**
   * slice x into numSlice pieces.
   * @param x - the item to be slice
   * @param numSlice - the number of slices
   * @return the iterator of slices
   *
   */
  def slice(x: T, numSlice: Int): Iterator[T]

  /**
   * compose slices into item.
   * @param iter - the iterator of slices to be composed
   * @return the composed item of T
   */
  def compose(iter: Iterator[T]): T
}


object ElementwiseSlicing {

  /**
   * implicitly convert Array[T] to ElementwiseSlicing[ Array[T] ]
   */
  implicit def arrayToSlicing[T: ClassTag]: ElementwiseSlicing[Array[T]] = {
    new ElementwiseSlicing[Array[T]] {
      override def slice(x: Array[T], num: Int): Iterator[Array[T]] = {
        val sliceLength = math.ceil(x.length.toDouble / num).toInt
        x.sliding(sliceLength, sliceLength)
      }

      override def compose(iter: Iterator[Array[T]]): Array[T] = {
        val (iter1, iter2) = iter.duplicate

        val totalLength = iter1.map(arr => arr.length).sum
        val comArray = new Array[T](totalLength)
        var accumNum = 0
        iter2.foreach { arr =>
          System.arraycopy(arr, 0, comArray, accumNum, arr.length)
          accumNum += arr.length
        }
        comArray
      }
    }
  }

  /**
   * implicitly convert Iterable[T] to ElementwiseSlicing[ Iterable[T] ]
   */
  implicit def iterableToSlicing[T: ClassTag]: ElementwiseSlicing[Iterable[T]] = {
    new ElementwiseSlicing[Iterable[T]] {
      override def slice(x: Iterable[T], num: Int): Iterator[Iterable[T]] = {
        val sliceLength = math.ceil(x.size.toDouble / num).toInt
        x.sliding(sliceLength, sliceLength)
      }

      override def compose(iter: Iterator[Iterable[T]]): Iterable[T] = {
        val (iter1, iter2) = iter.duplicate

        val totalLength = iter1.map(it => it.size).sum
        val comArray = new Array[T](totalLength)
        var accumNum = 0
        iter2.foreach { it =>
          it.copyToArray(comArray, accumNum, it.size)
          accumNum += it.size
        }
        comArray.toIterable
      }
    }
  }

  /**
   * implicitly convert Option[ ElementwiseSlicing[T] ] to ElementwiseSlicing[ Option[T] ]
   */
  implicit def optionToSlicing[T](implicit slicing: ElementwiseSlicing[T]):
      ElementwiseSlicing[Option[T]] = {
    new ElementwiseSlicing[Option[T]] {
      override def slice(x: Option[T], num: Int): Iterator[Option[T]] = {
        if (x.isDefined) {
          slicing.slice(x.get, num).map(Some(_))
        } else {
          Iterator.single(Option.empty[T])
        }
      }

      override def compose(iter: Iterator[Option[T]]): Option[T] = {
        val items = iter.flatMap { opt =>
          if (opt.isDefined) Iterator.single(opt.get) else Iterator.empty
        }

        if (items.hasNext) {
          Some(slicing.compose(items))
        } else {
          None
        }
      }
    }
  }

  /**
   * implicitly convert Tuple2[ ElementwiseSlicing[T1], ElementwiseSlicing[T2] ] to
   * ElementwiseSlicing[ Tuple2[T1, T2] ]
   */
  implicit def tuple2ToSlicing[T1: ClassTag, T2: ClassTag]
      (implicit firstSlicing: ElementwiseSlicing[T1], secondSlicing: ElementwiseSlicing[T2]):
      ElementwiseSlicing[(T1, T2)] = {

    new ElementwiseSlicing[(T1, T2)] {
      override def slice(x: (T1, T2), num: Int): Iterator[(T1, T2)] = {
        val firstSlices = implicitly[ElementwiseSlicing[T1]].slice(x._1, num)
        val secondSlices = implicitly[ElementwiseSlicing[T2]].slice(x._2, num)
        firstSlices.zip(secondSlices)
      }

      override def compose(iter: Iterator[(T1, T2)]): (T1, T2) = {
        val (iter1, iter2) = iter.duplicate
        val length = iter1.length

        val firstArr = new Array[T1](length)
        val secondArr = new Array[T2](length)

        var index = 0
        iter2.foreach { t =>
          firstArr(index) = t._1
          secondArr(index) = t._2
          index += 1
        }
        val first = firstSlicing.compose(firstArr.toIterator)
        val second = secondSlicing.compose(secondArr.toIterator)
        (first, second)
      }
    }
  }

  /**
   * implicitly convert Tuple3[ ElementwiseSlicing[T1], ElementwiseSlicing[T2],
   * ElementwiseSlicing[T3] ] to ElementwiseSlicing[ Tuple3[T1, T2, T3] ]
   */

  implicit def tuple3ToSlicing[T1: ClassTag, T2: ClassTag, T3: ClassTag]
      (implicit firstSlicing: ElementwiseSlicing[T1], secondSlicing: ElementwiseSlicing[T2],
       thirdSlicing: ElementwiseSlicing[T3]): ElementwiseSlicing[(T1, T2, T3)] = {

    new ElementwiseSlicing[(T1, T2, T3)] {
      override def slice(x: (T1, T2, T3), num: Int): Iterator[(T1, T2, T3)] = {
        val firstSlices = implicitly[ElementwiseSlicing[T1]].slice(x._1, num)
        val secondSlices = implicitly[ElementwiseSlicing[T2]].slice(x._2, num)
        val thirdSlices = implicitly[ElementwiseSlicing[T3]].slice(x._3, num)

        firstSlices.zip(secondSlices).zip(thirdSlices).map(x => (x._1._1, x._1._2, x._2))
      }

      override def compose(iter: Iterator[(T1, T2, T3)]): (T1, T2, T3) = {
        val (iter1, iter2) = iter.duplicate
        val length = iter1.length

        val firstArr = new Array[T1](length)
        val secondArr = new Array[T2](length)
        val thirdArr = new Array[T3](length)

        var index = 0
        iter2.foreach { t =>
          firstArr(index) = t._1
          secondArr(index) = t._2
          thirdArr(index) = t._3
          index += 1
        }
        val first = firstSlicing.compose(firstArr.toIterator)
        val second = secondSlicing.compose(secondArr.toIterator)
        val third = thirdSlicing.compose(thirdArr.toIterator)
        (first, second, third)
      }
    }
  }

  /**
   * Implicitly convert DenseVector to ElementwiseSlicing[DenseVector]
   */
  implicit def denseVectorToSlicing: ElementwiseSlicing[DenseVector] = {
    new ElementwiseSlicing[DenseVector] {
      override def slice(x: DenseVector, numSlices: Int): Iterator[DenseVector] = {
        val sliceLength = math.ceil(x.size.toDouble / numSlices).toInt
        x.toDense.values.sliding(sliceLength, sliceLength)
          .map(slice => new DenseVector(slice))
      }

      override def compose(iter: Iterator[DenseVector]): DenseVector = {
        val (iter1, iter2) = iter.duplicate

        val totalLength = iter1.map { dv => dv.values.length }.sum

        val comArray = new Array[Double](totalLength)

        var accumNum = 0
        iter2.foreach { dv =>
          dv.values.copyToArray(comArray, accumNum, dv.values.length)
          accumNum += dv.values.length
        }
        new DenseVector(comArray)
      }
    }
  }


  /**
   * Implicitly convert SparseVector to ElementwiseSlicing[SparseVector]
   */
  implicit def sparseVectorToSlicing: ElementwiseSlicing[SparseVector] = {
    new ElementwiseSlicing[SparseVector] {
      override def slice(x: SparseVector, numSlices: Int): Iterator[SparseVector] = {
        val sliceLength = math.ceil(x.size.toDouble / numSlices).toInt

        var thisNumSlice = x.size / numSlices
        if (x.size % numSlices != 0) thisNumSlice += 1

        val result = (0 until thisNumSlice).toArray.map { i =>
          if (i < thisNumSlice - 1) {
            new Array[Double](sliceLength)
          } else {
            new Array[Double](x.size - sliceLength * (numSlices - 1))
          }
        }

        x.indices.foreach { index =>
          val i = index / sliceLength
          val j = index % sliceLength
          result(i)(j)
        }
        result.map(arr => new DenseVector(arr).toSparse).toIterator
      }

      override def compose(iter: Iterator[SparseVector]): SparseVector = {
        val (iter1, iter2) = iter.duplicate

        val totalNnz = iter1.map { sv => sv.numNonzeros }.sum

        val comIndices = new Array[Int](totalNnz)
        val comValues = new Array[Double](totalNnz)

        var accumNum = 0
        var totalSize = 0
        iter2.foreach { sv =>
          sv.values.indices.foreach { i =>
            if (sv.values(i) != 0.0) {
              comIndices(accumNum) = i
              comValues(accumNum) = sv.values(i)
              accumNum += 1
            }
          }
          totalSize += sv.values.length
        }

        new SparseVector(totalSize, comIndices, comValues)
      }
    }
  }


  /**
   * Implicitly convert Vector to ElementwiseSlicing[Vector]
   */
  implicit def vectorToSlicing: ElementwiseSlicing[Vector] = {

    new ElementwiseSlicing[Vector] {
      override def slice(x: Vector, numSlices: Int): Iterator[Vector] = {
        x match {
          case dv: DenseVector =>
            denseVectorToSlicing.slice(dv, numSlices)
          case sv: SparseVector =>
            sparseVectorToSlicing.slice(sv, numSlices)
        }
      }

      override def compose(iter: Iterator[Vector]): Vector = {
        val (iter1, iter2) = iter.duplicate
        iter1.next() match {
          case dv: DenseVector =>
            denseVectorToSlicing.compose(iter2.map(_.toDense))
          case sv: SparseVector =>
            sparseVectorToSlicing.compose(iter2.map(_.toSparse))
        }
      }
    }
  }
}

