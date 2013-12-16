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

package org.apache.spark

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import collection.mutable
import java.util.Random
import scala.math.exp
import scala.math.signum
import org.apache.spark.SparkContext._

class AccumulatorSuite extends FunSuite with ShouldMatchers with LocalSparkContext {


  implicit def setAccum[A] = new AccumulableParam[mutable.Set[A], A] {
    def addInPlace(t1: mutable.Set[A], t2: mutable.Set[A]) : mutable.Set[A] = {
      t1 ++= t2
      t1
    }
    def addAccumulator(t1: mutable.Set[A], t2: A) : mutable.Set[A] = {
      t1 += t2
      t1
    }
    def zero(t: mutable.Set[A]) : mutable.Set[A] = {
      new mutable.HashSet[A]()
    }
  }

  test ("basic accumulation"){
    sc = new SparkContext("local", "test")
    val acc : Accumulator[Int] = sc.accumulator(0)

    val d = sc.parallelize(1 to 20)
    d.foreach{x => acc += x}
    acc.value should be (210)


    val longAcc = sc.accumulator(0l)
    val maxInt = Integer.MAX_VALUE.toLong
    d.foreach{x => longAcc += maxInt + x}
    longAcc.value should be (210l + maxInt * 20)
  }

  test ("value not assignable from tasks") {
    sc = new SparkContext("local", "test")
    val acc : Accumulator[Int] = sc.accumulator(0)

    val d = sc.parallelize(1 to 20)
    evaluating {d.foreach{x => acc.value = x}} should produce [Exception]
  }

  test ("add value to collection accumulators") {
    val maxI = 1000
    for (nThreads <- List(1, 10)) { //test single & multi-threaded
      sc = new SparkContext("local[" + nThreads + "]", "test")
      val acc: Accumulable[mutable.Set[Any], Any] = sc.accumulable(new mutable.HashSet[Any]())
      val d = sc.parallelize(1 to maxI)
      d.foreach {
        x => acc += x
      }
      val v = acc.value.asInstanceOf[mutable.Set[Int]]
      for (i <- 1 to maxI) {
        v should contain(i)
      }
      resetSparkContext()
    }
  }

  test ("value not readable in tasks") {
    val maxI = 1000
    for (nThreads <- List(1, 10)) { //test single & multi-threaded
      sc = new SparkContext("local[" + nThreads + "]", "test")
      val acc: Accumulable[mutable.Set[Any], Any] = sc.accumulable(new mutable.HashSet[Any]())
      val d = sc.parallelize(1 to maxI)
      evaluating {
        d.foreach {
          x => acc.value += x
        }
      } should produce [SparkException]
      resetSparkContext()
    }
  }

  test ("collection accumulators") {
    val maxI = 1000
    for (nThreads <- List(1, 10)) {
      // test single & multi-threaded
      sc = new SparkContext("local[" + nThreads + "]", "test")
      val setAcc = sc.accumulableCollection(mutable.HashSet[Int]())
      val bufferAcc = sc.accumulableCollection(mutable.ArrayBuffer[Int]())
      val mapAcc = sc.accumulableCollection(mutable.HashMap[Int,String]())
      val d = sc.parallelize((1 to maxI) ++ (1 to maxI))
      d.foreach {
        x => {setAcc += x; bufferAcc += x; mapAcc += (x -> x.toString)}
      }

      // Note that this is typed correctly -- no casts necessary
      setAcc.value.size should be (maxI)
      bufferAcc.value.size should be (2 * maxI)
      mapAcc.value.size should be (maxI)
      for (i <- 1 to maxI) {
        setAcc.value should contain(i)
        bufferAcc.value should contain(i)
        mapAcc.value should contain (i -> i.toString)
      }
      resetSparkContext()
    }
  }

  test ("localValue readable in tasks") {
    val maxI = 1000
    for (nThreads <- List(1, 10)) { //test single & multi-threaded
      sc = new SparkContext("local[" + nThreads + "]", "test")
      val acc: Accumulable[mutable.Set[Any], Any] = sc.accumulable(new mutable.HashSet[Any]())
      val groupedInts = (1 to (maxI/20)).map {x => (20 * (x - 1) to 20 * x).toSet}
      val d = sc.parallelize(groupedInts)
      d.foreach {
        x => acc.localValue ++= x
      }
      acc.value should be ( (0 to maxI).toSet)
      resetSparkContext()
    }
  }

}
