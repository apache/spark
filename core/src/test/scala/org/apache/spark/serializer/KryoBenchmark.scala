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

package org.apache.spark.serializer

import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.serializer.KryoTest._
import org.apache.spark.util.Benchmark

class KryoBenchmark extends SparkFunSuite {
  val benchmark = new Benchmark("Benchmark Kryo Unsafe vs safe Serialization", 1024 * 1024 * 15, 10)

  ignore(s"Benchmark Kryo Unsafe vs safe Serialization") {
    Seq (true, false).foreach (runBenchmark)
    benchmark.run()

    // scalastyle:off
    /*
      Benchmark Kryo Unsafe vs safe Serialization: Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
      ------------------------------------------------------------------------------------------------
      basicTypes: Int unsafe:true                    160 /  178         98.5          10.1       1.0X
      basicTypes: Long unsafe:true                   210 /  218         74.9          13.4       0.8X
      basicTypes: Float unsafe:true                  203 /  213         77.5          12.9       0.8X
      basicTypes: Double unsafe:true                 226 /  235         69.5          14.4       0.7X
      Array: Int unsafe:true                        1087 / 1101         14.5          69.1       0.1X
      Array: Long unsafe:true                       2758 / 2844          5.7         175.4       0.1X
      Array: Float unsafe:true                      1511 / 1552         10.4          96.1       0.1X
      Array: Double unsafe:true                     2942 / 2972          5.3         187.0       0.1X
      Map of string->Double unsafe:true             2645 / 2739          5.9         168.2       0.1X
      basicTypes: Int unsafe:false                   211 /  218         74.7          13.4       0.8X
      basicTypes: Long unsafe:false                  247 /  253         63.6          15.7       0.6X
      basicTypes: Float unsafe:false                 211 /  216         74.5          13.4       0.8X
      basicTypes: Double unsafe:false                227 /  233         69.2          14.4       0.7X
      Array: Int unsafe:false                       3012 / 3032          5.2         191.5       0.1X
      Array: Long unsafe:false                      4463 / 4515          3.5         283.8       0.0X
      Array: Float unsafe:false                     2788 / 2868          5.6         177.2       0.1X
      Array: Double unsafe:false                    3558 / 3752          4.4         226.2       0.0X
      Map of string->Double unsafe:false            2806 / 2933          5.6         178.4       0.1X
    */
    // scalastyle:on
  }

  private def runBenchmark(useUnsafe: Boolean): Unit = {
    def addBenchmark(name: String, values: Long)(f: => Long): Unit = {
      benchmark.addCase(s"$name unsafe:$useUnsafe") { _ =>
        f
      }
    }

    def check[T: ClassTag](t: T, ser: SerializerInstance): Int = {
      if (ser.deserialize[T](ser.serialize(t)) === t) 1 else 0
    }

    val N1 = 1000000
    def basicTypes[T: ClassTag](name: String, fn: () => T): Unit = {
      lazy val ser = createSerializer(useUnsafe)
      addBenchmark(s"basicTypes: $name", N1) {
        var sum = 0L
        var i = 0
        while (i < N1) {
          sum += check(fn(), ser)
          i += 1
        }
        sum
      }
    }
    basicTypes("Int", Random.nextInt)
    basicTypes("Long", Random.nextLong)
    basicTypes("Float", Random.nextFloat)
    basicTypes("Double", Random.nextDouble)

    val N2 = 10000
    def basicTypeArray[T: ClassTag](name: String, fn: () => T): Unit = {
      lazy val ser = createSerializer(useUnsafe)
      addBenchmark(s"Array: $name", N2) {
        var sum = 0L
        var i = 0
        while (i < N2) {
          val arr = Array.fill[T](i)(fn())
          sum += check(arr, ser)
          i += 1
        }
        sum
      }
    }
    basicTypeArray("Int", Random.nextInt)
    basicTypeArray("Long", Random.nextLong)
    basicTypeArray("Float", Random.nextFloat)
    basicTypeArray("Double", Random.nextDouble)

    {
      val N3 = 1000
      lazy val ser = createSerializer(useUnsafe)
      addBenchmark("Map of string->Double", N3) {
        var sum = 0L
        var i = 0
        while (i < N3) {
          val map = Array.fill(i)((Random.nextString(i / 10), Random.nextDouble())).toMap
          sum += check(map, ser)
          i += 1
        }
        sum
      }
    }
  }

  def createSerializer(useUnsafe: Boolean): SerializerInstance = {
    val conf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    conf.set("spark.kryo.unsafe", useUnsafe.toString)

    new KryoSerializer(conf).newInstance()
  }

}
