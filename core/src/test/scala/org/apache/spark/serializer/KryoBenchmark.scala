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
      basicTypes: Int with unsafe:true               151 /  170        104.2           9.6       1.0X
      basicTypes: Long with unsafe:true              175 /  191         89.8          11.1       0.9X
      basicTypes: Float with unsafe:true             177 /  184         88.8          11.3       0.9X
      basicTypes: Double with unsafe:true            193 /  216         81.4          12.3       0.8X
      Array: Int with unsafe:true                    513 /  587         30.7          32.6       0.3X
      Array: Long with unsafe:true                  1211 / 1358         13.0          77.0       0.1X
      Array: Float with unsafe:true                  890 /  964         17.7          56.6       0.2X
      Array: Double with unsafe:true                1335 / 1428         11.8          84.9       0.1X
      Map of string->Double  with unsafe:true        931 /  988         16.9          59.2       0.2X
      basicTypes: Int with unsafe:false              197 /  217         79.9          12.5       0.8X
      basicTypes: Long with unsafe:false             219 /  240         71.8          13.9       0.7X
      basicTypes: Float with unsafe:false            208 /  217         75.7          13.2       0.7X
      basicTypes: Double with unsafe:false           208 /  225         75.6          13.2       0.7X
      Array: Int with unsafe:false                  2559 / 2681          6.1         162.7       0.1X
      Array: Long with unsafe:false                 3425 / 3516          4.6         217.8       0.0X
      Array: Float with unsafe:false                2025 / 2134          7.8         128.7       0.1X
      Array: Double with unsafe:false               2241 / 2358          7.0         142.5       0.1X
      Map of string->Double  with unsafe:false      1044 / 1085         15.1          66.4       0.1X
    */
    // scalastyle:on
  }

  private def runBenchmark(useUnsafe: Boolean): Unit = {
    def check[T: ClassTag](t: T, ser: SerializerInstance): Int = {
      if (ser.deserialize[T](ser.serialize(t)) === t) 1 else 0
    }

    // Benchmark Primitives
    val basicTypeCount = 1000000
    def basicTypes[T: ClassTag](name: String, gen: () => T): Unit = {
      lazy val ser = createSerializer(useUnsafe)
      val arrayOfBasicType: Array[T] = Array.fill(basicTypeCount)(gen())

      benchmark.addCase(s"basicTypes: $name with unsafe:$useUnsafe") { _ =>
        var sum = 0L
        var i = 0
        while (i < basicTypeCount) {
          sum += check(arrayOfBasicType(i), ser)
          i += 1
        }
        sum
      }
    }
    basicTypes("Int", Random.nextInt)
    basicTypes("Long", Random.nextLong)
    basicTypes("Float", Random.nextFloat)
    basicTypes("Double", Random.nextDouble)

    // Benchmark Array of Primitives
    val arrayCount = 10000
    def basicTypeArray[T: ClassTag](name: String, gen: () => T): Unit = {
      lazy val ser = createSerializer(useUnsafe)
      val arrayOfArrays: Array[Array[T]] =
        Array.fill(arrayCount)(Array.fill[T](Random.nextInt(arrayCount))(gen()))

      benchmark.addCase(s"Array: $name with unsafe:$useUnsafe") { _ =>
        var sum = 0L
        var i = 0
        while (i < arrayCount) {
          val arr = arrayOfArrays(i)
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

    // Benchmark Maps
    val mapsCount = 1000
    lazy val ser = createSerializer(useUnsafe)
    val arrayOfMaps: Array[Map[String, Double]] = Array.fill(mapsCount) {
      Array.fill(Random.nextInt(mapsCount)) {
        (Random.nextString(mapsCount / 10), Random.nextDouble())
      }.toMap
    }

    benchmark.addCase(s"Map of string->Double  with unsafe:$useUnsafe") { _ =>
      var sum = 0L
      var i = 0
      while (i < mapsCount) {
        val map = arrayOfMaps(i)
        sum += check(map, ser)
        i += 1
      }
      sum
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
