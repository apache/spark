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

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Kryo._
import org.apache.spark.serializer.KryoTest._

/**
 * Benchmark for Kryo Unsafe vs safe Serialization.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/test:runMain <this class>"
 *      Results will be written to "benchmarks/KryoBenchmark-results.txt".
 * }}}
 */
object KryoBenchmark extends BenchmarkBase {

  val N = 1000000
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val name = "Benchmark Kryo Unsafe vs safe Serialization"
    runBenchmark(name) {
      val benchmark = new Benchmark(name, N, 10, output = output)
      Seq(true, false).foreach(useUnsafe => run(useUnsafe, benchmark))
      benchmark.run()
    }
  }

  private def run(useUnsafe: Boolean, benchmark: Benchmark): Unit = {
    def check[T: ClassTag](t: T, ser: SerializerInstance): Int = {
      if (ser.deserialize[T](ser.serialize(t)) == t) 1 else 0
    }

    // Benchmark Primitives
    def basicTypes[T: ClassTag](name: String, gen: () => T): Unit = {
      lazy val ser = createSerializer(useUnsafe)
      val arrayOfBasicType: Array[T] = Array.fill(N)(gen())

      benchmark.addCase(s"basicTypes: $name with unsafe:$useUnsafe") { _ =>
        var sum = 0L
        var i = 0
        while (i < N) {
          sum += check(arrayOfBasicType(i), ser)
          i += 1
        }
        sum
      }
    }
    basicTypes("Int", () => Random.nextInt())
    basicTypes("Long", () => Random.nextLong())
    basicTypes("Float", () => Random.nextFloat())
    basicTypes("Double", () => Random.nextDouble())

    // Benchmark Array of Primitives
    val arrayCount = 4000
    val arrayLength = N / arrayCount
    def basicTypeArray[T: ClassTag](name: String, gen: () => T): Unit = {
      lazy val ser = createSerializer(useUnsafe)
      val arrayOfArrays: Array[Array[T]] =
        Array.fill(arrayCount)(Array.fill[T](arrayLength + Random.nextInt(arrayLength / 4))(gen()))

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
    basicTypeArray("Int", () => Random.nextInt())
    basicTypeArray("Long", () => Random.nextLong())
    basicTypeArray("Float", () => Random.nextFloat())
    basicTypeArray("Double", () => Random.nextDouble())

    // Benchmark Maps
    val mapsCount = 200
    val mapKeyLength = 20
    val mapLength = N  / mapsCount / mapKeyLength
    lazy val ser = createSerializer(useUnsafe)
    val arrayOfMaps: Array[Map[String, Double]] = Array.fill(mapsCount) {
      Array.fill(mapLength + Random.nextInt(mapLength / 4)) {
        (Random.nextString(mapKeyLength), Random.nextDouble())
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
    conf.set(SERIALIZER, "org.apache.spark.serializer.KryoSerializer")
    conf.set(KRYO_USER_REGISTRATORS, Seq(classOf[MyRegistrator].getName))
    conf.set(KRYO_USE_UNSAFE, useUnsafe)

    new KryoSerializer(conf).newInstance()
  }

}
