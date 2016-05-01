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

package org.apache.spark.scheduler

import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark.{SharedSparkContext, SparkFunSuite}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.serializer.KryoTest._
import org.apache.spark.util.Benchmark

class KryoBenchmark extends SparkFunSuite with SharedSparkContext {
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)

  val benchmark = new Benchmark("Benchmark Kryo Unsafe vs safe Serialization", 1024 * 1024 * 15, 10)

  test(s"Benchmark Kryo Unsafe vs safe Serialization") {
    Seq (false, true).foreach (runBenchmark)
    benchmark.run()

    // scalastyle:off
    /*
      Java HotSpot(TM) 64-Bit Server VM 1.8.0_60-b27 on Mac OS X 10.11.4
      Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz

      Benchmark Kryo Unsafe vs safe Serialization: Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
      ------------------------------------------------------------------------------------------------
      basicTypes: Int unsafe:false                     2 /    4       8988.0           0.1       1.0X
      basicTypes: Long unsafe:false                    1 /    1      13981.3           0.1       1.6X
      basicTypes: Float unsafe:false                   1 /    1      14460.6           0.1       1.6X
      basicTypes: Double unsafe:false                  1 /    1      15876.9           0.1       1.8X
      Array: Int unsafe:false                         33 /   44        474.8           2.1       0.1X
      Array: Long unsafe:false                        18 /   25        888.6           1.1       0.1X
      Array: Float unsafe:false                       10 /   16       1627.4           0.6       0.2X
      Array: Double unsafe:false                      10 /   13       1523.1           0.7       0.2X
      Map of string->Double unsafe:false             413 /  447         38.1          26.3       0.0X
      basicTypes: Int unsafe:true                      1 /    1      16402.6           0.1       1.8X
      basicTypes: Long unsafe:true                     1 /    1      19732.1           0.1       2.2X
      basicTypes: Float unsafe:true                    1 /    1      19752.9           0.1       2.2X
      basicTypes: Double unsafe:true                   1 /    1      23111.4           0.0       2.6X
      Array: Int unsafe:true                           7 /    8       2239.9           0.4       0.2X
      Array: Long unsafe:true                          8 /    9       2000.1           0.5       0.2X
      Array: Float unsafe:true                         7 /    8       2191.5           0.5       0.2X
      Array: Double unsafe:true                        9 /   10       1841.2           0.5       0.2X
      Map of string->Double unsafe:true              387 /  407         40.7          24.6       0.0X
    */
    // scalastyle:on
  }

  private def runBenchmark(useUnsafe: Boolean): Unit = {
    conf.set("spark.kryo.useUnsafe", useUnsafe.toString)
    val ser = new KryoSerializer(conf).newInstance()

    def addBenchmark(name: String, values: Long)(f: => Long): Unit = {
      benchmark.addCase(s"$name unsafe:$useUnsafe") { iters =>
        f + 1
      }
    }

    def check[T: ClassTag](t: T): Int = {
      if (ser.deserialize[T](ser.serialize(t)) === t) 1 else 0
    }

    var N = 5000000
    basicTypes("Int", Random.nextInt)
    basicTypes("Long", Random.nextLong)
    basicTypes("Float", Random.nextFloat)
    basicTypes("Double", Random.nextDouble)

    N = 100000
    basicTypeArray("Int", Random.nextInt)
    basicTypeArray("Long", Random.nextLong)
    basicTypeArray("Float", Random.nextFloat)
    basicTypeArray("Double", Random.nextDouble)

    N = 500
    addBenchmark("Map of string->Double", N) {
      var sum = 0L
      for (i <- 1 to N) {
        val map = Array.fill(i)((Random.nextString(i/10), Random.nextDouble())).toMap
        sum += check(map)
      }
      sum
    }

    def basicTypes[T: ClassTag](name: String, fn: () => T): Unit = {
      addBenchmark(s"basicTypes: $name", N) {
        var sum = 0L
        for (i <- 1 to N) {
          sum += check(fn)
        }
        sum
      }
    }

    def basicTypeArray[T: ClassTag](name: String, fn: () => T): Unit = {
      addBenchmark(s"Array: $name", N) {
        var sum = 0L
        for (i <- 1 to N) {
          val arr = Array.fill[T](i)(fn())
          sum += check(arr)
        }
        sum
      }
    }
  }

}
