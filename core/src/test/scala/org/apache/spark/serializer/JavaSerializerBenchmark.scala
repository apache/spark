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

import java.nio.ByteBuffer

import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.internal.config._

/**
 * Benchmark for Java Serializer Deserialization use vs not use Class Resolve Cache.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar>
 *   2. build/sbt "core/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/test:runMain <this class>"
 *      Results will be written to "benchmarks/JavaSerializerBenchmark-results.txt".
 * }}}
 */
object JavaSerializerBenchmark extends BenchmarkBase {

  val N = 100000
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val name = "Benchmark Java Serializer Deserialization use vs not use Class Resolve Cache"
    runBenchmark(name) {
      val benchmark = new Benchmark(name, N, 10, output = output)
      Seq(true, false).foreach(useUnsafe => run(useUnsafe, benchmark))
      benchmark.run()
    }
  }

  private def run(useCache: Boolean, benchmark: Benchmark): Unit = {
    def createCase[T: ClassTag](name: String, size: Int, gen: () => T): Unit = {
      lazy val ser = createSerializer(useCache)
      val data: Array[ByteBuffer] = Array.fill(size)(ser.serialize(gen()))

      benchmark.addCase(s"$name with cache:$useCache") { _ =>
        var i = 0
        var s: ByteBuffer = null
        while (i < size) {
          s = data(i)
          s.rewind()
          ser.deserialize(s)
          i += 1
        }
      }
    }
    createCase("Int", N, () => Random.nextInt())
    createCase("Long", N, () => Random.nextLong())
    createCase("Float", N, () => Random.nextFloat())
    createCase("Double", N, () => Random.nextDouble())
    createCase("String(10)", N, () => Random.nextString(10))

    val personNameLen = 10
    createCase("Person(String(10), Int)", N,
      () => Person(Random.nextString(personNameLen), Random.nextInt()))

    val schoolLen = 10
    createCase("Student(Person, String(10))", N, () =>
      Student(Person(Random.nextString(personNameLen), Random.nextInt()),
        Random.nextString(schoolLen)))
  }

  def createSerializer(cache: Boolean): SerializerInstance = {
    val conf = new SparkConf()
    conf.set(JAVA_SERIALIZER_CACHE_RESOLVED_CLASSES, cache)

    new JavaSerializer(conf).newInstance()
  }
}

case class Person(name: String, id: Int)
case class Student(person: Person, school: String)
