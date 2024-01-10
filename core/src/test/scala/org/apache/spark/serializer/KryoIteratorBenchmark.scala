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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Kryo._
import org.apache.spark.serializer.KryoTest._

/**
 * Benchmark for kryo asIterator on a deserialization stream". To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to "benchmarks/KryoSerializerBenchmark-results.txt".
 * }}}
 */
object KryoIteratorBenchmark extends BenchmarkBase {
  val N = 10000
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val name = "Benchmark of kryo asIterator on deserialization stream"
    runBenchmark(name) {
      val benchmark = new Benchmark(name, N, 10, output = output)
      Seq(true, false).map(useIterator => run(useIterator, benchmark))
      benchmark.run()
    }
  }

  private def run(useIterator: Boolean, benchmark: Benchmark): Unit = {
    val ser = createSerializer()

    def roundTrip[T: ClassTag](
        elements: Array[T],
        useIterator: Boolean,
        ser: SerializerInstance): Int = {
      val serialized: Array[Byte] = {
        val baos = new ByteArrayOutputStream()
        val serStream = ser.serializeStream(baos)
        var i = 0
        while (i < elements.length) {
          serStream.writeObject(elements(i))
          i += 1
        }
        serStream.close()
        baos.toByteArray
      }

      val deserStream = ser.deserializeStream(new ByteArrayInputStream(serialized))
      if (useIterator) {
        if (deserStream.asIterator.toArray.length == elements.length) 1 else 0
      } else {
        val res = new Array[T](elements.length)
        var i = 0
        while (i < elements.length) {
          res(i) = deserStream.readValue()
          i += 1
        }
        deserStream.close()
        if (res.length == elements.length) 1 else 0
      }
    }

    def createCase[T: ClassTag](name: String, elementCount: Int, createElement: => T): Unit = {
      val elements = Array.fill[T](elementCount)(createElement)

      benchmark.addCase(
        s"Colletion of $name with $elementCount elements, useIterator: $useIterator") { _ =>
        var sum = 0L
        var i = 0
        while (i < N) {
          sum += roundTrip(elements, useIterator, ser)
          i += 1
        }
        sum
      }
    }

    createCase("int", 1, Random.nextInt())
    createCase("int", 10, Random.nextInt())
    createCase("int", 100, Random.nextInt())
    createCase("string", 1, Random.nextString(5))
    createCase("string", 10, Random.nextString(5))
    createCase("string", 100, Random.nextString(5))
    createCase("Array[int]", 1, Array.fill(10)(Random.nextInt()))
    createCase("Array[int]", 10, Array.fill(10)(Random.nextInt()))
    createCase("Array[int]", 100, Array.fill(10)(Random.nextInt()))
  }

  def createSerializer(): SerializerInstance = {
    val conf = new SparkConf()
    conf.set(SERIALIZER, "org.apache.spark.serializer.KryoSerializer")
    conf.set(KRYO_USER_REGISTRATORS, Seq(classOf[MyRegistrator].getName))

    new KryoSerializer(conf).newInstance()
  }
}
