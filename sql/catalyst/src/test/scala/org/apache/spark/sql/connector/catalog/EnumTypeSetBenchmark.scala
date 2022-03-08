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
package org.apache.spark.sql.connector.catalog

import java.util
import java.util.Collections

import scala.collection.JavaConverters._

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.connector.catalog.TableCapability._

/**
 * Benchmark for EnumSet vs HashSet hold enumeration type
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <spark catalyst test jar>
 *   2. build/sbt "catalyst/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "catalyst/test:runMain <this class>"
 *      Results will be written to "benchmarks/EnumTypeSetBenchmark-results.txt".
 * }}}
 */
object EnumTypeSetBenchmark extends BenchmarkBase {

  def emptyHashSet(): util.Set[TableCapability] = Collections.emptySet()

  def emptyEnumSet(): util.Set[TableCapability] =
    util.EnumSet.noneOf(classOf[TableCapability])

  def oneItemHashSet(): util.Set[TableCapability] = Set(TRUNCATE).asJava

  def oneItemEnumSet(): util.Set[TableCapability] = util.EnumSet.of(TRUNCATE)

  def threeItemsHashSet(): util.Set[TableCapability] =
    Set(BATCH_READ, TRUNCATE, V1_BATCH_WRITE).asJava

  def threeItemsEnumSet(): util.Set[TableCapability] =
    util.EnumSet.of(BATCH_READ, TRUNCATE, V1_BATCH_WRITE)

  def fiveItemsHashSet(): util.Set[TableCapability] =
    Set(BATCH_READ, CONTINUOUS_READ, TRUNCATE, V1_BATCH_WRITE, OVERWRITE_BY_FILTER).asJava

  def fiveItemsEnumSet(): util.Set[TableCapability] =
    util.EnumSet.of(BATCH_READ, CONTINUOUS_READ, TRUNCATE, V1_BATCH_WRITE, OVERWRITE_BY_FILTER)

  def allItemsHashSet(): util.Set[TableCapability] =
    Set( BATCH_READ, MICRO_BATCH_READ, CONTINUOUS_READ, BATCH_WRITE, STREAMING_WRITE, TRUNCATE,
      OVERWRITE_BY_FILTER, OVERWRITE_DYNAMIC, ACCEPT_ANY_SCHEMA, V1_BATCH_WRITE).asJava

  def allItemsEnumSet(): util.Set[TableCapability] =
    util.EnumSet.allOf(classOf[TableCapability])


  def testCreateSetWithEnumType(
      valuesPerIteration: Int,
      sizeLiteral: String,
      creatHashSetFunctions: () => util.Set[TableCapability],
      creatEnumSetFunctions: () => util.Set[TableCapability]): Unit = {

    val benchmark =
      new Benchmark(s"Test create $sizeLiteral Set", valuesPerIteration, output = output)

    benchmark.addCase("Use HashSet") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {creatHashSetFunctions.apply()}
    }

    benchmark.addCase("Use EnumSet") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {creatEnumSetFunctions.apply()}
    }
    benchmark.run()
  }

  def testContainsOperation(
      valuesPerIteration: Int,
      sizeLiteral: String,
      hashSet: util.Set[TableCapability],
      enumSet: util.Set[TableCapability]): Unit = {

    val capabilities = TableCapability.values()

    val benchmark = new Benchmark(
      s"Test contains use $sizeLiteral Set",
        valuesPerIteration * capabilities.length,
        output = output)

    benchmark.addCase("Use HashSet") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        capabilities.foreach(hashSet.contains)
      }
    }

    benchmark.addCase("Use EnumSet") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        capabilities.foreach(enumSet.contains)
      }
    }
    benchmark.run()
  }

  def testCreateAndContainsOperation(
      valuesPerIteration: Int,
      sizeLiteral: String,
      creatHashSetFunctions: () => util.Set[TableCapability],
      creatEnumSetFunctions: () => util.Set[TableCapability]): Unit = {

    val capabilities = TableCapability.values()

    val benchmark = new Benchmark(
      s"Test create and contains use $sizeLiteral Set",
      valuesPerIteration * capabilities.length,
      output = output)

    benchmark.addCase("Use HashSet") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        capabilities.foreach(creatHashSetFunctions.apply().contains)
      }
    }

    benchmark.addCase("Use EnumSet") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        capabilities.foreach(creatEnumSetFunctions.apply().contains)
      }
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    val valuesPerIteration = 100000

    // Test Contains
    testContainsOperation(valuesPerIteration, "empty", emptyHashSet(), emptyEnumSet())
    testContainsOperation(valuesPerIteration, "1 item", oneItemHashSet(), oneItemEnumSet())
    testContainsOperation(valuesPerIteration,
      "3 items", threeItemsHashSet(), threeItemsEnumSet())
    testContainsOperation(valuesPerIteration, "5 items", fiveItemsHashSet(), fiveItemsEnumSet())
    testContainsOperation(valuesPerIteration, s"${TableCapability.values().length} items",
      allItemsHashSet(), allItemsEnumSet())

    // Test Create
    testCreateSetWithEnumType(valuesPerIteration,
      "empty", () => emptyHashSet(), () => emptyEnumSet())
    testCreateSetWithEnumType(valuesPerIteration,
      "1 item", () => oneItemHashSet(), () => oneItemEnumSet())
    testCreateSetWithEnumType(valuesPerIteration, "3 items",
      () => threeItemsHashSet(), () => threeItemsEnumSet())
    testCreateSetWithEnumType(valuesPerIteration, "5 items",
      () => fiveItemsHashSet(), () => fiveItemsEnumSet())
    testCreateSetWithEnumType(valuesPerIteration, s"${TableCapability.values().length} items",
      () => allItemsHashSet(), () => allItemsEnumSet())

    // Test Create and Contains
    testCreateAndContainsOperation(valuesPerIteration, "empty",
      () => emptyHashSet(), () => emptyEnumSet())
    testCreateAndContainsOperation(valuesPerIteration, "1 item",
      () => oneItemHashSet(), () => oneItemEnumSet())
    testCreateAndContainsOperation(valuesPerIteration, "3 items",
      () => threeItemsHashSet(), () => threeItemsEnumSet())
    testCreateAndContainsOperation(valuesPerIteration, "5 items",
      () => fiveItemsHashSet(), () => fiveItemsEnumSet())
    testCreateAndContainsOperation(valuesPerIteration, s"${TableCapability.values().length} items",
      () => allItemsHashSet(), () => allItemsEnumSet())
  }
}
