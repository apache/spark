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

import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.util.JacksonUtils

/**
 * Benchmark for Json4s vs Jackson serialization and deserialization.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to "benchmarks/JsonSerDeBenchmark-results.txt".
 * }}}
 * */
object JsonSerDeBenchmark extends BenchmarkBase {

  private def toJsonWithoutNested(valuesPerIteration: Int): Unit = {
    val benchmark = new Benchmark(
      s"Test to Json with out nested",
      valuesPerIteration,
      output = output)

    benchmark.addCase("Use Json4s") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        jsonString
      }
    }

    benchmark.addCase("Use Scala object") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val map = Map("intValue" -> 1,
          "longValue" -> 2L,
          "doubleValue" -> 3.0D,
          "stringValue" -> "4",
          "floatValue" -> 5.0F,
          "booleanValue" -> true)
        JacksonUtils.writeValueAsString(map)
      }
    }

    benchmark.addCase("Use JsonNode") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val node = JacksonUtils.createObjectNode
        node.put("intValue", 1)
        node.put("longValue", 2L)
        node.put("doubleValue", 3.0D)
        node.put("stringValue", "4")
        node.put("floatValue", 5.0F)
        node.put("booleanValue", true)
        JacksonUtils.writeValueAsString(node)
      }
    }

    benchmark.addCase("Use Json Generator") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        JacksonUtils.toJsonString { g =>
          g.writeStartObject()
          g.writeNumberField("intValue", 1)
          g.writeNumberField("longValue", 2L)
          g.writeNumberField("doubleValue", 3.0D)
          g.writeStringField("stringValue", "4")
          g.writeNumberField("floatValue", 5.0F)
          g.writeBooleanField("booleanValue", true)
          g.writeEndObject()
        }
      }
    }

    benchmark.run()
  }


  private def jsonString = {
    val jValue = ("intValue" -> 1) ~ ("longValue" -> 2L) ~ ("doubleValue" -> 3.0D) ~
      ("stringValue" -> "4") ~ ("floatValue" -> 5.0F) ~ ("booleanValue" -> true)
    compact(render(jValue))
  }

  private def toJsonWithNested(valuesPerIteration: Int): Unit = {
    val benchmark = new Benchmark(
      s"Test to Json with nested",
      valuesPerIteration,
      output = output)

    benchmark.addCase("Use Json4s") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        nestedJsonString
      }
    }

    benchmark.addCase("Use Scala object") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val arrayValue = (0 until 100).toArray
        val mapValue = Map("intValue" -> 1,
          "longValue" -> 2L,
          "doubleValue" -> 3.0D,
          "stringValue" -> "4",
          "floatValue" -> 5.0F,
          "booleanValue" -> true)
        val map = Map("intArray" -> arrayValue,
          "mapValue" -> mapValue)
        JacksonUtils.writeValueAsString(map)
      }
    }

    benchmark.addCase("Use JsonNode") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        import com.fasterxml.jackson.databind.JsonNode
        val factory = JacksonUtils.defaultNodeFactory
        val arrayNode = factory.arrayNode(100)
        (0 until 100).foreach(i => arrayNode.add(factory.numberNode(i)))

        val mapNode = JacksonUtils.createObjectNode
        mapNode.put("intValue", 1)
        mapNode.put("longValue", 2L)
        mapNode.put("doubleValue", 3.0D)
        mapNode.put("stringValue", "4")
        mapNode.put("floatValue", 5.0F)
        mapNode.put("booleanValue", true)

        val obj = factory.objectNode()

        obj.set[JsonNode]("intArray", arrayNode)
        obj.set[JsonNode]("mapValue", mapNode)

        JacksonUtils.writeValueAsString(obj)
      }
    }

    benchmark.addCase("Use Json Generator") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        JacksonUtils.toJsonString { g =>
          g.writeStartObject()

          g.writeArrayFieldStart("intArray")
          (0 until 100).foreach(i => g.writeNumber(i))
          g.writeEndArray()

          g.writeObjectFieldStart("mapValue")
          g.writeNumberField("intValue", 1)
          g.writeNumberField("longValue", 2L)
          g.writeNumberField("doubleValue", 3.0D)
          g.writeStringField("stringValue", "4")
          g.writeNumberField("floatValue", 5.0F)
          g.writeBooleanField("booleanValue", true)
          g.writeEndObject()

          g.writeEndObject()
        }
      }
    }

    benchmark.run()
  }

  private def fromJsonString(json: String,
      name: String , valuesPerIteration: Int): Unit = {
    val benchmark = new Benchmark(
      s"Test from Json $name",
      valuesPerIteration,
      output = output)

    benchmark.addCase("Use Json4s") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        parse(json)
      }
    }

    benchmark.addCase("Use Jackson") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        JacksonUtils.readTree(json)
      }
    }

    benchmark.run()
  }


  private def nestedJsonString = {
    val mapValue = ("intValue" -> 1) ~ ("longValue" -> 2L) ~ ("doubleValue" -> 3.0D) ~
      ("stringValue" -> "4") ~ ("floatValue" -> 5.0F) ~ ("booleanValue" -> true)

    val jValue =
      ("intArray" -> (0 until 100)) ~ ("mapValue" -> mapValue)

    compact(render(jValue))
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val valuesPerIteration = 100000
    toJsonWithoutNested(valuesPerIteration)
    toJsonWithNested(valuesPerIteration)

    fromJsonString(jsonString, "without nested", valuesPerIteration)
    fromJsonString(nestedJsonString, "with nested", valuesPerIteration)
  }
}
