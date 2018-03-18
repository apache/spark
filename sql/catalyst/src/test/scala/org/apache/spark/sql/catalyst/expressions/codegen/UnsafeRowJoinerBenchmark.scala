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
package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.sql.{RandomDataGenerator, Row}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.types._
import org.apache.spark.util.Benchmark

/**
 * Benchmarks for [[UnsafeRowJoiner]] implementations.
 */
object UnsafeRowJoinerBenchmark {

  private def generateRows(schema: StructType, numRows: Int): Array[UnsafeRow] = {
    val generator = RandomDataGenerator.forType(schema, nullable = false).get
    val encoder = RowEncoder(schema)
    Array.fill(numRows) {
      encoder.toRow(generator().asInstanceOf[Row]).copy().asInstanceOf[UnsafeRow]
    }
  }

  def main(args: Array[String]): Unit = {
    val numRows = 1024 * 128
    val benchmark = new Benchmark("unsafe row joiner", numRows)
    def run(name: String, schema1: StructType, schema2: StructType, iters: Int): Unit = {
      val rows1 = generateRows(schema1, 1024)
      val rows2 = generateRows(schema2, 1024)
      def func(joiner: UnsafeRowJoiner): (Int => Unit) = { iter =>
        var sum = iter.toLong
        var i = 0
        while (i < numRows) {
          val rowId = i & 1023
          sum += joiner.join(rows1(rowId), rows2(rowId)).hashCode()
          i += 1
        }
        sum
      }
      benchmark.addCase(s"$name - interpreted", iters) {
        func(new InterpretedUnsafeRowJoiner(schema1, schema2))
      }
      benchmark.addCase(s"$name - code generated", iters) {
        func(GenerateUnsafeRowJoiner.create(schema1, schema2))
      }
    }
    def runSingle(name: String, dataType: DataType, n: Int, iters: Int): Unit = {
      val schema = StructType(Seq.tabulate(n)(i => StructField(s"a$i", dataType)))
      run(name, schema, schema, iters)
    }

    /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_92-b14 on Mac OS X 10.12.6
    Intel(R) Core(TM) i7-4980HQ CPU @ 2.80GHz

    unsafe row joiner:                       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    1 int - interpreted                              4 /    4         31.8          31.4       1.0X
    1 int - code generated                           3 /    3         42.5          23.6       1.3X
    64 ints - interpreted                           53 /   55          2.5         402.8       0.1X
    64 ints - code generated                        52 /   55          2.5         393.1       0.1X
    67 ints - interpreted                           55 /   56          2.4         416.7       0.1X
    67 ints - code generated                        53 /   55          2.5         405.5       0.1X
    1 long - interpreted                             4 /    4         33.2          30.1       1.0X
    1 long - code generated                          3 /    4         39.0          25.7       1.2X
    64 longs - interpreted                          53 /   57          2.5         401.8       0.1X
    64 longs - code generated                       51 /   53          2.6         391.6       0.1X
    67 longs - interpreted                          54 /   57          2.4         415.6       0.1X
    67 longs - code generated                       54 /   56          2.4         408.3       0.1X
    1 string - interpreted                         139 /  147          0.9        1061.5       0.0X
    1 string - code generated                      139 /  144          0.9        1059.4       0.0X
    4 string - interpreted                         558 /  566          0.2        4258.8       0.0X
    4 string - code generated                      557 /  570          0.2        4247.3       0.0X
    complex - interpreted                          169 /  175          0.8        1290.4       0.0X
    complex - code generated                       167 /  175          0.8        1275.3       0.0X
     */
    runSingle("1 int", IntegerType, 1, 128)
    runSingle("64 ints", IntegerType, 64, 64)
    runSingle("67 ints", IntegerType, 65, 64)
    runSingle("1 long", LongType, 1, 128)
    runSingle("64 longs", LongType, 64, 64)
    runSingle("67 longs", LongType, 65, 64)
    runSingle("1 string", StringType, 1, 32)
    runSingle("4 string", StringType, 4, 16)
    val schema = new StructType()
      .add("a", IntegerType)
      .add("b", StringType)
      .add("c", LongType, nullable = true)
      .add("d", BooleanType)
      .add("e", new ArrayType(FloatType, false))
      .add("f", new StructType().add("fa", FloatType))
    run("complex", schema, schema, 16)
    benchmark.run()
  }
}
