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

package org.apache.spark.sql

import java.math.BigInteger
import java.security.SecureRandom
import java.util.Random

import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Benchmark

object UTF8StringBenchmark {
  def test(name: String, numRows: Int, iters: Int): Unit = {
    val rand = new Random
    val validInputs =
      (1 to numRows).map(_ => UTF8String.fromString(String.valueOf(rand.nextInt()))).toArray

    val secureRandom = new SecureRandom()
    val invalidInputs = (1 to numRows).map(_ =>
      UTF8String.fromString(String.valueOf(new BigInteger(130, secureRandom).toString(32)))).toArray

    val benchmark = new Benchmark(name, iters * numRows)
    benchmark.addCase("without check over valid integers") { _: Int =>
      var sum = 0
      for (_ <- 0L until iters) {
        var i = 0
        while (i < numRows) {
          try {
            sum += validInputs(i).toInt
          } catch {
            case _: NumberFormatException => null
          }
          i += 1
        }
      }
    }

    benchmark.addCase("with check over valid integers") { _: Int =>
      var sum = 0
      for (_ <- 0L until iters) {
        var i = 0
        while (i < numRows) {
          try {
            val input = validInputs(i)
            if (input.isIntMaybe) {
              sum += input.toInt
            }
          } catch {
            case _: NumberFormatException => null
          }
          i += 1
        }
      }
    }


    benchmark.addCase("without check over in-valid integers") { _: Int =>
      var sum = 0
      for (_ <- 0L until iters) {
        var i = 0
        while (i < numRows) {
          try {
            sum += invalidInputs(i).toInt
          } catch {
            case _: NumberFormatException => null
          }
          i += 1
        }
      }
    }

    benchmark.addCase("with check over in-valid integers") { _: Int =>
      var sum = 0
      for (_ <- 0L until iters) {
        var i = 0
        while (i < numRows) {
          try {
            val input = invalidInputs(i)
            if (input.isIntMaybe) {
              sum += input.toInt
            }
          } catch {
            case _: NumberFormatException => null
          }
          i += 1
        }
      }
    }

    benchmark.run()
  }

  def main(args: Array[String]): Unit = {
    /*
    Intel(R) Core(TM) i7-6920HQ CPU @ 2.90GHz

    conversion to int:                      Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    without check over valid integers              500 /  503         33.6          29.8       1.0X
    with check over valid integers                1069 / 1072         15.7          63.7       0.5X
    without check over in-valid integers        34515 / 36096          0.5        2057.3       0.0X
    with check over in-valid integers              718 /  895         23.4          42.8       0.7X
    */
    test("conversion to int", 1 << 15, 1 << 9)
  }
}
