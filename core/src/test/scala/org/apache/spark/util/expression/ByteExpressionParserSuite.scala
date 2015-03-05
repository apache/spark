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
package org.apache.spark.util.expression

import org.apache.spark.util.expression.parserTrait.ByteUnitParsing
import org.apache.spark.util.expression.quantity.ByteQuantity
import org.scalatest.FunSuite


class ByteExpressionParserSuite extends FunSuite {
  val parser = new ByteParser()

  def testParser(in: String, expectedResult:ByteQuantity): Unit = {
    val parseResult = parser.parse(in)
    assert(parseResult.map(ByteQuantity(_)) == Some(expectedResult))
  }

  test("simple bytes") {
    testParser(in = "500", expectedResult = ByteQuantity(500))
  }

  test("kbytes") {
    testParser(in = "5 KB", expectedResult = ByteQuantity(5 * 1000))
  }

  test("jvm kbytes") {
    testParser(in = "5 K", expectedResult = ByteQuantity(5 * 1000))
  }

  test("kbytes2") {
    testParser(in = "5KB", expectedResult =  ByteQuantity(5 * 1000))
  }

  test("mbytes") {
    testParser(in = "5 MB", expectedResult =  ByteQuantity(5 * Math.pow(1000,2)))
  }

  test("jvm mbytes") {
    testParser(in = "5 M", expectedResult =  ByteQuantity(5 * Math.pow(1000,2)))
  }

  test("gbytes") {
    testParser(in = "5 GB", expectedResult =  ByteQuantity(5 * Math.pow(1000,3)))
  }

  test("jvm gbytes") {
    testParser(in = "5 G", expectedResult =  ByteQuantity(5 * Math.pow(1000,3)))
  }

  test("tbytes") {
    testParser(in = "5 tB", expectedResult =  ByteQuantity(5 * Math.pow(1000,4)))
  }

  test("jvm tbytes") {
    testParser(in = "5 t", expectedResult =  ByteQuantity(5 * Math.pow(1000,4)))
  }

  test("pbytes") {
    testParser(in = "5 pb", expectedResult =  ByteQuantity(5 * Math.pow(1000,5)))
  }

  test("ebytes") {
    testParser(in = "5 eb", expectedResult =  ByteQuantity(5 * Math.pow(1000,6)))
  }

  test("kibytes") {
    testParser(in = "5 KiB", expectedResult = ByteQuantity(5 * 1024))
  }

  test("mibytes") {
    testParser(in = "5 MiB", expectedResult = ByteQuantity(5 * Math.pow(1024,2)))
  }

  test("gibytes") {
    testParser(in = "5 GiB", expectedResult = ByteQuantity(5 * Math.pow(1024,3)))
  }

  test("tibytes") {
    testParser(in = "5 TiB", expectedResult = ByteQuantity(5 * Math.pow(1024,4)))
  }

  test("pibytes") {
    testParser(in = "5 PiB", expectedResult = ByteQuantity(5 * Math.pow(1024,5)))
  }

  test("eibytes") {
    testParser(in = "5 EiB", expectedResult = ByteQuantity(5 * Math.pow(1024,6)))
  }

  test("kbytes arithmetic") {
    testParser(in = "5 KB * 5", expectedResult = ByteQuantity(5 * 5 * 1000))
  }

  test("kbytes arithmetic2") {
    testParser(in = "5 * 5 KB ", expectedResult = ByteQuantity(5 * 5 * 1000))
  }

  test("kbytes divide") {
    testParser(in = "10 kb / 2 ", expectedResult = ByteQuantity( 5 * 1000))
  }

  test("kbytes divide2") {
    testParser(in = "10 kb * 0.5 ", expectedResult = ByteQuantity( 5 * 1000))
  }

  test("kbytes addition") {
    testParser(in = "10 kb + 5 MB ", expectedResult = ByteQuantity(5 * Math.pow(1000,2) + 10000))
  }

  test("test totalMemoryBytes arith") {
    testParser(in = "maxMemoryBytes*10", expectedResult = ByteQuantity(Runtime.getRuntime.maxMemory*10.0))
  }

  test("test totalMemoryBytes arith2") {
    testParser(in = "maxMemoryBytes/1", expectedResult = ByteQuantity(Runtime.getRuntime.maxMemory))
  }

  // The following tests are not strict as they involve them querying dynamic system state,
  // and thus there is no reliable means to verify that the returned values are correct
  test("test totalMemoryBytes") {
    val parseResult = parser.parse("totalMemoryBytes")
    assert( parseResult.isDefined )
    assert( parseResult.get > 0 )
  }

  test("test totalFreeBytes") {
    val parseResult = parser.parse("freeMemoryBytes")
    assert( parseResult.isDefined )
    assert( parseResult.get > 0 )
  }

  test("test maxMemoryBytes") {
    val parseResult = parser.parse("maxMemoryBytes")
    assert( parseResult.isDefined )
    assert( parseResult.get > 0 )
  }

}
