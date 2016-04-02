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

package org.apache.spark.sql.execution.datasources.csv

import org.apache.spark.SparkFunSuite

/**
 * test cases for StringIteratorReader
 */
class CSVParserSuite extends SparkFunSuite {

  private def readAll(iter: Iterator[String]) = {
    val reader = new StringIteratorReader(iter)
    var c: Int = -1
    val read = new scala.collection.mutable.StringBuilder()
    do {
      c = reader.read()
      read.append(c.toChar)
    } while (c != -1)

    read.dropRight(1).toString
  }

  private def readBufAll(iter: Iterator[String], bufSize: Int) = {
    val reader = new StringIteratorReader(iter)
    val cbuf = new Array[Char](bufSize)
    val read = new scala.collection.mutable.StringBuilder()

    var done = false
    do { // read all input one cbuf at a time
    var numRead = 0
      var n = 0
      do { // try to fill cbuf
        var off = 0
        var len = cbuf.length
        n = reader.read(cbuf, off, len)

        if (n != -1) {
          off += n
          len -= n
        }

        assert(len >= 0 && len <= cbuf.length)
        assert(off >= 0 && off <= cbuf.length)
        read.appendAll(cbuf.take(n))
      } while (n > 0)
      if(n != -1) {
        numRead += n
      } else {
        done = true
      }
    } while (!done)

    read.toString
  }

  test("Hygiene") {
    val reader = new StringIteratorReader(List("").toIterator)
    assert(reader.ready === true)
    assert(reader.markSupported === false)
    intercept[IllegalArgumentException] { reader.skip(1) }
    intercept[IllegalArgumentException] { reader.mark(1) }
    intercept[IllegalArgumentException] { reader.reset() }
  }

  test("Regular case") {
    val input = List("This is a string", "This is another string", "Small", "", "\"quoted\"")
    val read = readAll(input.toIterator)
    assert(read === input.mkString("\n") ++ "\n")
  }

  test("Empty iter") {
    val input = List[String]()
    val read = readAll(input.toIterator)
    assert(read === "")
  }

  test("Embedded new line") {
    val input = List("This is a string", "This is another string", "Small\n", "", "\"quoted\"")
    val read = readAll(input.toIterator)
    assert(read === input.mkString("\n") ++ "\n")
  }

  test("Buffer Regular case") {
    val input = List("This is a string", "This is another string", "Small", "", "\"quoted\"")
    val output = input.mkString("\n") ++ "\n"
    for(i <- 1 to output.length + 5) {
      val read = readBufAll(input.toIterator, i)
      assert(read === output)
    }
  }

  test("Buffer Empty iter") {
    val input = List[String]()
    val output = ""
    for(i <- 1 to output.length + 5) {
      val read = readBufAll(input.toIterator, 1)
      assert(read === "")
    }
  }

  test("Buffer Embedded new line") {
    val input = List("This is a string", "This is another string", "Small\n", "", "\"quoted\"")
    val output = input.mkString("\n") ++ "\n"
    for(i <- 1 to output.length + 5) {
      val read = readBufAll(input.toIterator, 1)
      assert(read === output)
    }
  }
}
