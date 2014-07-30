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

package org.apache.spark.sql.csv

import org.scalatest.FunSuite

class CsvTokenizerSuite extends FunSuite {

  def runTokenizer(
      tokenLines: List[List[String]],
      delimiter: String = ",",
      quote: Char = '"'): Iterator[List[String]] = {
    val linesIter = tokenLines.map(_.mkString(delimiter))
    new CsvTokenizer(linesIter.iterator, delimiter, quote).map(_.toList)
  }

  test("Tokenize simple delimited fields") {
    val tokens = List("V1", "V2", "V3", "V4", "V5")
    val line = tokens.mkString("; ")
    val tokenizer = new CsvTokenizer(Seq(line).iterator, delimiter = "; ", quote = '"')

    assert(tokenizer.next().toList === tokens)
  }

  test("Tokenize many lines of delimited fields") {
    val seed = List("AAA", "BBB", "CCC", "DDD")
    val tokenLines = (1 to 10).toList.map { lineNumber =>
      seed.map(_ * lineNumber + s"-$lineNumber")
    }
    assert(runTokenizer(tokenLines).toList === tokenLines)
  }

  test("Tokenize many lines of delimited fields containing quotes") {
    val seed = List("V1", "V2", "V3", "V4", "V5")
    val tokenLines = (1 to 10).toList.map { lineNumber =>
      seed.map(_ + s"-$lineNumber")
    }

    val quotedTokenLines = tokenLines.map { tokens =>
      tokens.map("\"" + _ + "\"")
    }

    assert(runTokenizer(quotedTokenLines).toList === tokenLines)
  }

  test("Tokenize many lines of delimited fields containing double quotes inside quotes") {
    val seed = List("V1", "V2", "V3", "V4", "V5")
    val tokenLines = (1 to 10).toList.map { lineNumber =>
      seed.map(_ + s""" ""quoted stuff""-$lineNumber""")
    }

    val parsedTokenLines = (1 to 10).toList.map { lineNumber =>
      seed.map(_ + s""" "quoted stuff"-$lineNumber""")
    }

    val quotedTokenLines = tokenLines.map { tokens =>
      tokens.map("\"" + _ + "\"")
    }

    assert(runTokenizer(quotedTokenLines).toList === parsedTokenLines)
  }

  test("Tokenize delimited fields containing new lines inside quotes") {

    val headerLine = "Make; Model; Year; Note"
    val firstLine = """Honda; Civic; 2006; "Reliable"""
    val secondLine = """ car""""
    val thirdLine = """Toyota; Camry; 2006; "Best"""
    val fourthLine = "selling"
    val fifthLine = """car""""

    val tokenLines = List(headerLine, firstLine, secondLine, thirdLine, fourthLine, fifthLine)
    val tokenizer = new CsvTokenizer(tokenLines.iterator, delimiter = "; ", quote = '"')

    val firstRow = tokenizer.next()
    val secondRow = tokenizer.next()
    val thirdRow = tokenizer.next()

    assert(firstRow.toSeq === Seq("Make", "Model", "Year", "Note"))
    assert(secondRow(3) === """Reliable
        | car""".stripMargin)
    assert(thirdRow(3) === """Best
        |selling
        |car""".stripMargin)


  }

}
