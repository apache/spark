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

/**
 * Expression parser used for parsing expressions of byte quantities
 *
 * 3 MB
 * totalMemoryBytes / 5
 * (freeMemoryBytes - 50000) * 0.5
 * numCores * 20 MB
 *
 * Supports basic arithmetic (+-/star) operations with precedence and brackets,
 * all bytes units (case insensitive) (KB,MB,GB,TB,KiB,MiB,GiB,TiB etc)
 * as well as the following special operators (case insensitive)
 *
 * numCores:          Number of cores assigned to the JVM
 * totalMemoryBytes:  current bytes of memory allocated to the JVM
 * maxMemoryBytes:    Maximum number of bytes of memory available to the JVM
 * freeMemoryBytes:   maxMemoryBytes - totalMemoryBytes
 *
 */
class ByteExpressionParser extends ExpressionParser[ByteQuantity] {

  /**
   * Those expression that are unique to a ByteExpression
   */
  def extensions = externInfo | byteExpression

  /**
   * Provides for functions that query the underlying JVM for system statistics
   */
  def externInfo = """(?i)numCores""".r ^^ (x=>Runtime.getRuntime.availableProcessors() * 1.0) |
    """(?i)totalMemoryBytes""".r ^^ (x=>Runtime.getRuntime.totalMemory() * 1.0) |
    """(?i)maxMemoryBytes""".r ^^ (x=>Runtime.getRuntime.maxMemory() * 1.0) |
    """(?i)freeMemoryBytes""".r ^^ (x=>Runtime.getRuntime.freeMemory() * 1.0)

  /**
   * An expression of byte quantity eg 30 MB, 4 KiB etc
   * returns number of bytes
   */
  def byteExpression = decimalNumber~byteUnit ^^ {
    case decimalNumber~byteUnit => ByteQuantity(decimalNumber.toDouble, byteUnit.toUpperCase)
      .toBytes
  }

  def standAloneByteUnit: Parser[Double] = byteUnit ^^ {
    case byteUnit => ByteQuantity(1.0,byteUnit.toUpperCase).toBytes
  }

  def byteUnit: Parser[String] = """((?i)[KMGTPEZY]?i?B)""".r

  def parse(expression: String): Option[ByteQuantity] = {
    parseAll(p = expr, in = expression) match {
      case Success(x,_) => Option(ByteQuantity(x))
      case _ => None
    }
  }

}