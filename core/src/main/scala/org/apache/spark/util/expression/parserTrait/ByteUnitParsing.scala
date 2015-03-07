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
package org.apache.spark.util.expression.parserTrait

import org.apache.spark.util.expression.BaseParser
import org.apache.spark.util.expression.quantity.ByteQuantity

import scala.util.parsing.combinator.JavaTokenParsers

/**
 * A Trait that will match byte quantities and expand them into their equivalent number of bytes
 */
private[spark] trait ByteUnitParsing extends BaseParser {
  /**
   * Those expression that are unique to a ByteExpression
   */
  protected abstract override def stackedExtensions = byteExpression | standAloneByteUnit |
    super.stackedExtensions

  /**
   * An expression of byte quantity eg 30 MB, 4 KiB etc
   * returns number of bytes
   */
  private def byteExpression: Parser[Double] = decimalNumber~byteUnit ^^ {
    case decimalNumber~byteUnit => ByteQuantity(decimalNumber.toDouble, byteUnit).toBytes
  }

  /**
   * A byte quantity (eg 'MB') if not parsed as anything else is considered
   * a single unit of the specified quantity
   */
  private def standAloneByteUnit: Parser[Double] = byteUnit ^^ {
    case byteUnit => ByteQuantity(1.0,byteUnit).toBytes
  }

  private def byteUnit: Parser[String] = """^(?i)([KMGTPEZY]?i?B|[KMGT])""".r
}
