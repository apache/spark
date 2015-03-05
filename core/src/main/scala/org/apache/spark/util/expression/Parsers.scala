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

import org.apache.spark.util.expression.parserTrait._

import scala.collection.SortedMap

/**
 * Factory method to produce various types of parsers
 */
object Parsers {
  /**
   * Basic parser that will evaluate expressions of integers and floats
   * Supports basic arithmetic (+-/star) operations with precedence and brackets,
   * The following JVMInfoFunctions functions are also supported (case insensitive)
   *
   * numCores:          Number of cores assigned to the JVM
   * totalMemoryBytes:  current bytes of memory allocated to the JVM
   * maxMemoryBytes:    Maximum number of bytes of memory available to the JVM
   * freeMemoryBytes:   maxMemoryBytes - totalMemoryBytes
   */
  def NumberParser = new BaseParser with JVMInfoFunctions

  /**
   * A NumberParser that also supports expanding a caller-supplied dictionary of symbols
   * to their associated values (eg, a list of configuration variables)
   * @param dict Map of symbol names to expand to their associated values
   */
  def NumberDictParser(dict: SortedMap[String, Long]) = new BaseDictParser(dict) with JVMInfoFunctions

  /**
   * A Parser that will evaluate expressions of integers, floats and byte quantities
   * Supports basic arithmetic (+-/star) operations with precedence and brackets,
   * The ByteParser parser is used for parsing expressions of byte quantities eg:
   *
   * 3 MB
   * totalMemoryBytes / 5
   * (freeMemoryBytes - 50000) * 0.5
   * numCores * 20 MB
   *
   * Supports basic arithmetic (+-/star) operations with precedence and brackets,
   * all bytes units (case insensitive) (KB,MB,GB,TB,KiB,MiB,GiB,TiB etc) are expanded into
   * their equivalent number of bytes
   * The following JVMInfoFunctions functions are also supported (case insensitive)
   *
   * numCores:          Number of cores assigned to the JVM
   * totalMemoryBytes:  current bytes of memory allocated to the JVM
   * maxMemoryBytes:    Maximum number of bytes of memory available to the JVM
   * freeMemoryBytes:   maxMemoryBytes - totalMemoryBytes
   *
   */
  def ByteParser = new BaseParser with ByteUnitParsing with JVMInfoFunctions

  /**
   * A ByteParser that also supports expanding a caller-supplied dictionary of symbols
   * to their associated values (eg, a list of configuration variables)
   * @param dict Map of symbol names to expand to their associated values
   */
  def ByteDictParser(dict: SortedMap[String, Long]) = new BaseDictParser(dict)
    with ByteUnitParsing with JVMInfoFunctions with DictionaryExpansion

  /**
   * A Parser that will evaluate expressions of integers, floats and time periods
   * Numbers without any associated time units are assumed to be Milliseconds
   * Supports basic arithmetic (+-/star) operations with precedence and brackets,
   * The TimeAsMSParser is used for parsing expressions of time periods eg:
   * 300 ms
   * 5 seconds
   * 0.5 days
   */
  def TimeAsMSParser = new BaseParser with TimeUnitMSParsing

  /**
   * A TimeAsMSParser that also supports expanding a caller-supplied dictionary of symbols
   * to their associated values (eg, a list of configuration variables)
   * @param dict Map of symbol names to expand to their associated values
   */
  def TimeAsMSDictParser(dict: SortedMap[String, Long]) = new BaseDictParser(dict) with TimeUnitMSParsing

  /**
   * A Time Parser that assumes that Numbers without any associated time units are Seconds
   */
  def TimeAsSecParser = new BaseParser with TimeUnitSecParsing

  /**
   * A TimeAsSecParser that also supports expanding a caller-supplied dictionary of symbols
   * to their associated values (eg, a list of configuration variables)
   * @param dict Map of symbol names to expand to their associated values
   */
  def TimeAsSecDictParser(dict: SortedMap[String, Long]) = new BaseDictParser(dict) with TimeUnitSecParsing
}
