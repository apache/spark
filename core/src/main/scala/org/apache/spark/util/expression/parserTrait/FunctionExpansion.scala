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
import org.apache.spark.util.expression.parserTrait.Utils.createListRegexp

/**
 * Expands out user provided no-arg functions to their values
 */
private[spark] trait FunctionExpansion extends BaseParser {
  /**
   * Dictionary of functions - host class must provide a separate stacked track for those
   * that extends FunctionExpansion (eg JVMInfoFunctions and MachineInfoFunctions)
   * Symbols are matched case-insensitive. Longer symbols are matched in preference of longer ones
   */
  protected def functions: Map[String, ()=>Double] = Map()

  private def keyMatcher: Parser[String] = createListRegexp(functions.keys)

  private def functionExpansion: Parser[Double] = keyMatcher ^^ {
    case dictKeysRegex => functions(dictKeysRegex.toLowerCase)()
  }

  protected abstract override def stackedExtensions: Parser[Double] = functionExpansion |
    super.stackedExtensions
}

