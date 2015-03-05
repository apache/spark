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

import org.apache.spark.util.expression.parserTrait.DictionaryExpansion

import scala.collection.SortedMap

/**
 * An extension of BaseParser that also expands out a user supplied dictionary of symbols
 * @param dict Symbols to expand into their numeric representation
 */
class BaseDictParser(var dict: SortedMap[String, Long]) extends BaseParser with DictionaryExpansion
