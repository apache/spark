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

package org.apache.spark.sql.catalyst.expressions

/**
 * Utilities to make sure we pass the proper numeric ranges
 */
object IntegralLiteralTestUtils {

  val positiveShort: Short = (Byte.MaxValue + 1).toShort
  val negativeShort: Short = (Byte.MinValue - 1).toShort

  val positiveShortLit: Literal = Literal(positiveShort)
  val negativeShortLit: Literal = Literal(negativeShort)

  val positiveInt: Int = Short.MaxValue + 1
  val negativeInt: Int = Short.MinValue - 1

  val positiveIntLit: Literal = Literal(positiveInt)
  val negativeIntLit: Literal = Literal(negativeInt)

  val positiveLong: Long = Int.MaxValue + 1L
  val negativeLong: Long = Int.MinValue - 1L

  val positiveLongLit: Literal = Literal(positiveLong)
  val negativeLongLit: Literal = Literal(negativeLong)
}
