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

package org.apache.spark.sql.columnar

import scala.util.Random

import org.apache.spark.sql.catalyst.expressions.GenericMutableRow

// TODO Enrich test data
object ColumnarTestData {
  object GenericMutableRow {
    def apply(values: Any*) = {
      val row = new GenericMutableRow(values.length)
      row.indices.foreach { i =>
        row(i) = values(i)
      }
      row
    }
  }

  def randomBytes(length: Int) = {
    val bytes = new Array[Byte](length)
    Random.nextBytes(bytes)
    bytes
  }

  val nonNullRandomRow = GenericMutableRow(
    Random.nextInt(),
    Random.nextLong(),
    Random.nextFloat(),
    Random.nextDouble(),
    Random.nextBoolean(),
    Random.nextInt(Byte.MaxValue).asInstanceOf[Byte],
    Random.nextInt(Short.MaxValue).asInstanceOf[Short],
    Random.nextString(Random.nextInt(64)),
    randomBytes(Random.nextInt(64)),
    Map(Random.nextInt() -> Random.nextString(4)))

  val nullRow = GenericMutableRow(Seq.fill(10)(null): _*)
}
