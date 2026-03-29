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

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ArrayImplicits._

/**
 * A row implementation that uses an array of objects as the underlying storage. Note that, while
 * the array is not copied, and thus could technically be mutated after creation, this is not
 * allowed.
 */
class GenericRow(protected[sql] val values: Array[Any]) extends Row {

  /** No-arg constructor for serialization. */
  protected def this() = this(null)

  def this(size: Int) = this(new Array[Any](size))

  override def length: Int = values.length

  override def get(i: Int): Any = values(i)

  override def toSeq: Seq[Any] = values.clone().toImmutableArraySeq

  override def copy(): GenericRow = this
}

class GenericRowWithSchema(values: Array[Any], override val schema: StructType)
    extends GenericRow(values) {

  /** No-arg constructor for serialization. */
  protected def this() = this(null, null)

  override def fieldIndex(name: String): Int = schema.fieldIndex(name)
}
