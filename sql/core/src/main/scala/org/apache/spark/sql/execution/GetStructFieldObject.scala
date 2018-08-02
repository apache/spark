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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.{Expression, GetStructField}
import org.apache.spark.sql.types.StructField

/**
 * A Scala extractor that extracts the child expression and struct field from a [[GetStructField]].
 * This is in contrast to the [[GetStructField]] case class extractor which returns the field
 * ordinal instead of the field itself.
 */
private[execution] object GetStructFieldObject {
  def unapply(getStructField: GetStructField): Option[(Expression, StructField)] =
    Some((
      getStructField.child,
      getStructField.childSchema(getStructField.ordinal)))
}
