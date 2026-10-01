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

package org.apache.spark.sql.execution.columnar

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSeq}

/**
 * Resolves where the columns a scan requests sit within a cached batch's schema. Every
 * `CachedBatchSerializer` needs this mapping to project the requested columns out of a cached
 * batch, so it lives here rather than in each serializer.
 */
private[columnar] object CachedColumnIndices {

  /**
   * Returns, for each attribute in `selectedAttributes`, its ordinal in `cacheAttributes`, or -1
   * if the attribute is not part of the cached schema.
   */
  def apply(cacheAttributes: Seq[Attribute], selectedAttributes: Seq[Attribute]): Array[Int] = {
    // The explicit AttributeSeq wrapper is required. On a bare Seq[Attribute] the inherited
    // Seq.indexOf[B >: Attribute](elem: B) wins overload resolution with B inferred as Any, so
    // the implicit conversion never fires and every column silently resolves to -1.
    val cacheAttributeSeq = AttributeSeq(cacheAttributes)
    selectedAttributes.map(a => cacheAttributeSeq.indexOf(a.exprId)).toArray
  }
}
