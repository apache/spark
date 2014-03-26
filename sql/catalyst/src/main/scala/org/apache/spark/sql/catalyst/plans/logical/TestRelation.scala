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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.expressions.Attribute

object LocalRelation {
  def apply(output: Attribute*) =
    new LocalRelation(output)
}

case class LocalRelation(output: Seq[Attribute], data: Seq[Product] = Nil)
  extends LeafNode with analysis.MultiInstanceRelation {

  // TODO: Validate schema compliance.
  def loadData(newData: Seq[Product]) = new LocalRelation(output, data ++ newData)

  /**
   * Returns an identical copy of this relation with new exprIds for all attributes.  Different
   * attributes are required when a relation is going to be included multiple times in the same
   * query.
   */
  override final def newInstance: this.type = {
    LocalRelation(output.map(_.newInstance), data).asInstanceOf[this.type]
  }

  override protected def stringArgs = Iterator(output)
}
