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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.V2ViewDescription
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.LeafExecNode

/**
 * Physical plan node for showing view properties.
 */
case class ShowViewPropertiesExec(
    output: Seq[Attribute],
    desc: V2ViewDescription,
    propertyKey: Option[String]) extends V2CommandExec with LeafExecNode {

  override protected def run(): Seq[InternalRow] = {
    propertyKey match {
      case Some(p) =>
        val propValue = desc.properties.getOrElse(p,
          s"View ${desc.identifier} does not have property: $p")
        Seq(toCatalystRow(p, propValue))
      case None =>
        desc.properties.map {
          case (k, v) => toCatalystRow(k, v)
        }.toSeq
    }
  }
}
