/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver.cli

import org.apache.spark.service.cli.thrift.TColumnDesc
import org.apache.spark.sql.types.StructField

/**
 * A wrapper class for Spark's [[StructField]] with a column position,
 * and can be transform to [[TColumnDesc]]
 */
case class ColumnDescriptor(field: StructField, pos: Int) {
  /**
   * Transform a [[ColumnDescriptor]] to a [[TColumnDesc]] instance.
   */
  def toTColumnDesc: TColumnDesc = {
    val tColumnDesc = new TColumnDesc
    if (field != null) {
      tColumnDesc.setColumnName(field.name)
      tColumnDesc.setComment(field.getComment().getOrElse(""))
      tColumnDesc.setTypeDesc(TypeDescriptor(field.dataType).toTTypeDesc)
    }
    tColumnDesc.setPosition(pos)
    tColumnDesc
  }
}
