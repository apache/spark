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

package org.apache.spark.sql.catalog.v2.utils

import org.apache.spark.sql.catalog.v2.TableChange
import org.apache.spark.sql.catalyst.plans.logical.sql.QualifiedColType
import org.apache.spark.sql.types.DataType

trait TableChangeHelper {

  protected def addColumns(cols: Seq[QualifiedColType]): Seq[TableChange] = cols.map { col =>
    TableChange.addColumn(col.name.toArray, col.dataType, true, col.comment.orNull)
  }

  protected def alterColumn(
      colName: Seq[String],
      dataType: Option[DataType],
      comment: Option[String]): Seq[TableChange] = {
    val typeChange = dataType.map { newDataType =>
      TableChange.updateColumnType(colName.toArray, newDataType, true)
    }
    val commentChange = comment.map { newComment =>
      TableChange.updateColumnComment(colName.toArray, newComment)
    }
    typeChange.toSeq ++ commentChange.toSeq
  }

  protected def renameColumn(col: Seq[String], newName: String): Seq[TableChange] = {
    Seq(TableChange.renameColumn(col.toArray, newName))
  }

  protected def deleteColumns(cols: Seq[Seq[String]]): Seq[TableChange] = {
    cols.map(col => TableChange.deleteColumn(col.toArray))
  }

  protected def setProperties(props: Map[String, String]): Seq[TableChange] = props.map {
    case (key, value) => TableChange.setProperty(key, value)
  }.toSeq

  protected def removeProperties(keys: Seq[String]): Seq[TableChange] = {
    keys.map(key => TableChange.removeProperty(key))
  }

  protected def setLocation(newLocation: String): Seq[TableChange] = {
    Seq(TableChange.setProperty("location", newLocation))
  }
}
