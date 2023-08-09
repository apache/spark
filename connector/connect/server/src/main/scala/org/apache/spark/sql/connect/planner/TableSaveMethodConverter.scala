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
package org.apache.spark.sql.connect.planner

import java.util.Locale

import org.apache.spark.connect.proto

/**
 * Helper class for conversions between save table method string and
 * [[proto.WriteOperation.SaveTable.TableSaveMethod]].
 */
object TableSaveMethodConverter {
  def toTableSaveMethodProto(method: String): proto.WriteOperation.SaveTable.TableSaveMethod = {
    method.toLowerCase(Locale.ROOT) match {
      case "save_as_table" =>
        proto.WriteOperation.SaveTable.TableSaveMethod.TABLE_SAVE_METHOD_SAVE_AS_TABLE
      case "insert_into" =>
        proto.WriteOperation.SaveTable.TableSaveMethod.TABLE_SAVE_METHOD_INSERT_INTO
      case _ =>
        throw new IllegalArgumentException(
          "Cannot convert from TableSaveMethod to WriteOperation.SaveTable.TableSaveMethod: " +
            s"${method}")
    }
  }
}
