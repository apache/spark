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

package org.apache.spark.sql.hive.thriftserver.cli.operation

import java.util

import org.apache.hadoop.hive.metastore.TableType

class ClassicTableTypeMapping extends TableTypeMapping {

  object ClassicTableTypes extends Enumeration {
    type ClassicTableTypes = Value
    val TABLE, VIEW = Value
  }

  private val hiveToClientMap: util.Map[String, String] = new util.HashMap[String, String]
  private val clientToHiveMap: util.Map[String, String] = new util.HashMap[String, String]

  try {
    hiveToClientMap.put(TableType.MANAGED_TABLE.toString, ClassicTableTypes.TABLE.toString)
    hiveToClientMap.put(TableType.EXTERNAL_TABLE.toString, ClassicTableTypes.TABLE.toString)
    hiveToClientMap.put(TableType.VIRTUAL_VIEW.toString, ClassicTableTypes.VIEW.toString)
    clientToHiveMap.put(ClassicTableTypes.TABLE.toString, TableType.MANAGED_TABLE.toString)
    clientToHiveMap.put(ClassicTableTypes.VIEW.toString, TableType.VIRTUAL_VIEW.toString)
  } catch {
    case e: Throwable => e.printStackTrace()
  }

  override def mapToHiveType(clientTypeName: String): String =
    if (clientToHiveMap.containsKey(clientTypeName)) {
      clientToHiveMap.get(clientTypeName)
    } else {
      clientTypeName
    }

  override def mapToClientType(hiveTypeName: String): String =
    if (hiveToClientMap.containsKey(hiveTypeName)) {
      hiveToClientMap.get(hiveTypeName)
    } else {
      hiveTypeName
    }

  override def getTableTypeNames: Set[String] = {
    ClassicTableTypes.values.map(_.toString)
  }
}
