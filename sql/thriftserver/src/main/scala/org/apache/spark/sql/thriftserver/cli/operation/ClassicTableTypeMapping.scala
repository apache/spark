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

package org.apache.spark.sql.thriftserver.cli.operation

import java.util.Locale

import com.google.common.collect.{ArrayListMultimap, Iterables, Multimap}
import java.util
import org.apache.hadoop.hive.metastore.TableType

import org.apache.spark.internal.Logging

class ClassicTableTypeMapping extends TableTypeMapping with Logging {

  object ClassicTableTypes extends Enumeration {
    type ClassicTableTypes = Value
    val TABLE, VIEW, MATERIALIZED_VIEW = Value
  }

  private val hiveToClientMap = new util.HashMap[String, String]
  private val clientToHiveMap: ArrayListMultimap[String, String] = ArrayListMultimap.create()

  try {
    hiveToClientMap.put(HiveTableTypes.MANAGED_TABLE.toString, ClassicTableTypes.TABLE.toString)
    hiveToClientMap.put(HiveTableTypes.EXTERNAL_TABLE.toString, ClassicTableTypes.TABLE.toString)
    hiveToClientMap.put(HiveTableTypes.VIRTUAL_VIEW.toString, ClassicTableTypes.VIEW.toString)
    hiveToClientMap.put(HiveTableTypes.MATERIALIZED_VIEW.toString,
      ClassicTableTypes.MATERIALIZED_VIEW.toString)

    clientToHiveMap.putAll(ClassicTableTypes.TABLE.toString,
      util.Arrays.asList(TableType.MANAGED_TABLE.toString,
        HiveTableTypes.EXTERNAL_TABLE.toString))
    clientToHiveMap.put(ClassicTableTypes.VIEW.toString, HiveTableTypes.VIRTUAL_VIEW.toString)
    clientToHiveMap.put(ClassicTableTypes.MATERIALIZED_VIEW.toString,
      HiveTableTypes.MATERIALIZED_VIEW.toString)
  } catch {
    case e: Throwable => e.printStackTrace()
  }

  override def mapToHiveType(clientTypeName: String): Array[String] = {
    val hiveTableType: util.Collection[String] =
      clientToHiveMap.get(clientTypeName.toUpperCase(Locale.ROOT))
    if (hiveTableType == null) {
      logWarning("Not supported client table type " + clientTypeName)
      return Array[String](clientTypeName)
    }
    return Iterables.toArray(hiveTableType, classOf[String])
  }

  override def mapToClientType(hiveTypeName: String): String =
    if (hiveToClientMap.containsKey(hiveTypeName)) {
      hiveToClientMap.get(hiveTypeName)
    } else {
      logWarning("Invalid hive table type " + hiveTypeName)
      hiveTypeName
    }

  override def getTableTypeNames: Set[String] = {
    ClassicTableTypes.values.map(_.toString)
  }
}
