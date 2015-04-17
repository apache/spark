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
 
package org.apache.spark.sql.hive

import org.apache.hadoop.hive.ql.exec.Description
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.spark.sql.types._

// deterministic in the query range
@Description(name = "current_database",
    value = "_FUNC_() - returns currently using database name")
class sqlUDFCurrentDB extends GenericUDF {

  override def initialize(arguments: Array[ObjectInspector]): ObjectInspector = {
    val database = SessionState.get.getCurrentDatabase
    HiveShim.getStringWritableConstantObjectInspector(UTF8String(database))
  }

  override def evaluate(arguments: Array[GenericUDF.DeferredObject]): Object = {
    SessionState.get.getCurrentDatabase
  }

  override def getDisplayString(children: Array[String]): String = {
    return "current_database()"
  }
}
