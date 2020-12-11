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

package org.apache.spark.sql.execution.streaming.state

import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}

case class StateSchemaNotCompatible(message: String) extends Exception(message)

class StateSchemaCompatibilityChecker(
    providerId: StateStoreProviderId,
    hadoopConf: Configuration) extends Logging {

  private val stateFileManager = new StateSchemaFileManager(providerId.storeId, hadoopConf)

  def check(keySchema: StructType, valueSchema: StructType): Unit = {
    if (stateFileManager.fileExist()) {
      logDebug(s"Schema file for provider $providerId exists. Comparing with provided schema.")
      val (storedKeySchema, storedValueSchema) = stateFileManager.readSchema()
      if (storedKeySchema.equals(keySchema) && storedValueSchema.equals(valueSchema)) {
        // schema is exactly same
      } else if (!schemasCompatible(storedKeySchema, keySchema) ||
        !schemasCompatible(storedValueSchema, valueSchema)) {
        val errorMsg = "Provided schema doesn't match to the schema for existing state! " +
          "Please note that Spark allow difference of field name: check count of fields " +
          "and data type of each field.\n" +
          s"- Provided key schema: $keySchema\n" +
          s"- Provided value schema: $valueSchema\n" +
          s"- Existing key schema: $storedKeySchema\n" +
          s"- Existing value schema: $storedValueSchema\n" +
          s"If you want to force running query without schema validation, please set " +
          s"${SQLConf.STATE_SCHEMA_CHECK_ENABLED.key} to false.\n" +
          "Please note running query with incompatible schema could cause indeterministic" +
          " behavior."
        logError(errorMsg)
        throw StateSchemaNotCompatible(errorMsg)
      } else {
        logInfo("Detected schema change which is compatible. Allowing to put rows.")
      }
    } else {
      // schema doesn't exist, create one now
      logDebug(s"Schema file for provider $providerId doesn't exist. Creating one.")
      stateFileManager.writeSchema(keySchema, valueSchema)
    }
  }

  private def schemasCompatible(storedSchema: StructType, schema: StructType): Boolean =
    DataType.equalsIgnoreNameAndCompatibleNullability(storedSchema, schema)
}
