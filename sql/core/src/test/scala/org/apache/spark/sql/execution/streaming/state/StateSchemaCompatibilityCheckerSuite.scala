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

import java.util.UUID

import scala.util.Random

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.execution.streaming.state.StateStoreTestsHelper.newDir
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class StateSchemaCompatibilityCheckerSuite extends SharedSparkSession {

  testQuietly("changing schema of state when restarting query") {
    val opId = Random.nextInt(100000)
    val partitionId = -1

    val hadoopConf: Configuration = new Configuration()

    def runSchemaChecker(
        dir: String,
        queryId: UUID,
        newKeySchema: StructType,
        newValueSchema: StructType): Unit = {
      // in fact, Spark doesn't support online state schema change, so need to check
      // schema only once for each running of JVM
      val providerId = StateStoreProviderId(
        StateStoreId(dir, opId, partitionId), queryId)

      new StateSchemaCompatibilityChecker(providerId, hadoopConf)
        .check(newKeySchema, newValueSchema)
    }

    def verifyException(
        oldKeySchema: StructType,
        oldValueSchema: StructType,
        newKeySchema: StructType,
        newValueSchema: StructType): Unit = {
      val dir = newDir()
      val queryId = UUID.randomUUID()
      runSchemaChecker(dir, queryId, oldKeySchema, oldValueSchema)

      val e = intercept[StateSchemaNotCompatible] {
        runSchemaChecker(dir, queryId, newKeySchema, newValueSchema)
      }

      e.getMessage.contains("Provided schema doesn't match to the schema for existing state!")
      e.getMessage.contains(newKeySchema.json)
      e.getMessage.contains(newValueSchema.json)
      e.getMessage.contains(oldKeySchema.json)
      e.getMessage.contains(oldValueSchema.json)
    }

    def verifySuccess(
        oldKeySchema: StructType,
        oldValueSchema: StructType,
        newKeySchema: StructType,
        newValueSchema: StructType): Unit = {
      val dir = newDir()
      val queryId = UUID.randomUUID()
      runSchemaChecker(dir, queryId, oldKeySchema, oldValueSchema)
      runSchemaChecker(dir, queryId, newKeySchema, newValueSchema)
    }

    val keySchema = new StructType()
      .add(StructField("key1", IntegerType, nullable = true))
      .add(StructField("key2", StringType, nullable = true))

    val valueSchema = new StructType()
      .add(StructField("value1", IntegerType, nullable = true))
      .add(StructField("value2", StringType, nullable = true))

    // adding field should fail
    val fieldAddedKeySchema = keySchema.add(StructField("newKey", IntegerType))
    val fieldAddedValueSchema = valueSchema.add(StructField("newValue", IntegerType))
    verifyException(keySchema, valueSchema, fieldAddedKeySchema, fieldAddedValueSchema)

    // removing field should fail
    val fieldRemovedKeySchema = StructType(keySchema.dropRight(1))
    val fieldRemovedValueSchema = StructType(valueSchema.drop(1))
    verifyException(keySchema, valueSchema, fieldRemovedKeySchema, fieldRemovedValueSchema)

    // changing the type of field should fail
    val typeChangedKeySchema = StructType(keySchema.map(_.copy(dataType = TimestampType)))
    val typeChangedValueSchema = StructType(keySchema.map(_.copy(dataType = TimestampType)))
    verifyException(keySchema, valueSchema, typeChangedKeySchema, typeChangedValueSchema)

    // changing the nullability of nullable to non-nullable should fail
    val nonNullChangedKeySchema = StructType(keySchema.map(_.copy(nullable = false)))
    val nonNullChangedValueSchema = StructType(valueSchema.map(_.copy(nullable = false)))
    verifyException(keySchema, valueSchema, nonNullChangedKeySchema, nonNullChangedValueSchema)

    // changing the nullability of non-nullable to nullable should be allowed
    verifySuccess(nonNullChangedKeySchema, nonNullChangedValueSchema, keySchema, valueSchema)

    // changing the name of field should be allowed
    val newName: StructField => StructField = f => f.copy(name = f.name + "_new")
    val fieldNameChangedKeySchema = StructType(keySchema.map(newName))
    val fieldNameChangedValueSchema = StructType(valueSchema.map(newName))

    verifySuccess(keySchema, valueSchema, fieldNameChangedKeySchema, fieldNameChangedValueSchema)
  }
}
