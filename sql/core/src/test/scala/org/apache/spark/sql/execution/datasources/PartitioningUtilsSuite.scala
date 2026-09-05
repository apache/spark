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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._

class PartitioningUtilsSuite extends SparkFunSuite {

  test("SPARK-51830: removeLeadingZerosFromNumberTypePartition with validation disabled") {
    // Test with validation enabled (default behavior)
    // String values that cannot be cast to numeric types should throw NumberFormatException
    intercept[NumberFormatException] {
      PartitioningUtils.removeLeadingZerosFromNumberTypePartition(
        "abc", IntegerType, validatePartitionColumns = true)
    }

    intercept[NumberFormatException] {
      PartitioningUtils.removeLeadingZerosFromNumberTypePartition(
        "xyz", LongType, validatePartitionColumns = true)
    }

    intercept[NumberFormatException] {
      PartitioningUtils.removeLeadingZerosFromNumberTypePartition(
        "invalid", DoubleType, validatePartitionColumns = true)
    }

    // Test with validation disabled
    // String values should be preserved as-is for legacy compatibility
    assert(
      PartitioningUtils.removeLeadingZerosFromNumberTypePartition(
        "abc", IntegerType, validatePartitionColumns = false) === "abc"
    )

    assert(
      PartitioningUtils.removeLeadingZerosFromNumberTypePartition(
        "xyz", LongType, validatePartitionColumns = false) === "xyz"
    )

    assert(
      PartitioningUtils.removeLeadingZerosFromNumberTypePartition(
        "invalid", DoubleType, validatePartitionColumns = false) === "invalid"
    )

    // Valid numeric strings should still be normalized when validation is disabled
    assert(
      PartitioningUtils.removeLeadingZerosFromNumberTypePartition(
        "42", IntegerType, validatePartitionColumns = false) === "42"
    )

    assert(
      PartitioningUtils.removeLeadingZerosFromNumberTypePartition(
        "9223372036854775807", LongType, validatePartitionColumns = false) === "9223372036854775807"
    )

    // Leading zeros should be removed for valid numbers
    assert(
      PartitioningUtils.removeLeadingZerosFromNumberTypePartition(
        "007", IntegerType, validatePartitionColumns = false) === "7"
    )

    assert(
      PartitioningUtils.removeLeadingZerosFromNumberTypePartition(
        "00123", LongType, validatePartitionColumns = true) === "123"
    )

    // Non-numeric types should pass through unchanged
    assert(
      PartitioningUtils.removeLeadingZerosFromNumberTypePartition(
        "abc", StringType, validatePartitionColumns = true) === "abc"
    )

    assert(
      PartitioningUtils.removeLeadingZerosFromNumberTypePartition(
        "2024-01-15", DateType, validatePartitionColumns = false) === "2024-01-15"
    )

    // Null values should be handled
    assert(
      PartitioningUtils.removeLeadingZerosFromNumberTypePartition(
        null, IntegerType, validatePartitionColumns = true) === null
    )

    assert(
      PartitioningUtils.removeLeadingZerosFromNumberTypePartition(
        null, StringType, validatePartitionColumns = false) === null
    )
  }
}

