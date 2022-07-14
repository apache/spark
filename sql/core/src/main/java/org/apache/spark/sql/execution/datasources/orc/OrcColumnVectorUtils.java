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

package org.apache.spark.sql.execution.datasources.orc;

import org.apache.hadoop.hive.ql.exec.vector.*;

import org.apache.spark.sql.types.*;

/**
 * Utility class for {@link OrcColumnVector}.
 */
class OrcColumnVectorUtils {

  /**
   * Convert a Hive's {@link ColumnVector} to a Spark's {@link OrcColumnVector}.
   *
   * @param type The data type of column vector
   * @param vector Hive's column vector
   * @return Spark's column vector
   */
  static OrcColumnVector toOrcColumnVector(DataType type, ColumnVector vector) {
    if (vector instanceof LongColumnVector ||
      vector instanceof DoubleColumnVector ||
      vector instanceof BytesColumnVector ||
      vector instanceof DecimalColumnVector ||
      vector instanceof TimestampColumnVector) {
      return new OrcAtomicColumnVector(type, vector);
    } else if (vector instanceof StructColumnVector) {
      StructColumnVector structVector = (StructColumnVector) vector;
      OrcColumnVector[] fields = new OrcColumnVector[structVector.fields.length];
      int ordinal = 0;
      for (StructField f : ((StructType) type).fields()) {
        fields[ordinal] = toOrcColumnVector(f.dataType(), structVector.fields[ordinal]);
        ordinal++;
      }
      return new OrcStructColumnVector(type, vector, fields);
    } else if (vector instanceof ListColumnVector) {
      ListColumnVector listVector = (ListColumnVector) vector;
      OrcColumnVector dataVector = toOrcColumnVector(
        ((ArrayType) type).elementType(), listVector.child);
      return new OrcArrayColumnVector(type, vector, dataVector);
    } else if (vector instanceof MapColumnVector) {
      MapColumnVector mapVector = (MapColumnVector) vector;
      MapType mapType = (MapType) type;
      OrcColumnVector keysVector = toOrcColumnVector(mapType.keyType(), mapVector.keys);
      OrcColumnVector valuesVector = toOrcColumnVector(mapType.valueType(), mapVector.values);
      return new OrcMapColumnVector(type, vector, keysVector, valuesVector);
    } else {
      throw new IllegalArgumentException(
        String.format("OrcColumnVectorUtils.toOrcColumnVector should not take %s as type " +
          "and %s as vector", type, vector));
    }
  }
}
