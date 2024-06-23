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

package org.apache.parquet.filter2.predicate;

import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.filter2.predicate.Operators.BinaryColumn;
import org.apache.parquet.filter2.predicate.Operators.BooleanColumn;
import org.apache.parquet.filter2.predicate.Operators.DoubleColumn;
import org.apache.parquet.filter2.predicate.Operators.FloatColumn;
import org.apache.parquet.filter2.predicate.Operators.IntColumn;
import org.apache.parquet.filter2.predicate.Operators.LongColumn;

/**
 * TODO (PARQUET-1809): This is a temporary workaround; it is intended to be moved to Parquet.
 */
public final class SparkFilterApi {
  public static IntColumn intColumn(String[] path) {
    return new IntColumn(ColumnPath.get(path));
  }

  public static LongColumn longColumn(String[] path) {
    return new LongColumn(ColumnPath.get(path));
  }

  public static FloatColumn floatColumn(String[] path) {
    return new FloatColumn(ColumnPath.get(path));
  }

  public static DoubleColumn doubleColumn(String[] path) {
    return new DoubleColumn(ColumnPath.get(path));
  }

  public static BooleanColumn booleanColumn(String[] path) {
    return new BooleanColumn(ColumnPath.get(path));
  }

  public static BinaryColumn binaryColumn(String[] path) {
    return new BinaryColumn(ColumnPath.get(path));
  }
}
