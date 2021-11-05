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

package org.apache.parquet.io;

/**
 * This is a workaround since methods below are not public in {@link ColumnIO}.
 *
 * TODO(SPARK-36511): we should remove this once PARQUET-2050 and PARQUET-2083 are released with
 *   Parquet 1.13.
 */
public class ColumnIOUtil {
  private ColumnIOUtil() {}

  public static int getDefinitionLevel(ColumnIO column) {
    return column.getDefinitionLevel();
  }

  public static int getRepetitionLevel(ColumnIO column) {
    return column.getRepetitionLevel();
  }

  public static String[] getFieldPath(ColumnIO column) {
    return column.getFieldPath();
  }
}
