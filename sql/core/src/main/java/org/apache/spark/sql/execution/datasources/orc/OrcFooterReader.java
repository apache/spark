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

import org.apache.orc.ColumnStatistics;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.spark.sql.types.*;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

/**
 * {@link OrcFooterReader} is a util class which encapsulates the helper
 * methods of reading ORC file footer.
 */
public class OrcFooterReader {

  /**
   * Read the columns statistics from ORC file footer.
   *
   * @param orcReader the reader to read ORC file footer.
   * @return Statistics for all columns in the file.
   */
  public static OrcColumnStatistics readStatistics(Reader orcReader) {
    TypeDescription orcSchema = orcReader.getSchema();
    ColumnStatistics[] orcStatistics = orcReader.getStatistics();
    StructType sparkSchema = OrcUtils.toCatalystSchema(orcSchema);
    return convertStatistics(sparkSchema, new LinkedList<>(Arrays.asList(orcStatistics)));
  }

  /**
   * Convert a queue of ORC {@link ColumnStatistics}s into Spark {@link OrcColumnStatistics}.
   * The queue of ORC {@link ColumnStatistics}s are assumed to be ordered as tree pre-order.
   */
  private static OrcColumnStatistics convertStatistics(
      DataType sparkSchema, Queue<ColumnStatistics> orcStatistics) {
    OrcColumnStatistics statistics = new OrcColumnStatistics(orcStatistics.remove());
    if (sparkSchema instanceof StructType) {
      for (StructField field : ((StructType) sparkSchema).fields()) {
        statistics.add(convertStatistics(field.dataType(), orcStatistics));
      }
    } else if (sparkSchema instanceof MapType) {
      statistics.add(convertStatistics(((MapType) sparkSchema).keyType(), orcStatistics));
      statistics.add(convertStatistics(((MapType) sparkSchema).valueType(), orcStatistics));
    } else if (sparkSchema instanceof ArrayType) {
      statistics.add(convertStatistics(((ArrayType) sparkSchema).elementType(), orcStatistics));
    }
    return statistics;
  }
}
