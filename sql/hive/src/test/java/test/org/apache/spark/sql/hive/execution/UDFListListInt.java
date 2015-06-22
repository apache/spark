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

package org.apache.spark.sql.hive.execution;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.List;

public class UDFListListInt extends UDF {
  /**
   * @param obj
   *   SQL schema: array&lt;struct&lt;x: int, y: int, z: int&gt;&gt;
   *   Java Type: List&lt;List&lt;Integer&gt;&gt;
   */
  @SuppressWarnings("unchecked")
  public long evaluate(Object obj) {
    if (obj == null) {
      return 0L;
    }
    List<List<?>> listList = (List<List<?>>) obj;
    long retVal = 0;
    for (List<?> aList : listList) {
      Number someInt = (Number) aList.get(1);
      try {
        retVal += someInt.longValue();
      } catch (NullPointerException e) {
        System.out.println(e);
      }
    }
    return retVal;
  }
}
