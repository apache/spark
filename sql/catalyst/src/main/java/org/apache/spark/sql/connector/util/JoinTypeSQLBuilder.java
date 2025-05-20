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

package org.apache.spark.sql.connector.util;

import org.apache.spark.SparkIllegalArgumentException;
import org.apache.spark.sql.connector.join.*;

import java.util.HashMap;
import java.util.Map;

/**
 * The builder to generate SQL for specific Join type.
 *
 * @since 4.0.0
 */
public class JoinTypeSQLBuilder {
  public String build(JoinType joinType) {
    if (joinType instanceof Inner inner) {
        return visitInnerJoin(inner);
    } else {
        return visitUnexpectedJoinType(joinType);
    }
  }

  protected String visitInnerJoin(Inner inner) {
        return "INNER JOIN";
    }

  protected String visitUnexpectedJoinType(JoinType joinType) throws IllegalArgumentException {
    Map<String, String> params = new HashMap<>();
    params.put("joinType", String.valueOf(joinType));
    throw new SparkIllegalArgumentException("_LEGACY_ERROR_TEMP_3209", params);
  }
}
