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

package org.apache.spark.sql.connector.expressions;

import org.apache.spark.annotation.Experimental;

/**
 * A null order used in sorting expressions.
 *
 * @since 3.2.0
 */
@Experimental
public enum NullOrdering {
  NULLS_FIRST, NULLS_LAST;

  @Override
  public String toString() {
    return switch (this) {
      case NULLS_FIRST -> "NULLS FIRST";
      case NULLS_LAST -> "NULLS LAST";
    };
  }
}
