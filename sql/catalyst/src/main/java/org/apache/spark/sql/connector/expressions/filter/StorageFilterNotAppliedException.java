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

package org.apache.spark.sql.connector.expressions.filter;

import java.util.Map;

import org.apache.spark.SparkThrowable;
import org.apache.spark.SparkThrowableHelper;
import org.apache.spark.annotation.Evolving;

/**
 * Thrown by a {@link StorageFilter} that cannot be applied to a row, for instance because
 * evaluating it raised an error. The reader catches it and reads the rest of the unit it was
 * filtering, such as a file or a row group, without the filter. The cause is the error the filter
 * met.
 * <p>
 * It is checked, so a reader handles it where it can fall back to reading without the filter. A
 * reader that lets it out anyway fails the task, instead of dropping or keeping rows the filter
 * never decided on. Its error condition, {@code STORAGE_FILTER_NOT_APPLIED}, then points at the
 * data source's reader.
 *
 * @since 5.0.0
 */
@Evolving
public class StorageFilterNotAppliedException extends Exception implements SparkThrowable {

  private static final String CONDITION = "STORAGE_FILTER_NOT_APPLIED";

  public StorageFilterNotAppliedException(Throwable cause) {
    super(SparkThrowableHelper.getMessage(CONDITION, Map.<String, String>of()), cause);
  }

  @Override
  public String getCondition() {
    return CONDITION;
  }
}
