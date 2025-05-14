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

package org.apache.spark.sql.connector.catalog;

import static com.google.common.base.Preconditions.checkNotNull;

public record TableSummary(Identifier identifier, String tableType) {
    public static final String REGULAR_TABLE_TYPE = "TABLE";
    public static final String REGULAR_VIEW_TABLE_TYPE = "VIEW";

    public TableSummary {
        checkNotNull(identifier, "Identifier of a table summary object cannot be null");
        checkNotNull(tableType, "Table type of a table summary object cannot be null");
    }
}
