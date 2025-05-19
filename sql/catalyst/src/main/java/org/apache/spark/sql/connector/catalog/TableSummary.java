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

import org.apache.spark.annotation.Evolving;

import static com.google.common.base.Preconditions.checkNotNull;

@Evolving
public interface TableSummary {
    String MANAGED_TABLE_TYPE = "MANAGED";
    String EXTERNAL_TABLE_TYPE = "EXTERNAL";
    String VIEW_TABLE_TYPE = "VIEW";
    String FOREIGN_TABLE_TYPE = "FOREIGN";

    Identifier identifier();
    String tableType();

    static TableSummary of(Identifier identifier, String tableType) {
        return new TableSummaryImpl(identifier, tableType);
    }
}

record TableSummaryImpl(Identifier identifier, String tableType) implements TableSummary {
    TableSummaryImpl {
        checkNotNull(identifier, "Identifier of a table summary object cannot be null");
        checkNotNull(tableType, "Table type of a table summary object cannot be null");
    }
}
