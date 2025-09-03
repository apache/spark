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

package org.apache.spark;

import org.apache.spark.annotation.Evolving;

/**
 * Query context of a {@link SparkThrowable}. It helps users understand where error occur
 * while executing queries.
 *
 * @since 3.4.0
 */
@Evolving
public interface QueryContext {
    // The type of this query context.
    QueryContextType contextType();

    // The object type of the query which throws the exception.
    // If the exception is directly from the main query, it should be an empty string.
    // Otherwise, it should be the exact object type in upper case. For example, a "VIEW".
    String objectType();

    // The object name of the query which throws the exception.
    // If the exception is directly from the main query, it should be an empty string.
    // Otherwise, it should be the object name. For example, a view name "V1".
    String objectName();

    // The starting index in the query text which throws the exception. The index starts from 0.
    int startIndex();

    // The stopping index in the query which throws the exception. The index starts from 0.
    int stopIndex();

    // The corresponding fragment of the query which throws the exception.
    String fragment();

    // The user code (call site of the API) that caused throwing the exception.
    String callSite();

    // Summary of the exception cause.
    String summary();
}
