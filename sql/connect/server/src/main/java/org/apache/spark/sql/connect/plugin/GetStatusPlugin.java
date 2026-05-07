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

package org.apache.spark.sql.connect.plugin;

import com.google.protobuf.Any;

import java.util.List;
import java.util.Optional;

import org.apache.spark.sql.connect.service.SessionHolder;

/**
 * Plugin interface for extending GetStatus RPC behavior in Spark Connect.
 *
 * <p>Classes implementing this interface must be trivially constructable (have a no-argument
 * constructor) and should not rely on internal state. The plugin is invoked during GetStatus
 * request handling, allowing custom logic to be executed based on request extensions.
 *
 * <p>The GetStatus RPC message has two extension points:
 * <ul>
 *   <li>{@code GetStatusRequest.extensions} - request-level extensions</li>
 *   <li>{@code GetStatusRequest.OperationStatusRequest.extensions}
 *       - operation-level extensions</li>
 * </ul>
 *
 * <p>And corresponding response extension points:
 * <ul>
 *   <li>{@code GetStatusResponse.extensions} - response-level extensions</li>
 *   <li>{@code GetStatusResponse.OperationStatus.extensions} - operation-level extensions</li>
 * </ul>
 */
public interface GetStatusPlugin {

    /**
     * Process request-level extensions from a GetStatus request.
     *
     * <p>This method is called once per GetStatus request, before operation statuses are processed.
     * Plugins can use the request extensions to customize behavior and return response extensions.
     *
     * @param sessionHolder the session holder for the current session
     * @param requestExtensions the extensions from the GetStatus request
     * @return optional list of response extensions to add to the GetStatusResponse;
     *         return {@code Optional.empty()} if this plugin does not handle the request extensions
     */
    Optional<List<Any>> processRequestExtensions(
        SessionHolder sessionHolder, List<Any> requestExtensions);

    /**
     * Process operation-level extensions from an OperationStatusRequest.
     *
     * <p>This method is called once per operation whose status is requested.
     * Plugins can use the operation-level extensions to customize per-operation behavior.
     *
     * @param operationId the operation ID being queried
     * @param sessionHolder the session holder for the current session
     * @param operationExtensions the extensions from the OperationStatusRequest
     * @return optional list of response extensions to add to the OperationStatus;
     *         return {@code Optional.empty()} if this plugin does not handle the extensions
     */
    Optional<List<Any>> processOperationExtensions(
        String operationId, SessionHolder sessionHolder, List<Any> operationExtensions);
}
