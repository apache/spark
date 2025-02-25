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

import java.util.Optional;

/**
 * :: Experimental ::
 * Behavior trait for supporting backend for the Spark Connect ML.
 *
 * @since 4.0.0
 */
public interface MLBackendPlugin {
    /**
     * Replaces the specified Spark ML operators with a backend-specific implementation.
     * Returns Optional.empty() if no replacement is provided for the given operator
     *
     * @param mlName The name of the operator
     * @return an optional replaced operator name
     */
    Optional<String> transform(String mlName);
}
