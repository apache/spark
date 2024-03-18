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

package org.apache.spark.sql.streaming;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.catalyst.plans.logical.*;

/**
 * Represents the type of ttl modes possible for user defined state
 * in [[StatefulProcessor]].
 */
@Experimental
@Evolving
public class TTLMode {

    /**
     * Specifies that there is no TTL for the state object. Such objects would not
     * be cleaned up by Spark automatically.
     */
    public static final TTLMode NoTTL() {
        return NoTTL$.MODULE$;
    }

    /**
     * Specifies that the specified ttl is in processing time.
     */
    public static final TTLMode ProcessingTimeTTL() {
        return ProcessingTimeTTL$.MODULE$;
    }

    /**
     * Specifies that the specified ttl is in event time.
     */
    public static final TTLMode EventTimeTTL() { return EventTimeTTL$.MODULE$; }
}
