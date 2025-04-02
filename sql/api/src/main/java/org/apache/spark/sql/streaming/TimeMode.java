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
import org.apache.spark.sql.catalyst.plans.logical.EventTime$;
import org.apache.spark.sql.catalyst.plans.logical.NoTime$;
import org.apache.spark.sql.catalyst.plans.logical.ProcessingTime$;

/**
 * Represents the time modes (used for specifying timers and ttl) possible for
 * the Dataset operations {@code transformWithState}.
 */
@Evolving
public class TimeMode {

    /**
     * Neither timers nor ttl is supported in this mode.
     */
    public static final TimeMode None() { return NoTime$.MODULE$; }

    /**
     * Stateful processor that uses query processing time to register timers and
     * calculate ttl expiration.
     */
    public static final TimeMode ProcessingTime() { return ProcessingTime$.MODULE$; }

    /**
     * Stateful processor that uses event time to register timers. Note that ttl is not
     * supported in this TimeMode.
     */
    public static final TimeMode EventTime() { return EventTime$.MODULE$; }
}
