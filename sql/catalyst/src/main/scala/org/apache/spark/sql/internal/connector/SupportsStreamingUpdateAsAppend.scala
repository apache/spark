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

package org.apache.spark.sql.internal.connector

import org.apache.spark.sql.connector.write.WriteBuilder

/**
 * An internal `WriteBuilder` mixin to support UPDATE streaming output mode. Now there's no good
 * way to pass the `keys` to upsert or replace (delete -> append), we do the same with append writes
 * and let end users to deal with.
 *
 * This approach may be still valid for streaming writers which can't do the upsert or replace.
 * We can promote the API to the official API along with the new API for upsert/replace.
 */
// TODO: design an official API for streaming output mode UPDATE which can do the upsert
//  (or delete -> append).
trait SupportsStreamingUpdateAsAppend extends WriteBuilder {
}
