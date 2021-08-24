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

package org.apache.spark.sql.connector.write;

import org.apache.spark.annotation.Unstable;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.sources.InsertableRelation;

/**
 * A logical write that should be executed using V1 InsertableRelation interface.
 * <p>
 * Tables that have {@link TableCapability#V1_BATCH_WRITE} in the list of their capabilities
 * must build {@link V1Write}.
 *
 * @since 3.2.0
 */
@Unstable
public interface V1Write extends Write {
  InsertableRelation toInsertableRelation();
}
