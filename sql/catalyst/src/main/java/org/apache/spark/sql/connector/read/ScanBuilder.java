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

package org.apache.spark.sql.connector.read;

import org.apache.spark.annotation.Evolving;

/**
 * An interface for building the {@link Scan}. Implementations can mixin SupportsPushDownXYZ
 * interfaces to do operator push down, and keep the operator push down result in the returned
 * {@link Scan}. When pushing down operators, the push down order is:
 * sample -&gt; filter -&gt; aggregate -&gt; limit/top-n(sort + limit) -&gt; offset -&gt;
 * column pruning.
 *
 * @since 3.0.0
 */
@Evolving
public interface ScanBuilder {
  Scan build();
}
