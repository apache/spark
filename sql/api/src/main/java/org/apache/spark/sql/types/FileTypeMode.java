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
package org.apache.spark.sql.types;

import org.apache.spark.annotation.Unstable;

/**
 * The mode of a {@link FileType} column.
 *
 * <ul>
 *   <li>{@code EXTERNAL} - simple references, lifecycle independent of the table</li>
 *   <li>{@code UNKNOWN} - mode not yet determined, e.g. a file produced by an expression or a UDF
 *   that is not (yet) stored in a column</li>
 * </ul>
 *
 * The mode is carried on {@link FileType} as part of the type's identity.
 *
 * @since 4.4.0
 */
@Unstable
public enum FileTypeMode {
    EXTERNAL,
    UNKNOWN,
}
