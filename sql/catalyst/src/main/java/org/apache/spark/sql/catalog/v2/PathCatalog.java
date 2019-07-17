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

package org.apache.spark.sql.catalog.v2;

import com.google.common.base.Preconditions;

/**
 * Catalog trait for file paths.
 * <p>
 * PathCatalog is a specific type of TableCatalog:
 * 1. The table namespace is always empty.
 * 2. The table name contains one or more file paths.
 *   2.1 if there is only one path, the table name is the path.
 *   2.2 otherwise, the table name is file paths joined by semicolon, e.g. "path1;path2;path3".
 */
public interface PathCatalog extends TableCatalog {
  default Identifier getTableIdentifier(String[] paths) {
    Preconditions.checkNotNull(paths, "Paths cannot be null");
    Preconditions.checkArgument(paths.length > 0, "Paths cannot be empty");
    String name = String.join(";", paths);
    return new IdentifierImpl(new String[]{}, name);
  }
}
