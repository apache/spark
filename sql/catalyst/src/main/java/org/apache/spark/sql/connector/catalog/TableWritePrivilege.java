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

package org.apache.spark.sql.connector.catalog;

/**
 * The table write privileges that will be provided when loading a table.
 *
 * @since 3.5.3
 */
public enum TableWritePrivilege {
  /**
   * The privilege for adding rows to the table.
   */
  INSERT,

  /**
   * The privilege for changing existing rows in th table.
   */
  UPDATE,

  /**
   * The privilege for deleting rows from the table.
   */
  DELETE
}
