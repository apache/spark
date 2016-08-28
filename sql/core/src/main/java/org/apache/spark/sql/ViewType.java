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
package org.apache.spark.sql;

/**
 * ViewType is used to specify the type of views.
 */
public enum ViewType {
  /**
   * Temporary means local temporary views. The views are session-scoped and automatically dropped
   * when the session terminates. Do not qualify a temporary table with a schema name. Existing
   * permanent tables or views with the same name are not visible while the temporary view exists,
   * unless they are referenced with schema-qualified names.
   */
  Temporary,
  /**
   * Permanent means global permanent views. The views are global-scoped and accessible by all
   * sessions. The permanent views stays until they are explicitly dropped.
   */
  Permanent,
  /**
   * Any means the view type is unknown.
   */
  Any
}
