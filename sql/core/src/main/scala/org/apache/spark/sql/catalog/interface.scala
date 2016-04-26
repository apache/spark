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

package org.apache.spark.sql.catalog

import javax.annotation.Nullable


class Database(
    val name: String,
    val description: String,
    val locationUri: String)

class Table(
    val name: String,
    @Nullable val database: String,
    @Nullable val description: String,
    val tableType: String,
    val isTemporary: Boolean)

class Column(
    val name: String,
    @Nullable val description: String,
    val dataType: String,
    val nullable: Boolean,
    val isPartition: Boolean,
    val isBucket: Boolean)

class Function(
    val name: String,
    @Nullable val description: String,
    val className: String,
    val isTemporary: Boolean)
