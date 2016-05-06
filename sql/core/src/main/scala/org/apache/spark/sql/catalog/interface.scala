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

import org.apache.spark.sql.catalyst.DefinedByConstructorParams


// Note: all classes here are expected to be wrapped in Datasets and so must extend
// DefinedByConstructorParams for the catalog to be able to create encoders for them.

class Database(
    val name: String,
    @Nullable val description: String,
    val locationUri: String)
  extends DefinedByConstructorParams {

  override def toString: String = {
    "Database[" +
      s"name='$name', " +
      Option(description).map { d => s"description='$d', " }.getOrElse("") +
      s"path='$locationUri']"
  }

}


class Table(
    val name: String,
    @Nullable val database: String,
    @Nullable val description: String,
    val tableType: String,
    val isTemporary: Boolean)
  extends DefinedByConstructorParams {

  override def toString: String = {
    "Table[" +
      s"name='$name', " +
      Option(database).map { d => s"database='$d', " }.getOrElse("") +
      Option(description).map { d => s"description='$d', " }.getOrElse("") +
      s"tableType='$tableType', " +
      s"isTemporary='$isTemporary']"
  }

}


class Column(
    val name: String,
    @Nullable val description: String,
    val dataType: String,
    val nullable: Boolean,
    val isPartition: Boolean,
    val isBucket: Boolean)
  extends DefinedByConstructorParams {

  override def toString: String = {
    "Column[" +
      s"name='$name', " +
      Option(description).map { d => s"description='$d', " }.getOrElse("") +
      s"dataType='$dataType', " +
      s"nullable='$nullable', " +
      s"isPartition='$isPartition', " +
      s"isBucket='$isBucket']"
  }

}


// TODO(andrew): should we include the database here?
class Function(
    val name: String,
    @Nullable val description: String,
    val className: String,
    val isTemporary: Boolean)
  extends DefinedByConstructorParams {

  override def toString: String = {
    "Function[" +
      s"name='$name', " +
      Option(description).map { d => s"description='$d', " }.getOrElse("") +
      s"className='$className', " +
      s"isTemporary='$isTemporary']"
  }

}
