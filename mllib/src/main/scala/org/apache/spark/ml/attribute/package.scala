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

package org.apache.spark.ml

import org.apache.spark.ml.attribute.{Attribute, AttributeGroup}
import org.apache.spark.sql.DataFrame

/**
 * ==ML attributes==
 *
 * The ML pipeline API uses `DataFrame`s as ML datasets.
 * Each dataset consists of typed columns, e.g., string, double, vector, etc.
 * However, knowing only the column type may not be sufficient to handle the data properly.
 * For instance, a double column with values 0.0, 1.0, 2.0, ... may represent some label indices,
 * which cannot be treated as numeric values in ML algorithms, and, for another instance, we may
 * want to know the names and types of features stored in a vector column.
 * ML attributes are used to provide additional information to describe columns in a dataset.
 *
 * ===ML columns===
 *
 * A column with ML attributes attached is called an ML column.
 * The data in ML columns are stored as double values, i.e., an ML column is either a scalar column
 * of double values or a vector column.
 * Columns of other types must be encoded into ML columns using transformers.
 * We use [[Attribute]] to describe a scalar ML column, and [[AttributeGroup]] to describe a vector
 * ML column.
 * ML attributes are stored in the metadata field of the column schema.
 */
package object attribute
