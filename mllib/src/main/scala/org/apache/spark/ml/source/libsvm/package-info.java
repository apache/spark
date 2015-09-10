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

/**
 * "libsvm" package implements Spark SQL data source API for loading LIBSVM data as
 * {@link org.apache.spark.sql.DataFrame}.
 * The loaded {@link org.apache.spark.sql.DataFrame} has two columns: "label" containing labels
 * stored as doubles and "features" containing feature vectors stored as
 * {@link org.apache.spark.mllib.linalg.Vector}s.
 *
 * To use LIBSVM data source, you need to set "libsvm" as the format in
 * {@link org.apache.spark.sql.DataFrameReader} and optionally specify options, for example:
 * <pre><code>
 *   DataFrame df = sqlContext.read.format("libsvm")
 *     .option("numFeatures", "780")
 *     .load("data/mllib/sample_libsvm_data.txt");
 * </code></pre>
 *
 * LIBSVM data source supports the following options:
 * <ul>
 *  <li>
 *    "numFeatures": number of features.
 *    If unspecified or nonpositive, the number of features will be determined automatically at the
 *    cost of one additional pass.
 *    This is also useful when the dataset is already split into multiple files and you want to load
 *    them separately, because some features may not present in certain files, which leads to
 *    inconsistent feature dimensions.
 *  </li>
 *  <li>
 *    "vectorType": feature vector type, "sparse" (default) or "dense".
 *  </li>
 * </ul>
 *
 *  @see <a href="https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/">LIBSVM datasets</a>
 *  @since 1.6.0
 */
package org.apache.spark.ml.source.libsvm;
