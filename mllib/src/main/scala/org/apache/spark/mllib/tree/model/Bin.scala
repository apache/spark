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

package org.apache.spark.mllib.tree.model

import org.apache.spark.mllib.tree.configuration.FeatureType._

/**
 * Used for "binning" the features bins for faster best split calculation. For a continuous
 * feature, a bin is determined by a low and a high "split". For a categorical feature,
 * the a bin is determined using a single label value (category).
 * @param lowSplit signifying the lower threshold for the continuous feature to be
 *                 accepted in the bin
 * @param highSplit signifying the upper threshold for the continuous feature to be
 *                 accepted in the bin
 * @param featureType type of feature -- categorical or continuous
 * @param category categorical label value accepted in the bin for binary classification
 */
private[tree]
case class Bin(lowSplit: Split, highSplit: Split, featureType: FeatureType, category: Double)
