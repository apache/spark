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

package org.apache.spark.types.variant;

import java.io.Serializable;

public class VariantMetrics implements Serializable {
  public long variantCount = 0;
  public long byteSize = 0;
  public long numScalars = 0;
  public long numPaths = 0;
  public long maxDepth = 0;
  public void reset() {
    variantCount = 0;
    byteSize = 0;
    numScalars = 0;
    numPaths = 0;
    maxDepth = 0;
  }
}
