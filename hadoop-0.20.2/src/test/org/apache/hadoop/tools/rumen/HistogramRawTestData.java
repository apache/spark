/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.tools.rumen;

import java.util.ArrayList;
import java.util.List;

class HistogramRawTestData {
  List<Long> data = new ArrayList<Long>();

  List<Integer> percentiles = new ArrayList<Integer>();

  int scale;

  public List<Integer> getPercentiles() {
    return percentiles;
  }

  public void setPercentiles(List<Integer> percentiles) {
    this.percentiles = percentiles;
  }

  public int getScale() {
    return scale;
  }

  public void setScale(int scale) {
    this.scale = scale;
  }

  public List<Long> getData() {
    return data;
  }

  public void setData(List<Long> data) {
    this.data = data;
  }
}
