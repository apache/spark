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

package org.apache.spark.util.sketch;

import java.io.InputStream;
import java.io.OutputStream;

abstract public class CountMinSketch {
  public abstract double relativeError();

  public abstract double confidence();

  public abstract int depth();

  public abstract int width();

  public abstract long totalCount();

  public abstract void add(Object item);

  public abstract void add(Object item, long count);

  public abstract long estimateCount(Object item);

  public abstract CountMinSketch mergeInPlace(CountMinSketch other);

  public abstract void writeTo(OutputStream out);

  public static CountMinSketch readFrom(InputStream in) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public static CountMinSketch create(int depth, int width, int seed) {
    return new CountMinSketchImpl(depth, width, seed);
  }

  public static CountMinSketch create(double eps, double confidence, int seed) {
    return new CountMinSketchImpl(eps, confidence, seed);
  }
}
