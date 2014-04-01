/*
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
package org.apache.hadoop.io.compress;

import java.io.IOException;
import java.io.InputStream;

/**
 * An InputStream covering a range of compressed data. The start and end
 * offsets requested by a client may be modified by the codec to fit block
 * boundaries or other algorithm-dependent requirements.
 */
public abstract class SplitCompressionInputStream
    extends CompressionInputStream {

  private long start;
  private long end;

  public SplitCompressionInputStream(InputStream in, long start, long end)
      throws IOException {
    super(in);
    this.start = start;
    this.end = end;
  }

  protected void setStart(long start) {
    this.start = start;
  }

  protected void setEnd(long end) {
    this.end = end;
  }

  /**
   * After calling createInputStream, the values of start or end
   * might change.  So this method can be used to get the new value of start.
   * @return The changed value of start
   */
  public long getAdjustedStart() {
    return start;
  }

  /**
   * After calling createInputStream, the values of start or end
   * might change.  So this method can be used to get the new value of end.
   * @return The changed value of end
   */
  public long getAdjustedEnd() {
    return end;
  }
}
