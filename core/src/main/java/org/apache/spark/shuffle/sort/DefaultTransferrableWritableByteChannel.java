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

package org.apache.spark.shuffle.sort;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import org.apache.spark.api.shuffle.TransferrableWritableByteChannel;
import org.apache.spark.util.Utils;

/**
 * This is used when transferTo is enabled but the shuffle plugin hasn't implemented
 * {@link org.apache.spark.api.shuffle.SupportsTransferTo}.
 * <p>
 * This default implementation exists as a convenience to the unsafe shuffle writer and
 * the bypass merge sort shuffle writers.
 */
public class DefaultTransferrableWritableByteChannel implements TransferrableWritableByteChannel {

  private final WritableByteChannel delegate;

  public DefaultTransferrableWritableByteChannel(WritableByteChannel delegate) {
    this.delegate = delegate;
  }

  @Override
  public void transferFrom(
      FileChannel source, long transferStartPosition, long numBytesToTransfer) {
    Utils.copyFileStreamNIO(source, delegate, transferStartPosition, numBytesToTransfer);
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
