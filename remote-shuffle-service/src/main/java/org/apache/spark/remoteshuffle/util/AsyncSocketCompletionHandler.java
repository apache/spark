/*
 * This file is copied from Uber Remote Shuffle Service
(https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.remoteshuffle.util;

import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.function.Consumer;

public class AsyncSocketCompletionHandler implements CompletionHandler<Integer, AsyncSocketState> {
  private Consumer<Throwable> exceptionCallback;

  public AsyncSocketCompletionHandler(Consumer<Throwable> exceptionCallback) {
    this.exceptionCallback = exceptionCallback;
  }

  @Override
  public void completed(Integer result, AsyncSocketState attachment) {
    ByteBuffer byteBuffer;

    synchronized (attachment) {
      byteBuffer = attachment.peekBuffer();
      if (byteBuffer == null) {
        return;
      }
    }

    if (byteBuffer.remaining() == 0) {
      synchronized (attachment) {
        ByteBuffer removed = attachment.removeBuffer();
        if (removed != byteBuffer) {
          throw new RuntimeException("Removed buffer not same as expected, something is wrong!");
        }

        byteBuffer = attachment.peekBuffer();
        if (byteBuffer == null) {
          return;
        }
      }

      attachment.getSocket().write(byteBuffer, attachment, this);
      return;
    }

    attachment.getSocket().write(byteBuffer, attachment, this);
  }

  @Override
  public void failed(Throwable exc, AsyncSocketState attachment) {
    this.exceptionCallback.accept(exc);
  }
}
