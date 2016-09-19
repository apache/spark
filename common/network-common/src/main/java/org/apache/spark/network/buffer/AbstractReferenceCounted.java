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

package org.apache.spark.network.buffer;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import io.netty.util.internal.PlatformDependent;

/**
 * Abstract base class for classes wants to implement {@link ReferenceCounted}.
 */
public abstract class AbstractReferenceCounted implements ReferenceCounted {

  private static final AtomicIntegerFieldUpdater<AbstractReferenceCounted> refCntUpdater;

  static {
    AtomicIntegerFieldUpdater<AbstractReferenceCounted> updater =
        PlatformDependent.newAtomicIntegerFieldUpdater(AbstractReferenceCounted.class, "refCnt");
    if (updater == null) {
      updater = AtomicIntegerFieldUpdater.newUpdater(AbstractReferenceCounted.class, "refCnt");
    }
    refCntUpdater = updater;
  }

  private volatile int refCnt = 1;

  @Override
  public int refCnt() {
    return refCnt;
  }

  /**
   * An unsafe operation intended for use by a subclass that sets the reference count of the buffer directly
   */
  protected void setRefCnt(int refCnt) {
    this.refCnt = refCnt;
  }

  @Override
  public ReferenceCounted retain() {
    doRetain(1);
    return this;
  }

  @Override
  public ReferenceCounted retain(int increment) {
    if (increment <= 0) {
      throw new IllegalArgumentException("increment: " + increment + " (expected: > 0)");
    }
    doRetain(increment);
    return this;
  }

  protected ReferenceCounted doRetain(int increment) {
    for (; ; ) {
      int refCnt = this.refCnt;
      final int nextCnt = refCnt + increment;

      // Ensure we not resurrect (which means the refCnt was 0) and also that we encountered an overflow.
      if (nextCnt <= increment) {
        throw new IllegalReferenceCountException(refCnt, increment);
      }
      if (refCntUpdater.compareAndSet(this, refCnt, nextCnt)) {
        break;
      }
    }
    return this;
  }

  @Override
  public boolean release() {
    return doRelease(1);
  }

  @Override
  public boolean release(int decrement) {
    if (decrement <= 0) {
      throw new IllegalArgumentException("decrement: " + decrement + " (expected: > 0)");
    }
    return doRelease(decrement);
  }

  protected boolean doRelease(int decrement) {
    for (; ; ) {
      int refCnt = this.refCnt;
      if (refCnt < decrement) {
        throw new IllegalReferenceCountException(refCnt, -decrement);
      }

      if (refCntUpdater.compareAndSet(this, refCnt, refCnt - decrement)) {
        if (refCnt == decrement) {
          deallocate();
          return true;
        }
        return false;
      }
    }
  }

  /**
   * Called once {@link #refCnt()} is equals 0.
   */
  protected abstract void deallocate();
}
