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

package org.apache.spark.unsafe.memory;

import org.apache.spark.unsafe.Platform;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

/**
 * A simple {@link MemoryAllocator} that uses {@code Unsafe} to allocate off-heap memory.
 */
public class UnsafeMemoryAllocator implements MemoryAllocator {

  private static Method bufAddrMethod;
  static {
    try {
      Class cb = UnsafeMemoryAllocator.class.getClassLoader().loadClass("java.nio.DirectByteBuffer");
      bufAddrMethod = cb.getMethod("address");
      bufAddrMethod.setAccessible(true);
    }
    catch(Exception ex) {
      throw new RuntimeException(ex.getMessage(), ex);
    }
  }

  @Override
  public OffHeapMemoryBlock allocate(long size) throws OutOfMemoryError {
    try {
      Object b = ByteBuffer.allocateDirect((int)size);
      long addr = (long)bufAddrMethod.invoke(b);
      return new OffHeapMemoryBlock(b, addr, size);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e.getMessage(), e);
    } catch (InvocationTargetException e) {
      Throwable tex = e.getTargetException();
      if( tex instanceof OutOfMemoryError) {
        throw (OutOfMemoryError) tex;
      }
      else {
        throw new RuntimeException(e.getMessage(), e);
      }
    }
  }

  @Override
  public void free(MemoryBlock memory) {
    // DirectByteBuffers are deallocated automatically by JVM when they become
    // unreachable much like normal Objects in heap
  }

  public OffHeapMemoryBlock reallocate(OffHeapMemoryBlock aBlock, long anOldSize, long aNewSize) {
    OffHeapMemoryBlock nb = this.allocate(aNewSize);
    if( aBlock.getBaseOffset() != 0 )
      Platform.copyMemory(aBlock, aBlock.getBaseOffset(), nb, nb.getBaseOffset(), anOldSize);

    return nb;
  }
}
