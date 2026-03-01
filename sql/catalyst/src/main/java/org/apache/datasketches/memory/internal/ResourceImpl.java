/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.memory.internal;

import static org.apache.datasketches.memory.internal.UnsafeUtil.unsafe;
import static org.apache.datasketches.memory.internal.Util.characterPad;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.datasketches.memory.MemoryBoundsException;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.ReadOnlyException;
import org.apache.datasketches.memory.Resource;

// temp copied for testing
// https://github.com/apache/datasketches-memory/pull/272

/**
 * Implements the root Resource methods plus some common static variables and check methods.
 *
 * @author Lee Rhodes
 */
@SuppressWarnings("restriction")
public abstract class ResourceImpl implements Resource {
  static final String JDK;
  static final int JDK_MAJOR; //8, 11, 17, etc

  //Used to convert "type" to bytes:  bytes = longs << LONG_SHIFT
  static final int BOOLEAN_SHIFT    = 0;
  static final int BYTE_SHIFT       = 0;
  static final long SHORT_SHIFT     = 1;
  static final long CHAR_SHIFT      = 1;
  static final long INT_SHIFT       = 2;
  static final long LONG_SHIFT      = 3;
  static final long FLOAT_SHIFT     = 2;
  static final long DOUBLE_SHIFT    = 3;

  //class type IDs. Do not change the bit orders
  //The lowest 3 bits are set dynamically
  // 0000 0XXX Group 1
  static final int WRITABLE  = 0; //bit 0 = 0
  static final int READONLY  = 1; //bit 0
  static final int REGION    = 2; //bit 1
  static final int DUPLICATE = 4; //bit 2, for Buffer only

  // 000X X000 Group 2
  static final int HEAP   = 0;    //bits 3,4 = 0
  static final int DIRECT = 8;    //bit 3
  static final int MAP    = 16;   //bit 4, Map is effectively Direct

  // 00X0 0000 Group 3 ByteOrder
  static final int NATIVE_BO    = 0; //bit 5 = 0
  static final int NONNATIVE_BO = 32;//bit 5

  // 0X00 0000 Group 4
  static final int MEMORY = 0;    //bit 6 = 0
  static final int BUFFER = 64;   //bit 6

  // X000 0000 Group 5
  static final int BYTEBUF = 128; //bit 7

  /**
   * The java line separator character as a String.
   */
  public static final String LS = System.getProperty("line.separator");

  static final String NOT_MAPPED_FILE_RESOURCE = "This is not a memory-mapped file resource";
  static final String THREAD_EXCEPTION_TEXT = "Attempted access outside owning thread";

  private static AtomicBoolean JAVA_VERSION_WARNING_PRINTED = new AtomicBoolean(false);

  static {
    final String jdkVer = System.getProperty("java.version");
    final int[] p = parseJavaVersion(jdkVer);
    JDK = p[0] + "." + p[1];
    JDK_MAJOR = (p[0] == 1) ? p[1] : p[0];
  }

  //set by the leaf nodes
  long capacityBytes;
  long cumOffsetBytes;
  long offsetBytes;
  int typeId;
  Thread owner = null;

  /**
   * The root of the Memory inheritance hierarchy
   */
  ResourceImpl() { }

  //MemoryRequestServer logic

  /**
   * User specified MemoryRequestServer. Set here and by leaf nodes.
   */
  MemoryRequestServer memReqSvr = null;

  @Override
  public MemoryRequestServer getMemoryRequestServer() {
    return memReqSvr;
  }

  @Override
  public boolean hasMemoryRequestServer() {
    return memReqSvr != null;
  }

  @Override
  public void setMemoryRequestServer(final MemoryRequestServer memReqSvr) { this.memReqSvr = memReqSvr; }

  //***

  /**
   * Check the requested offset and length against the allocated size.
   * The invariants equation is: {@code 0 <= reqOff <= reqLen <= reqOff + reqLen <= allocSize}.
   * If this equation is violated an {@link MemoryBoundsException} will be thrown.
   * @param reqOff the requested offset
   * @param reqLen the requested length
   * @param allocSize the allocated size.
   * @throws MemoryBoundsException if the given arguments constitute a violation
   * of the invariants equation expressed above.
   */
  public static void checkBounds(final long reqOff, final long reqLen, final long allocSize) {
    if ((reqOff | reqLen | (reqOff + reqLen) | (allocSize - (reqOff + reqLen))) < 0) {
      throw new MemoryBoundsException(
          "reqOffset: " + reqOff + ", reqLength: " + reqLen
              + ", (reqOff + reqLen): " + (reqOff + reqLen) + ", allocSize: " + allocSize);
    }
  }

  /**
   * Checks the runtime Java Version string. Note that Java 17 and 21 is allowed only because some clients do not use the
   * WritableMemory.allocateDirect(..) and related functions, which will not work with Java versions >= 14.
   * The on-heap functions may work with 17 and 21, nonetheless, versions > Java 11 are not officially supported.
   * Caveat emptor.
   * @param jdkVer the <i>System.getProperty("java.version")</i> string of the form "p0.p1.X"
   * @param p0 The first number group
   * @param p1 The second number group
   */
  static void checkJavaVersion(final String jdkVer, final int p0, final int p1 ) {
    final boolean ok = ((p0 == 1) && (p1 == 8)) || (p0 == 8) || (p0 == 11) || (p0 == 17 || (p0 == 21) || (p0 == 25));
    if (!ok) { throw new IllegalArgumentException(
        "Unsupported JDK Major Version. It must be one of 1.8, 8, 11, 17, 21, 25: " + jdkVer);
    }
    if (p0 > 11 && JAVA_VERSION_WARNING_PRINTED.compareAndSet(false, true)) {
      System.err.println(
          "Warning: Java versions > Java 11 can only operate in restricted mode where no off-heap operations are allowed!");
    }
  }

  void checkNotReadOnly() {
    if (isReadOnly()) {
      throw new ReadOnlyException("Cannot write to a read-only Resource.");
    }
  }

  /**
   * This checks that the current thread is the same as the given owner thread.
   * @Throws IllegalStateException if it is not.
   * @param owner the given owner thread.
   */
  static final void checkThread(final Thread owner) {
    if (owner != Thread.currentThread()) {
      throw new IllegalStateException(THREAD_EXCEPTION_TEXT);
    }
  }

  /**
   * @throws IllegalStateException if this Resource is AutoCloseable, and already closed, i.e., not <em>alive</em>.
   */
  void checkValid() {
    if (!isAlive()) {
      throw new IllegalStateException("this Resource is AutoCloseable, and already closed, i.e., not <em>alive</em>.");
    }
  }

  /**
   * Checks that this resource is still valid and throws a MemoryInvalidException if it is not.
   * Checks that the specified range of bytes is within bounds of this resource, throws
   * {@link MemoryBoundsException} if it's not: i. e. if offsetBytes &lt; 0, or length &lt; 0,
   * or offsetBytes + length &gt; {@link #getCapacity()}.
   * @param offsetBytes the given offset in bytes of this object
   * @param lengthBytes the given length in bytes of this object
   * @throws IllegalStateException if this resource is AutoCloseable and is no longer valid, i.e.,
   * it has already been closed.
   * @throws MemoryBoundsException if this resource violates the memory bounds of this resource.
   */
  public final void checkValidAndBounds(final long offsetBytes, final long lengthBytes) {
    checkValid();
    checkBounds(offsetBytes, lengthBytes, getCapacity());
  }

  /**
   * Checks that this resource is still valid and throws a MemoryInvalidException if it is not.
   * Checks that the specified range of bytes is within bounds of this resource, throws
   * {@link MemoryBoundsException} if it's not: i. e. if offsetBytes &lt; 0, or length &lt; 0,
   * or offsetBytes + length &gt; {@link #getCapacity()}.
   * Checks that this operation is a read-only operation and throws a ReadOnlyException if not.
   * @param offsetBytes the given offset in bytes of this object
   * @param lengthBytes the given length in bytes of this object
   * @Throws MemoryInvalidException if this resource is AutoCloseable and is no longer valid, i.e.,
   * it has already been closed.
   * @Throws MemoryBoundsException if this resource violates the memory bounds of this resource.
   * @Throws ReadOnlyException if the associated operation is not a Read-only operation.
   */
  final void checkValidAndBoundsForWrite(final long offsetBytes, final long lengthBytes) {
    checkValid();
    checkBounds(offsetBytes, lengthBytes, getCapacity());
    if (isReadOnly()) {
      throw new ReadOnlyException("Memory is read-only.");
    }
  }

  @Override
  public void close() {
    /* Overridden by the leaf sub-classes that need AutoCloseable. */
  }

  @Override
  public final boolean equalTo(final long thisOffsetBytes, final Resource that,
      final long thatOffsetBytes, final long lengthBytes) {
    if (that == null) { return false; }
    return CompareAndCopy.equals(this, thisOffsetBytes, (ResourceImpl) that, thatOffsetBytes, lengthBytes);
  }

  @Override
  public void force() { //overridden by Map Leaves
    throw new UnsupportedOperationException(NOT_MAPPED_FILE_RESOURCE);
  }

  //Overridden by ByteBuffer Leaves. Used internally and for tests.
  ByteBuffer getByteBuffer() {
    return null;
  }

  @Override
  public final ByteOrder getTypeByteOrder() {
    return isNativeOrder(getTypeId()) ? Util.NATIVE_BYTE_ORDER : Util.NON_NATIVE_BYTE_ORDER;
  }

  @Override
  public long getCapacity() {
    checkValid();
    return capacityBytes;
  }

  @Override
  public long getCumulativeOffset(final long addOffsetBytes) {
    return cumOffsetBytes + addOffsetBytes;
  }

  @Override
  public long getRelativeOffset() {
    return offsetBytes;
  }

  //Overridden by all leaves
  int getTypeId() {
    return typeId;
  }

  //Overridden by Heap and ByteBuffer leaves. Made public as getArray() in BaseWritableMemoryImpl and BaseWritableBufferImpl
  Object getUnsafeObject() {
    return null;
  }

  @Override
  public boolean hasByteBuffer() {
    return (getTypeId() & BYTEBUF) > 0;
  }

  @Override
  public final boolean isByteOrderCompatible(final ByteOrder byteOrder) {
    final ByteOrder typeBO = getTypeByteOrder();
    return typeBO == ByteOrder.nativeOrder() && typeBO == byteOrder;
  }

  static final boolean isBuffer(final int typeId) {
    return (typeId & BUFFER) > 0;
  }

  @Override
  public boolean isCloseable() {
    return (getTypeId() & (MAP | DIRECT)) > 0 && isAlive();
  }

  @Override
  public final boolean isDirect() {
    return getUnsafeObject() == null;
  }

  @Override
  public boolean isDuplicate() {
    return (getTypeId() & DUPLICATE) > 0;
  }

  @Override
  public final boolean isHeap() {
    checkValid();
    return getUnsafeObject() != null;
  }

  @Override
  public boolean isLoaded() { //overridden by Map Leaves
    throw new IllegalStateException(NOT_MAPPED_FILE_RESOURCE);
  }

  @Override
  public boolean isMapped() {
    return (getTypeId() & MAP) > 0;
  }

  @Override
  public boolean isMemory() {
    return (getTypeId() & BUFFER) == 0;
  }

  static final boolean isNativeOrder(final int typeId) { //not used
    return (typeId & NONNATIVE_BO) == 0;
  }

  @Override
  public boolean isNonNativeOrder() {
    return (getTypeId() & NONNATIVE_BO) > 0;
  }

  @Override
  public final boolean isReadOnly() {
    checkValid();
    return (getTypeId() & READONLY) > 0;
  }

  @Override
  public boolean isRegionView() {
    return (getTypeId() & REGION) > 0;
  }

  @Override
  public boolean isSameResource(final Resource that) {
    checkValid();
    if (that == null) { return false; }
    final ResourceImpl that1 = (ResourceImpl) that;
    that1.checkValid();
    if (this == that1) { return true; }
    return getCumulativeOffset(0) == that1.getCumulativeOffset(0)
            && getCapacity() == that1.getCapacity()
            && getUnsafeObject() == that1.getUnsafeObject()
            && getByteBuffer() == that1.getByteBuffer();
  }

  //Overridden by Direct and Map leaves
  @Override
  public boolean isAlive() {
    return true;
  }

  @Override
  public void load() { //overridden by Map leaves
    throw new IllegalStateException(NOT_MAPPED_FILE_RESOURCE);
  }

  private static String pad(final String s, final int fieldLen) {
    return characterPad(s, fieldLen, ' ' , true);
  }

  /**
   * Returns first two number groups of the java version string.
   * @param jdkVer the java version string from System.getProperty("java.version").
   * @return first two number groups of the java version string.
   * @throws IllegalArgumentException for an improper Java version string.
   */
  static int[] parseJavaVersion(final String jdkVer) {
    final int p0, p1;
    try {
      String[] parts = jdkVer.trim().split("^0-9\\.");//grab only number groups and "."
      parts = parts[0].split("\\."); //split out the number groups
      p0 = Integer.parseInt(parts[0]); //the first number group
      p1 = (parts.length > 1) ? Integer.parseInt(parts[1]) : 0; //2nd number group, or 0
    } catch (final NumberFormatException | ArrayIndexOutOfBoundsException  e) {
      throw new IllegalArgumentException("Improper Java -version string: " + jdkVer + LS + e);
    }
    checkJavaVersion(jdkVer, p0, p1);
    return new int[] {p0, p1};
  }

  //REACHABILITY FENCE
  static void reachabilityFence(final Object obj) { }

  final static int removeNnBuf(final int typeId) { return typeId & ~NONNATIVE_BO & ~BUFFER; }

  final static int setReadOnlyBit(final int typeId, final boolean readOnly) {
    return readOnly ? typeId | READONLY : typeId & ~READONLY;
  }

  /**
   * Returns a formatted hex string of an area of this object.
   * Used primarily for testing.
   * @param state the ResourceImpl
   * @param preamble a descriptive header
   * @param offsetBytes offset bytes relative to the MemoryImpl start
   * @param lengthBytes number of bytes to convert to a hex string
   * @return a formatted hex string in a human readable array
   */
  static final String toHex(final ResourceImpl state, final String preamble, final long offsetBytes, final int lengthBytes,
      final boolean withData) {
    final long capacity = state.getCapacity();
    ResourceImpl.checkBounds(offsetBytes, lengthBytes, capacity);
    final StringBuilder sb = new StringBuilder();
    final Object uObj = state.getUnsafeObject();
    final String uObjStr;
    final long uObjHeader;
    if (uObj == null) {
      uObjStr = "null";
      uObjHeader = 0;
    } else {
      uObjStr =  uObj.getClass().getSimpleName() + ", " + (uObj.hashCode() & 0XFFFFFFFFL);
      uObjHeader = UnsafeUtil.getArrayBaseOffset(uObj.getClass());
    }
    final ByteBuffer bb = state.getByteBuffer();
    final String bbStr = bb == null ? "null"
            : bb.getClass().getSimpleName() + ", " + (bb.hashCode() & 0XFFFFFFFFL);
    final MemoryRequestServer memReqSvr = state.getMemoryRequestServer();
    final String memReqStr = memReqSvr != null
        ? memReqSvr.getClass().getSimpleName() + ", " + (memReqSvr.hashCode() & 0XFFFFFFFFL)
        : "null";
    final long cumBaseOffset = state.getCumulativeOffset(0);
    sb.append(preamble).append(LS);
    sb.append("UnsafeObj, hashCode : ").append(uObjStr).append(LS);
    sb.append("UnsafeObjHeader     : ").append(uObjHeader).append(LS);
    sb.append("ByteBuf, hashCode   : ").append(bbStr).append(LS);
    sb.append("RegionOffset        : ").append(state.getRelativeOffset()).append(LS);
    if (ResourceImpl.isBuffer(state.typeId)) {
      sb.append("Start               : ").append(((PositionalImpl)state).getStart()).append(LS);
      sb.append("Position            : ").append(((PositionalImpl)state).getPosition()).append(LS);
      sb.append("End                 : ").append(((PositionalImpl)state).getEnd()).append(LS);
    }
    sb.append("Capacity            : ").append(capacity).append(LS);
    sb.append("CumBaseOffset       : ").append(cumBaseOffset).append(LS);
    sb.append("MemReqSvr, hashCode : ").append(memReqStr).append(LS);
    sb.append("is Alive            : ").append(state.isAlive()).append(LS);
    sb.append("Read Only           : ").append(state.isReadOnly()).append(LS);
    sb.append("Type Byte Order     : ").append(state.getTypeByteOrder().toString()).append(LS);
    sb.append("Native Byte Order   : ").append(ByteOrder.nativeOrder().toString()).append(LS);
    sb.append("JDK Runtime Version : ").append(JDK).append(LS);
    //Data detail
    if (withData) {
      sb.append("Data, bytes         :  0  1  2  3  4  5  6  7");

      for (long i = 0; i < lengthBytes; i++) {
        final int b = unsafe.getByte(uObj, cumBaseOffset + offsetBytes + i) & 0XFF;
        if (i % 8 == 0) { //row header
          sb.append(String.format("%n%20s: ", offsetBytes + i));
        }
        sb.append(String.format("%02x ", b));
      }
      sb.append(LS);
    }
    sb.append("### END SUMMARY");
    return sb.toString();
  }

  @Override
  public final String toString(final String header, final long offsetBytes, final int lengthBytes,
      final boolean withData) {
    checkValid();
    final String klass = this.getClass().getSimpleName();
    final String s1 = String.format("(..., %d, %d)", offsetBytes, lengthBytes);
    final long hcode = hashCode() & 0XFFFFFFFFL;
    final String call = ".toHexString" + s1 + ", hashCode: " + hcode;
    final StringBuilder sb = new StringBuilder();
    sb.append("### ").append(klass).append(" SUMMARY ###").append(LS);
    sb.append("Type Info           : ").append(typeDecode(typeId)).append(LS + LS);
    sb.append("Header Comment      : ").append(header).append(LS);
    sb.append("Call Parameters     : ").append(call);
    return toHex(this, sb.toString(), offsetBytes, lengthBytes, withData);
  }

  @Override
  public final String toString() {
    return toString("", 0, (int)this.getCapacity(), false);
  }

  /**
   * Decodes the resource type. This is primarily for debugging.
   * @param typeId the given typeId
   * @return a human readable string.
   */
  static final String typeDecode(final int typeId) {
    final StringBuilder sb = new StringBuilder();
    final int group1 = typeId & 0x7;
    switch (group1) { // 0000 0XXX
      case 0 : sb.append(pad("Writable + ",32)); break;
      case 1 : sb.append(pad("ReadOnly + ",32)); break;
      case 2 : sb.append(pad("Writable + Region + ",32)); break;
      case 3 : sb.append(pad("ReadOnly + Region + ",32)); break;
      case 4 : sb.append(pad("Writable + Duplicate + ",32)); break;
      case 5 : sb.append(pad("ReadOnly + Duplicate + ",32)); break;
      case 6 : sb.append(pad("Writable + Region + Duplicate + ",32)); break;
      case 7 : sb.append(pad("ReadOnly + Region + Duplicate + ",32)); break;
      default: break;
    }
    final int group2 = (typeId >>> 3) & 0x3;
    switch (group2) { // 000X X000
      case 0 : sb.append(pad("Heap + ",15)); break;
      case 1 : sb.append(pad("Direct + ",15)); break;
      case 2 : sb.append(pad("Map + Direct + ",15)); break;
      case 3 : sb.append(pad("Map + Direct + ",15)); break;
      default: break;
    }
    final int group3 = (typeId >>> 5) & 0x1;
    switch (group3) { // 00X0 0000
      case 0 : sb.append(pad("NativeOrder + ",17)); break;
      case 1 : sb.append(pad("NonNativeOrder + ",17)); break;
      default: break;
    }
    final int group4 = (typeId >>> 6) & 0x1;
    switch (group4) { // 0X00 0000
      case 0 : sb.append(pad("Memory + ",9)); break;
      case 1 : sb.append(pad("Buffer + ",9)); break;
      default: break;
    }
    final int group5 = (typeId >>> 7) & 0x1;
    switch (group5) { // X000 0000
      case 0 : sb.append(pad("",10)); break;
      case 1 : sb.append(pad("ByteBuffer",10)); break;
      default: break;
    }
    return sb.toString();
  }

  @Override
  public final long xxHash64(final long offsetBytes, final long lengthBytes, final long seed) {
    checkValid();
    return XxHash64.hash(getUnsafeObject(), getCumulativeOffset(0) + offsetBytes, lengthBytes, seed);
  }

  @Override
  public final long xxHash64(final long in, final long seed) {
    return XxHash64.hash(in, seed);
  }

}