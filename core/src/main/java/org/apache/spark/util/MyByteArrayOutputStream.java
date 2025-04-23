package org.apache.spark.util;

import java.io.ByteArrayOutputStream;

/** Subclass of ByteArrayOutputStream that exposes `buf` directly. */
public final class MyByteArrayOutputStream extends ByteArrayOutputStream {
    public MyByteArrayOutputStream(int size) { super(size); }
    public byte[] getBuf() { return buf; }
}
