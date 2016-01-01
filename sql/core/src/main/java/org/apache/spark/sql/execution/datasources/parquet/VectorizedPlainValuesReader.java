package org.apache.spark.sql.execution.datasources.parquet;

import java.io.IOException;

import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.unsafe.Platform;

import org.apache.parquet.column.values.ValuesReader;

/**
 * An implementation of the Parquet PLAIN decoder that supports the vectorized interface.
 */
public class VectorizedPlainValuesReader extends ValuesReader implements VectorizedValuesReader {
  private byte[] buffer;
  private int offset;
  private final int byteSize;

  public VectorizedPlainValuesReader(int byteSize) {
    this.byteSize = byteSize;
  }

  @Override
  public void initFromPage(int valueCount, byte[] bytes, int offset) throws IOException {
    this.buffer = bytes;
    this.offset = offset + Platform.BYTE_ARRAY_OFFSET;
  }

  @Override
  public void skip() {
    offset += byteSize;
  }

  @Override
  public void skip(int n) {
    offset += n * byteSize;
  }

  @Override
  public void readIntegers(int total, ColumnVector c, int rowId) {
    c.putIntsLittleEndian(rowId, total, buffer, offset - Platform.BYTE_ARRAY_OFFSET);
    offset += 4 * total;
  }

  @Override
  public int readInteger() {
    int v = Platform.getInt(buffer, offset);
    offset += 4;
    return v;
  }
}