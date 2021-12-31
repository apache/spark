package org.apache.spark.sql.execution.vectorized;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * This class adds the constant support to ColumnVector.
 * It supports all the types and contains put APIs,
 * which will put the exact same value to all rows.
 *
 * Capacity: The vector only stores one copy of the data, and acts as an unbounded vector
 * (get from any row will return the same value)
 */
public class ConstantColumnVector extends ColumnVector {

  private byte nullData;
  private byte byteData;
  private short shortData;
  private int intData;
  private long longData;
  private float floatData;
  private double doubleData;
  private byte[] byteArrayData;
  private ConstantColumnVector childData;
  private ColumnarArray arrayData;
  private ColumnarMap mapData;

  /**
   * Sets up the data type of this constant column vector.
   * @param type
   */
  public ConstantColumnVector(DataType type) {
    super(type);
  }

  @Override
  public void close() {
    byteArrayData = null;
    childData = null;
    arrayData = null;
    mapData = null;
  }

  @Override
  public boolean hasNull() {
    return nullData == 1;
  }

  @Override
  public int numNulls() {
    return -1;
  }

  @Override
  public boolean isNullAt(int rowId) {
    return nullData == 1;
  }

  public void putNull() {
    nullData = (byte) 1;
  }

  public void putNotNull() {
    nullData = (byte) 0;
  }

  @Override
  public boolean getBoolean(int rowId) {
    return byteData == 1;
  }

  public void putBoolean(boolean value) {
    byteData = (byte) ((value) ? 1 : 0);
  }

  @Override
  public byte getByte(int rowId) {
    return byteData;
  }

  public void putByte(byte value) {
    byteData = value;
  }

  @Override
  public short getShort(int rowId) {
    return shortData;
  }

  public void putShort(short value) {
    shortData = value;
  }

  @Override
  public int getInt(int rowId) {
    return intData;
  }

  public void putInt(int value) {
    intData = value;
  }

  @Override
  public long getLong(int rowId) {
    return longData;
  }

  public void putLong(long value) {
    longData = value;
  }

  @Override
  public float getFloat(int rowId) {
    return floatData;
  }

  public void putFloat(float value) {
    floatData = value;
  }

  @Override
  public double getDouble(int rowId) {
    return doubleData;
  }

  public void putDouble(double value) {
    doubleData = value;
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    return arrayData;
  }

  public void putArray(ColumnarArray value) {
    arrayData = value;
  }

  @Override
  public ColumnarMap getMap(int ordinal) {
    return mapData;
  }

  public void putMap(ColumnarMap value) {
    mapData = value;
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    // copy and modify from WritableColumnVector
    if (precision <= Decimal.MAX_INT_DIGITS()) {
      return Decimal.createUnsafe(getInt(rowId), precision, scale);
    } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
      return Decimal.createUnsafe(getLong(rowId), precision, scale);
    } else {
      byte[] bytes = getBinary(rowId);
      BigInteger bigInteger = new BigInteger(bytes);
      BigDecimal javaDecimal = new BigDecimal(bigInteger, scale);
      return Decimal.apply(javaDecimal, precision, scale);
    }
  }

  public void putDecimal(Decimal value, int precision) {
    // copy and modify from WritableColumnVector
    if (precision <= Decimal.MAX_INT_DIGITS()) {
      putInt((int) value.toUnscaledLong());
    } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
      putLong(value.toUnscaledLong());
    } else {
      BigInteger bigInteger = value.toJavaBigDecimal().unscaledValue();
      putByteArray(bigInteger.toByteArray());
    }
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    return UTF8String.fromBytes(byteArrayData);
  }

  public void putUtf8String(UTF8String value) {
    putByteArray(value.getBytes());
  }

  private void putByteArray(byte[] value) {
    byteArrayData =  value;
  }

  @Override
  public byte[] getBinary(int rowId) {
    return byteArrayData;
  }

  public void putBinary(byte[] value) {
    putByteArray(value);
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    return childData;
  }

  public void putChild(ConstantColumnVector value) {
    childData = value;
  }
}
