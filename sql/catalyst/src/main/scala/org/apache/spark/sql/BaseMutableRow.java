package org.apache.spark.sql;

import org.apache.spark.sql.catalyst.expressions.MutableRow;


public abstract class BaseMutableRow extends BaseRow implements MutableRow {

  @Override
  public void update(int ordinal, Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setInt(int ordinal, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setLong(int ordinal, long value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setDouble(int ordinal, double value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBoolean(int ordinal, boolean value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setShort(int ordinal, short value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setByte(int ordinal, byte value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setFloat(int ordinal, float value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setString(int ordinal, String value) {
    throw new UnsupportedOperationException();
  }
}
