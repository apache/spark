package org.apache.spark.sql;


import java.math.BigDecimal;
import java.sql.Date;
import java.util.List;

import scala.collection.Seq;

import org.apache.spark.sql.types.StructType;

public abstract class BaseRow implements Row {

  @Override
  public int length() {
    return size();
  }

  @Override
  public boolean anyNull() {
    for (int i=0; i< size(); i++) {
      if (isNullAt(i)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public StructType schema() { throw new UnsupportedOperationException(); }

  @Override
  public Object apply(int i) {
    return get(i);
  }

  @Override
  public int getInt(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLong(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloat(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDouble(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte getByte(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public short getShort(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean getBoolean(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getString(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BigDecimal getDecimal(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Date getDate(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> Seq<T> getSeq(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> List<T> getList(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <K, V> scala.collection.Map<K, V> getMap(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> scala.collection.immutable.Map<String, T> getValuesMap(Seq<String> fieldNames) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <K, V> java.util.Map<K, V> getJavaMap(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Row getStruct(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T getAs(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T getAs(String fieldName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int fieldIndex(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Row copy() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Seq<Object> toSeq() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    return mkString("[", ",", "]");
  }

  @Override
  public String mkString() {
    return toSeq().mkString();
  }

  @Override
  public String mkString(String sep) {
    return toSeq().mkString(sep);
  }

  @Override
  public String mkString(String start, String sep, String end) {
    return toSeq().mkString(start, sep, end);
  }

}
