package org.apache.spark.sql.catalyst.expressions;

/**
 *
 */
public class GenericBean {
  private int field1;
  private String field2;

  public GenericBean() {}

  public GenericBean(int field1, String field2) {
    this.field1 = field1;
    this.field2 = field2;
  }

  public int getField1() {
    return field1;
  }

  public void setField1(int field1) {
    this.field1 = field1;
  }

  public String getField2() {
    return field2;
  }

  public void setField2(String field2) {
    this.field2 = field2;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    GenericBean that = (GenericBean) o;

    if (field1 != that.field1) {
      return false;
    }
    return field2 != null ? field2.equals(that.field2) : that.field2 == null;
  }

  @Override
  public int hashCode() {
    int result = field1;
    result = 31 * result + (field2 != null ? field2.hashCode() : 0);
    return result;
  }
}
