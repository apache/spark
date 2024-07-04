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
package org.apache.spark.sql.hive.test;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.hadoop.hive.serde2.thrift.test.IntString;
import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.EncodingUtils;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;

/**
 * This is a fork of Hive 0.13's org/apache/hadoop/hive/serde2/thrift/test/Complex.java, which
 * does not contain union fields that are not supported by Spark SQL.
 */

@SuppressWarnings("all")
public class Complex implements org.apache.thrift.TBase<Complex, Complex._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("Complex");

  private static final org.apache.thrift.protocol.TField AINT_FIELD_DESC = new org.apache.thrift.protocol.TField("aint", org.apache.thrift.protocol.TType.I32, (short)1);
  private static final org.apache.thrift.protocol.TField A_STRING_FIELD_DESC = new org.apache.thrift.protocol.TField("aString", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField LINT_FIELD_DESC = new org.apache.thrift.protocol.TField("lint", org.apache.thrift.protocol.TType.LIST, (short)3);
  private static final org.apache.thrift.protocol.TField L_STRING_FIELD_DESC = new org.apache.thrift.protocol.TField("lString", org.apache.thrift.protocol.TType.LIST, (short)4);
  private static final org.apache.thrift.protocol.TField LINT_STRING_FIELD_DESC = new org.apache.thrift.protocol.TField("lintString", org.apache.thrift.protocol.TType.LIST, (short)5);
  private static final org.apache.thrift.protocol.TField M_STRING_STRING_FIELD_DESC = new org.apache.thrift.protocol.TField("mStringString", org.apache.thrift.protocol.TType.MAP, (short)6);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<>();
  static {
    schemes.put(StandardScheme.class, new ComplexStandardSchemeFactory());
    schemes.put(TupleScheme.class, new ComplexTupleSchemeFactory());
  }

  private int aint; // required
  private String aString; // required
  private List<Integer> lint; // required
  private List<String> lString; // required
  private List<IntString> lintString; // required
  private Map<String,String> mStringString; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    AINT((short)1, "aint"),
    A_STRING((short)2, "aString"),
    LINT((short)3, "lint"),
    L_STRING((short)4, "lString"),
    LINT_STRING((short)5, "lintString"),
    M_STRING_STRING((short)6, "mStringString");

    private static final Map<String, _Fields> byName = new HashMap<>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // AINT
          return AINT;
        case 2: // A_STRING
          return A_STRING;
        case 3: // LINT
          return LINT;
        case 4: // L_STRING
          return L_STRING;
        case 5: // LINT_STRING
          return LINT_STRING;
        case 6: // M_STRING_STRING
          return M_STRING_STRING;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __AINT_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<>(_Fields.class);
    tmpMap.put(_Fields.AINT, new org.apache.thrift.meta_data.FieldMetaData("aint", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.A_STRING, new org.apache.thrift.meta_data.FieldMetaData("aString", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.LINT, new org.apache.thrift.meta_data.FieldMetaData("lint", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST,
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32))));
    tmpMap.put(_Fields.L_STRING, new org.apache.thrift.meta_data.FieldMetaData("lString", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST,
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.LINT_STRING, new org.apache.thrift.meta_data.FieldMetaData("lintString", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST,
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, IntString.class))));
    tmpMap.put(_Fields.M_STRING_STRING, new org.apache.thrift.meta_data.FieldMetaData("mStringString", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP,
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING),
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(Complex.class, metaDataMap);
  }

  public Complex() {
  }

  public Complex(
    int aint,
    String aString,
    List<Integer> lint,
    List<String> lString,
    List<IntString> lintString,
    Map<String,String> mStringString)
  {
    this();
    this.aint = aint;
    setAintIsSet(true);
    this.aString = aString;
    this.lint = lint;
    this.lString = lString;
    this.lintString = lintString;
    this.mStringString = mStringString;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public Complex(Complex other) {
    __isset_bitfield = other.__isset_bitfield;
    this.aint = other.aint;
    if (other.isSetAString()) {
      this.aString = other.aString;
    }
    if (other.isSetLint()) {
      List<Integer> __this__lint = new ArrayList<>();
      for (Integer other_element : other.lint) {
        __this__lint.add(other_element);
      }
      this.lint = __this__lint;
    }
    if (other.isSetLString()) {
      List<String> __this__lString = new ArrayList<>();
      for (String other_element : other.lString) {
        __this__lString.add(other_element);
      }
      this.lString = __this__lString;
    }
    if (other.isSetLintString()) {
      List<IntString> __this__lintString = new ArrayList<>();
      for (IntString other_element : other.lintString) {
        __this__lintString.add(new IntString(other_element));
      }
      this.lintString = __this__lintString;
    }
    if (other.isSetMStringString()) {
      Map<String,String> __this__mStringString = new HashMap<>();
      for (Map.Entry<String, String> other_element : other.mStringString.entrySet()) {

        String other_element_key = other_element.getKey();
        String other_element_value = other_element.getValue();

        String __this__mStringString_copy_key = other_element_key;

        String __this__mStringString_copy_value = other_element_value;

        __this__mStringString.put(__this__mStringString_copy_key, __this__mStringString_copy_value);
      }
      this.mStringString = __this__mStringString;
    }
  }

  public Complex deepCopy() {
    return new Complex(this);
  }

  @Override
  public void clear() {
    setAintIsSet(false);
    this.aint = 0;
    this.aString = null;
    this.lint = null;
    this.lString = null;
    this.lintString = null;
    this.mStringString = null;
  }

  public int getAint() {
    return this.aint;
  }

  public void setAint(int aint) {
    this.aint = aint;
    setAintIsSet(true);
  }

  public void unsetAint() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __AINT_ISSET_ID);
  }

  /** Returns true if field aint is set (has been assigned a value) and false otherwise */
  public boolean isSetAint() {
    return EncodingUtils.testBit(__isset_bitfield, __AINT_ISSET_ID);
  }

  public void setAintIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __AINT_ISSET_ID, value);
  }

  public String getAString() {
    return this.aString;
  }

  public void setAString(String aString) {
    this.aString = aString;
  }

  public void unsetAString() {
    this.aString = null;
  }

  /** Returns true if field aString is set (has been assigned a value) and false otherwise */
  public boolean isSetAString() {
    return this.aString != null;
  }

  public void setAStringIsSet(boolean value) {
    if (!value) {
      this.aString = null;
    }
  }

  public int getLintSize() {
    return (this.lint == null) ? 0 : this.lint.size();
  }

  public java.util.Iterator<Integer> getLintIterator() {
    return (this.lint == null) ? null : this.lint.iterator();
  }

  public void addToLint(int elem) {
    if (this.lint == null) {
      this.lint = new ArrayList<>();
    }
    this.lint.add(elem);
  }

  public List<Integer> getLint() {
    return this.lint;
  }

  public void setLint(List<Integer> lint) {
    this.lint = lint;
  }

  public void unsetLint() {
    this.lint = null;
  }

  /** Returns true if field lint is set (has been assigned a value) and false otherwise */
  public boolean isSetLint() {
    return this.lint != null;
  }

  public void setLintIsSet(boolean value) {
    if (!value) {
      this.lint = null;
    }
  }

  public int getLStringSize() {
    return (this.lString == null) ? 0 : this.lString.size();
  }

  public java.util.Iterator<String> getLStringIterator() {
    return (this.lString == null) ? null : this.lString.iterator();
  }

  public void addToLString(String elem) {
    if (this.lString == null) {
      this.lString = new ArrayList<>();
    }
    this.lString.add(elem);
  }

  public List<String> getLString() {
    return this.lString;
  }

  public void setLString(List<String> lString) {
    this.lString = lString;
  }

  public void unsetLString() {
    this.lString = null;
  }

  /** Returns true if field lString is set (has been assigned a value) and false otherwise */
  public boolean isSetLString() {
    return this.lString != null;
  }

  public void setLStringIsSet(boolean value) {
    if (!value) {
      this.lString = null;
    }
  }

  public int getLintStringSize() {
    return (this.lintString == null) ? 0 : this.lintString.size();
  }

  public java.util.Iterator<IntString> getLintStringIterator() {
    return (this.lintString == null) ? null : this.lintString.iterator();
  }

  public void addToLintString(IntString elem) {
    if (this.lintString == null) {
      this.lintString = new ArrayList<>();
    }
    this.lintString.add(elem);
  }

  public List<IntString> getLintString() {
    return this.lintString;
  }

  public void setLintString(List<IntString> lintString) {
    this.lintString = lintString;
  }

  public void unsetLintString() {
    this.lintString = null;
  }

  /** Returns true if field lintString is set (has been assigned a value) and false otherwise */
  public boolean isSetLintString() {
    return this.lintString != null;
  }

  public void setLintStringIsSet(boolean value) {
    if (!value) {
      this.lintString = null;
    }
  }

  public int getMStringStringSize() {
    return (this.mStringString == null) ? 0 : this.mStringString.size();
  }

  public void putToMStringString(String key, String val) {
    if (this.mStringString == null) {
      this.mStringString = new HashMap<>();
    }
    this.mStringString.put(key, val);
  }

  public Map<String,String> getMStringString() {
    return this.mStringString;
  }

  public void setMStringString(Map<String,String> mStringString) {
    this.mStringString = mStringString;
  }

  public void unsetMStringString() {
    this.mStringString = null;
  }

  /** Returns true if field mStringString is set (has been assigned a value) and false otherwise */
  public boolean isSetMStringString() {
    return this.mStringString != null;
  }

  public void setMStringStringIsSet(boolean value) {
    if (!value) {
      this.mStringString = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case AINT:
      if (value == null) {
        unsetAint();
      } else {
        setAint((Integer)value);
      }
      break;

    case A_STRING:
      if (value == null) {
        unsetAString();
      } else {
        setAString((String)value);
      }
      break;

    case LINT:
      if (value == null) {
        unsetLint();
      } else {
        setLint((List<Integer>)value);
      }
      break;

    case L_STRING:
      if (value == null) {
        unsetLString();
      } else {
        setLString((List<String>)value);
      }
      break;

    case LINT_STRING:
      if (value == null) {
        unsetLintString();
      } else {
        setLintString((List<IntString>)value);
      }
      break;

    case M_STRING_STRING:
      if (value == null) {
        unsetMStringString();
      } else {
        setMStringString((Map<String,String>)value);
      }
      break;

    default:
    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case AINT:
      return Integer.valueOf(getAint());

    case A_STRING:
      return getAString();

    case LINT:
      return getLint();

    case L_STRING:
      return getLString();

    case LINT_STRING:
      return getLintString();

    case M_STRING_STRING:
      return getMStringString();

    default:
    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case AINT:
      return isSetAint();
    case A_STRING:
      return isSetAString();
    case LINT:
      return isSetLint();
    case L_STRING:
      return isSetLString();
    case LINT_STRING:
      return isSetLintString();
    case M_STRING_STRING:
      return isSetMStringString();
    default:
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null) {
      return false;
    }
    if (that instanceof Complex) {
      return this.equals((Complex) that);
    }
    return false;
  }

  public boolean equals(Complex that) {
    if (that == null) {
      return false;
    }

    boolean this_present_aint = true;
    boolean that_present_aint = true;
    if (this_present_aint || that_present_aint) {
      if (!(this_present_aint && that_present_aint)) {
        return false;
      }
      if (this.aint != that.aint) {
        return false;
      }
    }

    boolean this_present_aString = true && this.isSetAString();
    boolean that_present_aString = true && that.isSetAString();
    if (this_present_aString || that_present_aString) {
      if (!(this_present_aString && that_present_aString)) {
        return false;
      }
      if (!this.aString.equals(that.aString)) {
        return false;
      }
    }

    boolean this_present_lint = true && this.isSetLint();
    boolean that_present_lint = true && that.isSetLint();
    if (this_present_lint || that_present_lint) {
      if (!(this_present_lint && that_present_lint)) {
        return false;
      }
      if (!this.lint.equals(that.lint)) {
        return false;
      }
    }

    boolean this_present_lString = true && this.isSetLString();
    boolean that_present_lString = true && that.isSetLString();
    if (this_present_lString || that_present_lString) {
      if (!(this_present_lString && that_present_lString)) {
        return false;
      }
      if (!this.lString.equals(that.lString)) {
        return false;
      }
    }

    boolean this_present_lintString = true && this.isSetLintString();
    boolean that_present_lintString = true && that.isSetLintString();
    if (this_present_lintString || that_present_lintString) {
      if (!(this_present_lintString && that_present_lintString)) {
        return false;
      }
      if (!this.lintString.equals(that.lintString)) {
        return false;
      }
    }

    boolean this_present_mStringString = true && this.isSetMStringString();
    boolean that_present_mStringString = true && that.isSetMStringString();
    if (this_present_mStringString || that_present_mStringString) {
      if (!(this_present_mStringString && that_present_mStringString)) {
        return false;
      }
      if (!this.mStringString.equals(that.mStringString)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    boolean present_aint = true;
    builder.append(present_aint);
    if (present_aint) {
      builder.append(aint);
    }

    boolean present_aString = true && (isSetAString());
    builder.append(present_aString);
    if (present_aString) {
      builder.append(aString);
    }

    boolean present_lint = true && (isSetLint());
    builder.append(present_lint);
    if (present_lint) {
      builder.append(lint);
    }

    boolean present_lString = true && (isSetLString());
    builder.append(present_lString);
    if (present_lString) {
      builder.append(lString);
    }

    boolean present_lintString = true && (isSetLintString());
    builder.append(present_lintString);
    if (present_lintString) {
      builder.append(lintString);
    }

    boolean present_mStringString = true && (isSetMStringString());
    builder.append(present_mStringString);
    if (present_mStringString) {
      builder.append(mStringString);
    }

    return builder.toHashCode();
  }

  public int compareTo(Complex other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    Complex typedOther = other;

    lastComparison = Boolean.valueOf(isSetAint()).compareTo(typedOther.isSetAint());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetAint()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.aint, typedOther.aint);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetAString()).compareTo(typedOther.isSetAString());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetAString()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.aString, typedOther.aString);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetLint()).compareTo(typedOther.isSetLint());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLint()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.lint, typedOther.lint);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetLString()).compareTo(typedOther.isSetLString());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLString()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.lString, typedOther.lString);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetLintString()).compareTo(typedOther.isSetLintString());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLintString()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.lintString, typedOther.lintString);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetMStringString()).compareTo(typedOther.isSetMStringString());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMStringString()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.mStringString, typedOther.mStringString);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Complex(");
    boolean first = true;

    sb.append("aint:");
    sb.append(this.aint);
    first = false;
    if (!first) {
      sb.append(", ");
    }
    sb.append("aString:");
    if (this.aString == null) {
      sb.append("null");
    } else {
      sb.append(this.aString);
    }
    first = false;
    if (!first) {
      sb.append(", ");
    }
    sb.append("lint:");
    if (this.lint == null) {
      sb.append("null");
    } else {
      sb.append(this.lint);
    }
    first = false;
    if (!first) {
      sb.append(", ");
    }
    sb.append("lString:");
    if (this.lString == null) {
      sb.append("null");
    } else {
      sb.append(this.lString);
    }
    first = false;
    if (!first) {
      sb.append(", ");
    }
    sb.append("lintString:");
    if (this.lintString == null) {
      sb.append("null");
    } else {
      sb.append(this.lintString);
    }
    first = false;
    if (!first) {
      sb.append(", ");
    }
    sb.append("mStringString:");
    if (this.mStringString == null) {
      sb.append("null");
    } else {
      sb.append(this.mStringString);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class ComplexStandardSchemeFactory implements SchemeFactory {
    public ComplexStandardScheme getScheme() {
      return new ComplexStandardScheme();
    }
  }

  private static class ComplexStandardScheme extends StandardScheme<Complex> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, Complex struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) {
          break;
        }
        switch (schemeField.id) {
          case 1: // AINT
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.aint = iprot.readI32();
              struct.setAintIsSet(true);
            } else {
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // A_STRING
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.aString = iprot.readString();
              struct.setAStringIsSet(true);
            } else {
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // LINT
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list0 = iprot.readListBegin();
                struct.lint = new ArrayList<>(_list0.size);
                for (int _i1 = 0; _i1 < _list0.size; ++_i1)
                {
                  int _elem2; // required
                  _elem2 = iprot.readI32();
                  struct.lint.add(_elem2);
                }
                iprot.readListEnd();
              }
              struct.setLintIsSet(true);
            } else {
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // L_STRING
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list3 = iprot.readListBegin();
                struct.lString = new ArrayList<>(_list3.size);
                for (int _i4 = 0; _i4 < _list3.size; ++_i4)
                {
                  String _elem5; // required
                  _elem5 = iprot.readString();
                  struct.lString.add(_elem5);
                }
                iprot.readListEnd();
              }
              struct.setLStringIsSet(true);
            } else {
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // LINT_STRING
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list6 = iprot.readListBegin();
                struct.lintString = new ArrayList<>(_list6.size);
                for (int _i7 = 0; _i7 < _list6.size; ++_i7)
                {
                  IntString _elem8; // required
                  _elem8 = new IntString();
                  _elem8.read(iprot);
                  struct.lintString.add(_elem8);
                }
                iprot.readListEnd();
              }
              struct.setLintStringIsSet(true);
            } else {
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 6: // M_STRING_STRING
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map9 = iprot.readMapBegin();
                struct.mStringString = new HashMap<String,String>(2*_map9.size);
                for (int _i10 = 0; _i10 < _map9.size; ++_i10)
                {
                  String _key11; // required
                  String _val12; // required
                  _key11 = iprot.readString();
                  _val12 = iprot.readString();
                  struct.mStringString.put(_key11, _val12);
                }
                iprot.readMapEnd();
              }
              struct.setMStringStringIsSet(true);
            } else {
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, Complex struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(AINT_FIELD_DESC);
      oprot.writeI32(struct.aint);
      oprot.writeFieldEnd();
      if (struct.aString != null) {
        oprot.writeFieldBegin(A_STRING_FIELD_DESC);
        oprot.writeString(struct.aString);
        oprot.writeFieldEnd();
      }
      if (struct.lint != null) {
        oprot.writeFieldBegin(LINT_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.I32, struct.lint.size()));
          for (int _iter13 : struct.lint)
          {
            oprot.writeI32(_iter13);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.lString != null) {
        oprot.writeFieldBegin(L_STRING_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, struct.lString.size()));
          for (String _iter14 : struct.lString)
          {
            oprot.writeString(_iter14);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.lintString != null) {
        oprot.writeFieldBegin(LINT_STRING_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.lintString.size()));
          for (IntString _iter15 : struct.lintString)
          {
            _iter15.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.mStringString != null) {
        oprot.writeFieldBegin(M_STRING_STRING_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, struct.mStringString.size()));
          for (Map.Entry<String, String> _iter16 : struct.mStringString.entrySet())
          {
            oprot.writeString(_iter16.getKey());
            oprot.writeString(_iter16.getValue());
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ComplexTupleSchemeFactory implements SchemeFactory {
    public ComplexTupleScheme getScheme() {
      return new ComplexTupleScheme();
    }
  }

  private static class ComplexTupleScheme extends TupleScheme<Complex> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, Complex struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetAint()) {
        optionals.set(0);
      }
      if (struct.isSetAString()) {
        optionals.set(1);
      }
      if (struct.isSetLint()) {
        optionals.set(2);
      }
      if (struct.isSetLString()) {
        optionals.set(3);
      }
      if (struct.isSetLintString()) {
        optionals.set(4);
      }
      if (struct.isSetMStringString()) {
        optionals.set(5);
      }
      oprot.writeBitSet(optionals, 6);
      if (struct.isSetAint()) {
        oprot.writeI32(struct.aint);
      }
      if (struct.isSetAString()) {
        oprot.writeString(struct.aString);
      }
      if (struct.isSetLint()) {
        {
          oprot.writeI32(struct.lint.size());
          for (int _iter17 : struct.lint)
          {
            oprot.writeI32(_iter17);
          }
        }
      }
      if (struct.isSetLString()) {
        {
          oprot.writeI32(struct.lString.size());
          for (String _iter18 : struct.lString)
          {
            oprot.writeString(_iter18);
          }
        }
      }
      if (struct.isSetLintString()) {
        {
          oprot.writeI32(struct.lintString.size());
          for (IntString _iter19 : struct.lintString)
          {
            _iter19.write(oprot);
          }
        }
      }
      if (struct.isSetMStringString()) {
        {
          oprot.writeI32(struct.mStringString.size());
          for (Map.Entry<String, String> _iter20 : struct.mStringString.entrySet())
          {
            oprot.writeString(_iter20.getKey());
            oprot.writeString(_iter20.getValue());
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, Complex struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(6);
      if (incoming.get(0)) {
        struct.aint = iprot.readI32();
        struct.setAintIsSet(true);
      }
      if (incoming.get(1)) {
        struct.aString = iprot.readString();
        struct.setAStringIsSet(true);
      }
      if (incoming.get(2)) {
        {
          org.apache.thrift.protocol.TList _list21 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.I32, iprot.readI32());
          struct.lint = new ArrayList<>(_list21.size);
          for (int _i22 = 0; _i22 < _list21.size; ++_i22)
          {
            int _elem23; // required
            _elem23 = iprot.readI32();
            struct.lint.add(_elem23);
          }
        }
        struct.setLintIsSet(true);
      }
      if (incoming.get(3)) {
        {
          org.apache.thrift.protocol.TList _list24 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.lString = new ArrayList<>(_list24.size);
          for (int _i25 = 0; _i25 < _list24.size; ++_i25)
          {
            String _elem26; // required
            _elem26 = iprot.readString();
            struct.lString.add(_elem26);
          }
        }
        struct.setLStringIsSet(true);
      }
      if (incoming.get(4)) {
        {
          org.apache.thrift.protocol.TList _list27 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.lintString = new ArrayList<>(_list27.size);
          for (int _i28 = 0; _i28 < _list27.size; ++_i28)
          {
            IntString _elem29; // required
            _elem29 = new IntString();
            _elem29.read(iprot);
            struct.lintString.add(_elem29);
          }
        }
        struct.setLintStringIsSet(true);
      }
      if (incoming.get(5)) {
        {
          org.apache.thrift.protocol.TMap _map30 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.mStringString = new HashMap<>(2*_map30.size);
          for (int _i31 = 0; _i31 < _map30.size; ++_i31)
          {
            String _key32; // required
            String _val33; // required
            _key32 = iprot.readString();
            _val33 = iprot.readString();
            struct.mStringString.put(_key32, _val33);
          }
        }
        struct.setMStringStringIsSet(true);
      }
    }
  }

}

