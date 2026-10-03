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
package test.org.apache.spark.sql;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Java records used by the Java record encoder tests.
 */
public class JavaRecordEncoderTestData {

  public record SimpleRecord(int id, String name, double value) implements Serializable {}

  public record BoxedRecord(Integer id, Long count, Double amount) {}

  public enum Color { RED, GREEN }

  public record LeafTypesRecord(boolean flag, BigDecimal dec, LocalDate date, Instant instant,
      Color color) {}

  // Not in alphabetical order: unlike JavaBean properties, record fields are not sorted by name.
  public record Address(String street, String city) {}

  public record Person(String name, int age, Address address) {}

  public record CollectionRecord(List<String> items, Set<Integer> numbers,
      List<Address> addresses, Map<String, Address> addressesByName) {}

  public record ArrayRecord(int[] ints, String[] strings, Address[] addresses) {}

  public record EmptyRecord() {}

  public record NonnullRecord(@Nonnull String name, @Nonnull Integer count, String note) {}

  // Only components become fields; getB() and CONSTANT are ignored.
  public record WithExtraMethods(int a) {
    public static final int CONSTANT = 1;

    public int getB() {
      return a + 1;
    }
  }

  public record GenericBox<T>(T value) {}

  public record NumberBox<T extends Number>(T value) {}

  public record ComparableBox<T extends Comparable<T>>(T value) {}

  public record BoxHolder(GenericBox<String> stringBox, GenericBox<Integer> intBox,
      List<GenericBox<String>> boxes) {}

  public record BoundedBoxHolder(NumberBox<Integer> numberBox,
      ComparableBox<String> comparableBox) {}

  @SuppressWarnings("rawtypes")
  public record RawBoxHolder(GenericBox box) {}

  public record SelfReferencingRecord(String value, SelfReferencingRecord next) {}

  public record ValidatedRecord(String name, int age) {
    public ValidatedRecord {
      if (age < 0) {
        throw new IllegalArgumentException("Age cannot be negative: " + age);
      }
    }
  }

  public static class SimpleBean {
    private String name;
    private int value;

    public SimpleBean() {}

    public SimpleBean(String name, int value) {
      this.name = name;
      this.value = value;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public int getValue() {
      return value;
    }

    public void setValue(int value) {
      this.value = value;
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof SimpleBean other &&
        value == other.value && java.util.Objects.equals(name, other.name);
    }

    @Override
    public int hashCode() {
      return java.util.Objects.hash(name, value);
    }
  }

  public record RecordWithBean(String id, SimpleBean bean) {}

  public static class BeanWithRecord {
    private Address address;

    public Address getAddress() {
      return address;
    }

    public void setAddress(Address address) {
      this.address = address;
    }
  }
}
