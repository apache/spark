# Shredding Overview

The Spark Variant type is designed to store and process semi-structured data efficiently, even with heterogeneous values. Query engines encode each variant value in a self-describing format, and store it as a group containing **value** and **metadata** binary fields in Parquet. Since data is often partially homogenous, it can be beneficial to extract certain fields into separate Parquet columns to further improve performance. We refer to this process as "shredding". Each Parquet file remains fully self-describing, with no additional metadata required to read or fully reconstruct the Variant data from the file. Combining shredding with a binary residual provides the flexibility to represent complex, evolving data with an unbounded number of unique fields while limiting the size of file schemas, and retaining the performance benefits of a columnar format.

This document focuses on the shredding semantics, Parquet representation, implications for readers and writers, as well as the Variant reconstruction. For now, it does not discuss which fields to shred, user-facing API changes, or any engine-specific considerations like how to use shredded columns. The approach builds on top of the generic Spark Variant representation, and leverages the existing Parquet specification for maximum compatibility with the open-source ecosystem.

At a high level, we replace the **value** and **metadata** of the Variant Parquet group with one or more fields called **object**, **array**, **typed_value** and **untyped_value**. These represent a fixed schema suitable for constructing the full Variant value for each row.

Shredding lets Spark (or any other query engine) reap the full benefits of Parquet's columnar representation, such as more compact data encoding, min/max statistics for data skipping, and I/O and CPU savings from pruning unnecessary fields not accessed by a query (including the non-shredded Variant binary data).
Without shredding, any query that accesses a Variant column must fetch all bytes of the full binary buffer. With shredding, we can get nearly equivalent performance as in a relational (scalar) data model.

For example, `select variant_get(variant_col, ‘$.field1.inner_field2’, ‘string’) from tbl` only needs to access `inner_field2`, and the file scan could avoid fetching the rest of the Variant value if this field was shredded into a separate column in the Parquet schema. Similarly, for the query `select * from tbl where variant_get(variant_col, ‘$.id’, ‘integer’) = 123`, the scan could first decode the shredded `id` column, and only fetch/decode the full Variant value for rows that pass the filter.

# Parquet Example

Consider the following Parquet schema together with how Variant values might be mapped to it. Notice that we represent each shredded field in **object** as a group of two fields, **typed_value** and **untyped_value**. We extract all homogenous data items of a certain path into **typed_value**, and set aside incompatible data items in **untyped_value**. Intuitively, incompatibilities within the same path may occur because we store the shredding schema per Parquet file, and each file can contain several row groups. Selecting a type for each field that is acceptable for all rows would be impractical because it would require buffering the contents of an entire file before writing.

Typically, the expectation is that **untyped_value** exists at every level as an option, along with one of **object**, **array** or **typed_value**. If the actual Variant value contains a type that does not match the provided schema, it is stored in **untyped_value**. An **untyped_value** may also be populated if an object can be partially represented: any fields that are present in the schema must be written to those fields, and any missing fields are written to **untyped_valud**.

```
optional group variant_col {
 optional binary untyped_value;
 optional group object {
  optional group a {
   optional binary untyped_value;
   optional int64 typed_value;
  }
  optional group b {
   optional binary untyped_value;
   optional group object {
    optional group c {
      optional binary untyped_value;
      optional binary typed_value (STRING);
    }
   }
  }
 }
}
```

| Variant Value | Top-level untyped_value | b.untyped_value | Non-null in a | Non-null in b.c |
|---------------|--------------------------|---------------|---------------|
| {a: 123, b: {c: “hello”}} | null | null | typed_value | typed_value |
| {a: 1.23, b: {c: “123”}} | null | null | untyped_value | typed_value |
| {a: [1,2,3], b: {c: null}} | null | null | untyped_value | untyped_value |
| {a: 123, c: 456} | {c: 456} | null | typed_value | null |
| {a: 123, b: {c: "hello", d: 456}} | null | {d: 456} | typed_value | typed_value |
| [{a: 1, b: {c: 2}}, {a: 3, b: {c: 4}}] | [{a: 1, b: {c: 2}}, {a: 3, b: {c: 4}}] | null | null | null |

# Parquet Layout

The **array** and **object** fields represent Variant array and object types, respectively. Arrays must use the three-level list structure described in https://github.com/apache/parquet-format/blob/master/LogicalTypes.md.

An **object** field must be a group. Each field name of this inner group corresponds to the Variant value's object field name. Each inner field's type is a recursively shredded variant value: that is, the fields of each object field must be one or more of **object**, **array**, **typed_value** or **untyped_value**.

Similarly the elements of an **array** must be a group containing one or more of **object**, **array**, **typed_value** or **untyped_value**.

Each leaf in the schema can store an arbitrary Variant value. It contains an **untyped_value** binary field and a **typed_value** field. If non-null, **untyped_value** represents the value stored as a Variant binary; the metadata and value of a normal Variant are concatenated. The **typed_value** field may be any type that has a corresponding Variant type. For each value in the data, at most one of the **typed_value** and **untyped_value** may be non-null. A writer may omit either field, which is equivalent to all rows being null.

| typed_value | untyped_value | Meaning |
|-------------|----------------|---------|
| null | null | Field is missing in the reconstructed Variant. |
| null | non-null | Field may be any type in the reconstructed Variant. |
| non-null | null | Field has this column’s type in the reconstructed Variant. |
| non-null | non-null | Invalid |

The **typed_value** may be absent from the Parquet schema for any field, which is equivalent to its value being always null (in which case the shredded field is always stored as a Variant binary). By the same token, **untyped_value** may be absent, which is equivalent to their value being always null (in which case the field will always be missing or have the type of the **typed_value** column).

The full metadata and value can be reconstructed from **untyped_value** by treating the leading bytes as metadata, and using the header, dictionary size and final dictionary offset to determine the start of the Variant value section. (See the metadata description in the common/variant/README.md for more detail on how to interpret it.) For example, in the binary below, there is a one-element dictionary, and the final offset (`offset[1]`) indicates that the last dictionary entry ends at the second byte. Therefore the full metadata size is six bytes, and the rest is the value section of the Variant.

```
   hdr    sz   offset[0] offset[1] bytes[0] bytes[1]  value
 --------------------------------------------------------------------
|      |      |         |         |        |        |
| 0x01 | 0x01 | 0x00    | 0x02    | ‘h’    | ‘i’    | . . . . . . . .
|______|______|_________|_________|________|________|________________
```

# Unshredded values

If all values can be represented at a given level by whichever of **object**, **array** or **typed_value** is present, **untyped_value** is set to null.

If a value cannot be represented by whichever of **object**, **array** or **typed_value** is present in the schema, then it is stored in **untyped_value**, and the other fields are set to null. In the Parquet example above, if field **a** was an object or array, or a non-integer scalar, it would be stored in **untyped_value**.

If a value is an object, and the **object** field is present but does not contain all of the fields in the value, then any remaining fields are stored in an object in **untyped_value**. In the Parquet example above, if field **b** was an object of the form **{"c": 1, "d": 2}"**, then the object **{"d": 2}** would be stored in **untyped_value**, and the **c** field would be shredded recursively under **object.c**.

Note that an array is always fully shredded if there is an **array** field, so the above consideration for **object** is not relevant for arrays: only one of **array** or **untyped_value** may be non-null at a given level.

# Using untyped_value vs. typed_value

In general, it is desirable to store values in the **typed_value** field rather than the **untyped_value** whenever possible. This will typically improve encoding efficiency, and allow the use of Parquet statistics to filter at the row group or page level. In the best case, the **untyped_value** fields are all null and the engine does not need to read them (or it can omit them from the schema on write entirely). There are two main motivations for including the **untyped_value** column:

1) In a case where there are rare type mismatches (for example, a numeric field with rare strings like “n/a”), we allow the field to be shredded, which could still be a significant performance benefit compared to fetching and decoding the full value/metadata binary.
2) Since there is a single schema per file, there would be no easy way to recover from a type mismatch encountered late in a file write. Parquet files can be large, and buffering all file data before starting to write could be expensive. Including an untyped column for every field guarantees we can adhere to the requested shredding schema.

The **untyped_value** is stored in a single binary column, rather than storing the value and metadata separately as is done in the unshredded binary format. The motivation for storing them separately for unshredded data is that this lets the engine encode and compress the metadata more efficiently when the fields are consistent across rows. We chose to combine them in the shredded fields: we expect the encoding/compression benefit to be lower, since in the case of uniform data, the values should be stored in typed columns. Using a single binary reduces the extra metadata required in the Parquet schema, which can be significant in some cases.

# Data Skipping

Shredded columns are expected to store statistics in the same format as a normal Parquet column. In general, the engine can only skip a row group or page if all rows in the **untyped_value** field are null, since it is possible for a `variant_get` expression to successfully cast a value from the **untyped_value** to the target type. For example, if **typed_value** is of type `int64`, then the string “123” might be contained in **untyped_value**, which would not be reflected in statistics, but could be retained by a filter like `where variant_get(col, “$.field”, “long”) = 123`. If **untyped_value** is all-null, then the engine can prune pages or row groups based on **typed_value**. This specification is not strict about what values may be stored in **untyped_value** rather than **typed_value**, so it is not safe to skip rows based on **typed_value** unless the corresponding **untyped_value** column is all-null, or the engine has specific knowledge of the behavior of the writer that produced the shredded data.

# Shredding Semantics

Variant defines a number of integer and decimal types of varying widths. When writing, it would be quite limiting to strictly enforce the mapping between Variant types and Parquet/Spark types. For example, if we chose to shred a field as `int64`, and encountered the value 123 encoded as `int32`, it seems preferable to write this to the **typed_value** column, even though it technically loses information about the type in the original Variant object, and would be reconstructed as an `int64`.

On the other hand, storing arbitrarily casted values in the **typed_value** column could create inconsistent behavior before and after shredding, and could leak behavior from the writing engine to the reading engine. For example, double-to-string casts can produce different results in different engines. Performing such a cast while shredding (even if we somehow retained the knowledge that the original value was a `double`) could result in confusing behavior changes if shredding took place using a different tool from the query engine that wrote it.

Our approach is a pragmatic compromise that allows the use of **typed_value** in cases where the type can be losslessly widened without resulting in a significant difference in the reconstructed Variant:

1) All integer and decimal types in Variant are conceptually a single “number” type. The engine may shred any number into the **typed_value** of any other number, provided that no information about the value is lost. For example, the integer 123 may be shredded as Decimal<9, 2>, but 1.23 may not be shredded as any integer type.

2) To ensure that behavior remains unchanged before and after shredding, we will aim to have all Spark expressions that operate on Variant be agnostic to the specific numeric type. For example, `cast(val as string)` should produce the string “123” if `val` is any integer or decimal type that is exactly equal to 123. Note that this is unlike the normal Spark behavior for `decimal` types, which would produce “123.00” for `Decimal<9,2>`.

3) One exception to the above is `schema_of_variant`, which will still report the underlying physical type. This means that `schema_of_variant` may report different numeric types before and after shredding.

4) Other than integer and decimal, we will not allow casting between types. For example, we will not write the string “123” to an integer **typed_value** column, even though `variant_get(“123”, “$”, “integer”)` would produce the integer 123. Similarly, double and float types are considered distinct from other numeric types, and we would not write them to a numeric **typed_value** column.

# Reconstructing a Variant

It is possible to recover a full Variant value using a recursive algorithm, where the initial call is to `ConstructVariant` with the top-level fields, which are assumed to be null if they are not present in the schema.

```
# Constructs a Variant from `untyped_value`, `object`, `array` and `typed_value`.
# Only one of object, array and typed_value may be non-null.
def ConstructVariant(untyped_value, object, array, typed_value):
  if object is null and array is null and typed_value is null: return untyped_value
  elif object is not null:
    return ConstructObject(untyped_value, object)
  elif array is not null:
    return ConstructArray(array)
  else:
    # Leaf in the tree.
    assert(untyped_value is null or untyped_value is VariantNull)
    return coalesce(untyped_value, cast(typed_value as Variant))

# Construct an object from an `object` group, and a (possibly null) Variant untyped_value
def ConstructObject(untyped_value, object)
  # If untyped_value is present and is not an Object, then the result is ambiguous.
  assert(untyped_value is null or is_object(untyped_value))
  all_keys = Union(untyped_value.keys, object.fields)
  return VariantObject(all_keys.map { key ->
    if object[field] is null: (key, untyped_value[field])
    else: (key, ConstructVariant(null, object[field], null, null))
  } 

def ConstructArray(array)
  newVariantArray = VariantArray()
  for i in range(array.size):
    # Any of these may be missing from the schema, in which case they are null.
    newVariantArray.append(ConstructVariant(array[i].untyped_value, array[i].object, array[i].array, array[i].typed_value)
```

# Nested Parquet Example

This section describes a more deeply nested example, using a top-level array as the shredding type.

Below is a sample of JSON that would be fully shredded in this example. It contains an array of objects, containing an “a” field shredded as an array, and a “b” field shredded as an integer.

```
[
  {
    "a": [1, 2, 3],
    "b": 100
  },
  {
    "a": [4, 5, 6],
    "b": 200
  }
]
```


The corresponding Parquet schema with “a” and “b” as leaf types is:

```
optional group variant_col {
 optional binary untyped_value;
 optional group array (LIST) {
  repeated group list {
   optional group element {
    optional binary untyped_value;
    optional group object {
     optional group a {
      optional binary untyped_value;
      optional group array (LIST) {
       repeated group list {
        optional group element {
         optional int64 typed_value;
         optional binary untyped_value;
        }
       }
      }
     }
     optional group b {
      optional int64 typed_value;
      optional binary untyped_value;
     }
    }
   }
  }
 }
}
```

In the above example schema, if “a” is an array containing a mix of integer and non-integer values, the engine will shred individual elements appropriately into either **typed_value** or **untyped_value**.
If the top-level Variant is not an array (for example, an object), the engine cannot shred the value and it will store it in the top-level **untyped_value**.
Similarly, if "a" is not an array, it will be stored in the **untyped_value** under "a".

Consider the following example:

```
[
  {
    "a": [1, 2, 3],
    "b": 100,
    “c”: “unexpected”
  },
  {
    "a": [4, 5, 6],
    "b": 200
  },
  “not an object”
]
```

The second array element can be fully shredded, but the first and third cannot be. The contents of `variant_col.array[*].untyped_value` would be as follows:

```
[
  { “c”: “unexpected” },
  NULL,
  “not an object”
]
```

# Backward and forward compatibility

Shredding is an optional features of Variant, and readers must continue to be able to read a group containing only a `value` and `metadata` column.

We will follow the convention defined in https://github.com/delta-io/delta/blob/master/protocol_rfcs/variant-type.md#variant-data-in-parquet, and ignore any fields in the same group as typed_value/untyped_value that start with `_` (underscore).
This is intended to allow future backwards-compatible extensions. In particular, the field names `_metadata_key_paths` and any name starting with `_spark` are reserved, and should not be used by other implementations.
Any extra field names that do not start with an underscore should be assumed to be backwards incompatible, and readers should fail when reading such a schema.

Engines without shredding support are not expected to be able to read Parquet files that use shredding. Since different files may contain conflicting schemas (e.g. a `typed_value` column with incompatible types in two files), it may not be possible to infer or specify a single schema that would allow all Parquet files for a table to be read.
