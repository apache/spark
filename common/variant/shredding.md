# Shredding Overview

The Spark Variant type is designed to store and process semi-structured data efficiently, even with heterogeneous values. Query engines encode each variant value in a self-describing format, and store it as a group containing **value** and **metadata** binary fields in Parquet. Since data is often partially homogenous, it can be beneficial to extract certain fields into separate Parquet columns to further improve performance. We refer to this process as "shredding". Each Parquet file remains fully self-describing, with no additional metadata required to read or fully reconstruct the Variant data from the file. Combining shredding with a binary residual provides the flexibility to represent complex, evolving data with an unbounded number of unique fields while limiting the size of file schemas, and retaining the performance benefits of a columnar format.

This document focuses on the shredding semantics, Parquet representation, implications for readers and writers, as well as the Variant reconstruction. For now, it does not discuss which fields to shred, user-facing API changes, or any engine-specific considerations like how to use shredded columns. The approach builds on top of the generic Spark Variant representation, and leverages the existing Parquet specification for maximum compatibility with the open-source ecosystem.

At a high level, we introduce a third field to the Variant Parquet group called **paths**. This represents a fixed schema suitable for merging into the main **value** and **metadata** to construct the full Variant value for each row.

Shredding lets Spark (or any other query engine) reap the full benefits of Parquet's columnar representation, such as more compact data encoding, min/max statistics for data skipping, and I/O and CPU savings from pruning unnecessary fields not accessed by a query (including the non-shredded Variant binary data).
Without shredding, any query that accesses a Variant column must fetch all bytes of the full binary buffer. With shredding, we can get nearly equivalent performance as in a relational (scalar) data model.

For example, `select variant_get(variant_col, ‘$.field1.inner_field2’, ‘string’) from tbl` only needs to access `inner_field2`, and the file scan could avoid fetching the rest of the Variant value if this field was shredded into a separate column in the Parquet schema. Similarly, for the query `select * from tbl where variant_get(variant_col, ‘$.id’, ‘integer’) = 123`, the scan could first decode the shredded `id` column, and only fetch/decode the full Variant value for rows that pass the filter.

# Parquet Example

Consider the following Parquet schema together with how Variant values might be mapped to it. Notice that we represent each shredded field in **paths** as a group of two fields, **typed_value** and **untyped_value**. We extract all homogenous data items of a certain path into **typed_value**, and set aside incompatible data items in **untyped_value**. Intuitively, incompatibilities within the same path may occur because we store the shredding schema per Parquet file, and each file can contain several row groups. Selecting a type for each field that is acceptable for all rows would be impractical because it would require buffering the contents of an entire file before writing.

```
optional group variant_col {
 optional binary value;
 optional binary metadata;
 optional group paths {
  optional group a {
   optional int64 typed_value;
   optional binary untyped_value;
  }
  optional group b {
   optional binary typed_value (STRING);
   optional binary untyped_value;
  }
 }
}
```


| Variant Value | Top-level value/metadata | Non-null in a | Non-null in b |
|---------------|--------------------------|---------------|---------------|
| {a: 123, b: “hello”} | null | typed_value | typed_value |
| {a: 1.23, b: “123”} | null | untyped_value | typed_value |
| {a: [1,2,3], b: null} | null | untyped_value | untyped_value |
| {a: 123, c: 456} | {c: 123} | typed_value | null |
| [{a: 1, b: 2}, {a:3, b:4}] | [{a: 1, b: 2}, {a:3, b:4}] | null | null |

# Parquet Layout

The **paths** group may contain any arbitrary nesting of list or group types, representing Variant array and object types, respectively. The group field names correspond to the Variant object field names. Arrays must use the three-level list structure described in https://github.com/apache/parquet-format/blob/master/LogicalTypes.md.

Each leaf in the **paths** schema can store an arbitrary Variant value. It contains an **untyped_value** binary field and a **typed_value** field. If non-null, **untyped_value** represents the value stored as a Variant binary; the metadata and value of a normal Variant are concatenated. The **typed_value** field may be any type that has a corresponding Variant type. For each value in the data, at most one of the **typed_value** and **untyped_value** may be non-null.

| typed_value | value/metadata | Meaning |
|-------------|----------------|---------|
| null | null | Field is missing in the reconstructed Variant. | 
| null | non-null | Field may be any type in the reconstructed Variant. | 
| non-null | null | Field has this column’s type in the reconstructed Variant. | 
| non-null | non-null | Invalid |

The **typed_value** may be absent from the Parquet schema for any field, which is equivalent to its value being always null (in which case the shredded field is always stored as a Variant binary). By the same token, **untyped_value** may be absent, which is equivalent to their value being always null (in which case the field will always be missing or have the type of the **typed_value** column).
(Exception: **typed_value** may be a nested type, but to avoid confusion, if the type of **typed_value** is itself a nested type, the value and metadata must be present to identify it as a leaf.)

It is possible to reconstruct the full metadata and value from **untyped_value** by treating the leading bytes as metadata, and using the header, dictionary size and final dictionary offset to determine the start of the Variant value section. (See the metadata description in the common/variant/README.md for more detail on how to interpret it.) For example, in the binary below, there is a one-element dictionary, and the final offset (`offset[1]`) indicates that the last dictionary entry ends at the second byte. Therefore the full metadata size is six bytes, and the rest is the value section of the Variant.

```
   hdr    sz   offset[0] offset[1] bytes[0] bytes[1]  value
 --------------------------------------------------------------------
|      |      |         |         |        |        |
| 0x01 | 0x01 | 0x00    | 0x02    | ‘h’    | ‘i’    | . . . . . . . .
|______|______|_________|_________|________|________|________________
```

# Top-level value/metadata

Shredded values are not redundantly stored in the top-level **value** binary. The top-level binary must only contain the values that cannot be represented in the **paths** group. For example, if **paths** contains a leaf field at the path `field1.sub_field1`, then that path should not appear in **value**. However `field1` may appear in **value** as an object with fields other than `sub_field1` (such as `sub_field2`), or as any non-object (such as an array or scalar value).

The top-level **metadata** only needs to contain dictionary entries that are referred to in the top-level **value**. When reconstructing a full variant, the metadata will need to be extended to contain dictionary entries implied by data in **paths**.

If all values can be represented in **paths**, the top-level value and metadata are set to null. If an array contains a mix of elements that could and couldn't be fully represented in **paths**, then elements that were fully represented are replaced will Variant NULL in the top-level value. For objects, fully represented fields are simply omitted from the object.

# Using untyped_value vs. typed_value

In general, it is desirable to store values in the **typed_value** field rather than the **untyped_value** whenever possible. This will typically improve encoding efficiency, and allow the use of Parquet statistics to filter at the row group or page level. In the best case, the **untyped_value** fields are all null and the engine does not need to read them (or it can omit them from the schema on write entirely). There are two main motivations for including the **untyped_value** column:

1) In a case where there are rare type mismatches (for example, a numeric field with rare strings like “n/a”), we allow the field to be shredded, which could still be a significant performance benefit compared to fetching and decoding the full value/metadata binary.
2) Since there is a single schema per file, there would be no easy way to recover from a type mismatch encountered late in a file write. Parquet files can be large, and buffering all file data before starting to write could be expensive. Including an untyped column for every field guarantees we can adhere to the requested shredding schema.

The **untyped_value** is stored in a single binary column, rather than storing the value and metadata separately as is done in the top-level binary format. The motivation for storing them separately at the top level is that this lets the engine encode and compress the metadata more efficiently when the fields are consistent across rows. We chose to combine them in the shredded fields: we expect the encoding/compression benefit to be lower, since in the case of uniform data, the values should be stored in the typed column. Using a single binary reduces the extra metadata required in the Parquet schema, which can be significant in some cases.

# Data Skipping

Shredded columns are expected to store statistics in the same format as a normal Parquet column. In general, the engine can only skip a row group or page if all rows in the **untyped_value** field are null, since it is possible for a `variant_get` expression to successfully cast a value from the **untyped_value** to the target type. For example, if **typed_value** is of type `int64`, then the string “123” might be contained in **untyped_value**, which would not be reflected in statistics, but could be retained by a filter like `where variant_get(col, “$.field”, “long”) = 123`. If **untyped_value** is all-null, then the engine can prune pages or row groups based on **typed_value**.

# Shredding Semantics

Variant defines a number of integer and decimal types of varying widths. When writing, it would be quite limiting to strictly enforce the mapping between Variant types and Parquet/Spark types. For example, if we chose to shred a field as `int64`, and encountered the value 123 encoded as `int32`, it seems preferable to write this to the **typed_value** column, even though it technically loses information about the type in the original Variant object, and would be reconstructed as an `int64`.

On the other hand, storing arbitrarily casted values in the **typed_value** column could create inconsistent behavior before and after shredding, and could leak behavior from the writing engine to the reading engine. For example, double-to-string casts can produce different results in different engines. Performing such a cast while shredding (even if we somehow retained the knowledge that the original value was a `double`) could result in confusing behavior changes if shredding took place using a different tool from the query engine that wrote it.

Our approach is a pragmatic compromise that allows the use of **typed_value** in cases where the type can be losslessly widened without resulting in a significant difference in the reconstructed Variant:

1) All integer and decimal types in Variant are conceptually a single “number” type. The engine may shred any number into the **typed_value** of any other number, provided that no information about the value is lost. For example, the integer 123 may be shredded as Decimal<9, 2>, but 1.23 may not be shredded as any integer type.

2) To ensure that behavior remains unchanged before and after shredding, we will aim to have all Spark expressions that operate on Variant be agnostic to the specific numeric type. For example, `cast(val as string)` should produce the string “123” if `val` is any integer or decimal type that is exactly equal to 123. Note that this is unlike the normal Spark behavior for `decimal` types, which would produce “123.00” for `Decimal<9,2>`.

3) One exception to the above is `schema_of_variant`, which will still report the underlying physical type. This means that `schema_of_variant` may report different numeric types before and after shredding.

4) Other than integer and decimal, we will not allow casting between types. For example, we will not write the string “123” to an integer **typed_value** column, even though `variant_get(“123”, “$”, “integer”)` would produce the integer 123. Similarly, double and float types are considered distinct from other numeric types, and we would not write them to a numeric **typed_value** column.

# Reconstructing a Variant

It is possible to recover a full Variant value using a recursive algorithm, where the initial call is to `ConstructVariant` with the Variant value implied by the top-level value/metadata, and the nested structure implied by the top-level **paths**.

Note that the top-level **paths** may represent an object, array or even a scalar (that is, **paths** itself may be the leaf of the tree in the pseudocode below).

```
# Constructs a Variant from `value` and `paths`.
# Either may be null, in which case the result is the Variant value implied by the other one.
def ConstructVariant(value, paths):
  if paths is null: return value
  elif is_struct(paths):
    return ConstructObject(value, paths)
  elif is_array(paths):
    return ConstructArray(value, paths)
  else:
    # Leaf in paths. If the value is also non-empty, then the result is ambiguous.
    assert(value is null or value is VariantNull)
    return coalesce(paths.untyped_value, cast(paths.typed_value as Variant))

# Construct an object from a `paths` group, and a (possibly null) Variant value
def ConstructObject(value, paths)
  # If value is present and is not an Object, then the result is ambiguous.
  assert(value is null or is_object(value))
  all_keys = Union(value.keys, paths.fields)
  return VariantObject(all_keys.map { key ->
    if paths[field] is null: (key, value[field])
    # Note: value[field] can be null here.
    else: (key, ConstructVariant(value[field], paths[field]))
  } 

def ConstructArray(value, paths)
  # If value is present and is not an Array of the same size, then the result is ambiguous.
  assert(value is null or value is VariantNull or (is_array(value) and value.size == paths.size)
  newVariantArray = VariantArray()
  for i in range(paths.size):
    # value[i] is null iff value is null
    newVariantArray.append(ConstructVariant(value[i], paths[i])
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
 optional binary value;
 optional binary metadata;
 optional group paths (LIST) {
  repeated group list {
   optional group element {
    optional group a (LIST) {
     repeated group list {
      optional group element {
       optional int64 typed_value;
       optional binary untyped_value;
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
```

In the above example schema, if “a” contains a mix of integer and non-integer values, the engine will shred individual elements appropriately into either **typed_value** or **untyped_value**. If the top-level Variant is not an array (for example, an object), the engine cannot shred the value and it will store it in the top-level value/metadata.

If the array contains a mix of elements that can and cannot be fully shredded, then the unshredded elements are stored as a Variant NULL. Consider the following example:

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

The second array element can be fully shredded, but the first and third cannot be. The resulting top-level Variant would be as follows:

```
[
  { “c”: “unexpected” },
  NULL,
  “not an object”
]
```

An alternative Parquet schema for the data above is the following:

```
optional group variant_col {
 optional binary value;
 optional binary metadata;
 optional group paths (LIST) {
  repeated group list {
   optional group element {
    optional group a {
     optional group typed_value (LIST) {
      repeated group list {
       optional int64 element;
       }
      }
     }
     optional binary untyped_value;
    }
    optional group b {
     optional int64 typed_value;
     optional binary untyped_value;
    }
   }
  }
 }
}
```

This is similar to the previous schema, but the **typed_value** is an entire list. In this case, if the “a” field is an array, but contains values that cannot be stored as int64, then the entire array is stored as a single **untyped_value** binary. We expect the pros and cons of these different schemes to be marginal, and we may choose not to support the latter on the write path. The example mainly serves to demonstrate flexibility in deciding how to shred.

