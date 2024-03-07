# Overview

A Variant represents a type that contain one of:
- Primitive: A type and corresponding value (e.g. INT, STRING)
- Array: An ordered list of Variant values
- Object: An unordered collection of string/Variant pairs (i.e. key/value pairs). An object may not contain duplicate keys.

A variant is encoded with 2 binary values, the value and the metadata.

There are a fixed number of allowed primitive types, provided in the table below. These represent a commonly supported subset of the [logical types](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md) allowed by the Parquet.

The Variant spec allows representation of semi-structured data (e.g. JSON) in a form that can be efficiently queried by path. The design is intended to allow efficient access to nested data even in the presence of very wide or deep structures.

Another motivation for the representation is that (aside from metadata) each inner Variant value is contiguous and self-contained. For example, in a Variant containing an Array of Variant values, the representation of an inner Variant value, when paired with the metadata of the full variant, is itself a valid Variant.

# Metadata encoding

The grammar for encoded metadata is as follows

```
metadata: <header> <dictionary_size> <dictionary>
header: 1 byte (<version> | <sorted_strings> << 4 | (<offset_size_minus_one> << 6))
version: a 4-bit version ID. Currently, must always contain the value 1
sorted_strings: a 1-bit value indicating whether metadata strings are sorted
offset_size_minus_one: 2-bit value providing the number of bytes per dictionary size and offset field.
dictionary_size: `offset_size` bytes. little-endian value indicating the number of strings in the dictionary
dictionary: <offset>* <bytes>
offset: `offset_size` bytes. little-endian value indicating the starting position of the ith string in `bytes`. The list should contain `dictionary_size + 1` values, where the last value is the total length of `bytes`.
bytes: dictionary string values
```

Notes:
- Offsets are relative to the start of the `bytes` array.
- The length of the ith string can be computed as offset[i+1] - offset[i].
- The offset of the first string is always equal to 0 and is therefore redundant. It is included in the spec to simplify in-memory-processing.
- `offset_size_minus_one` indicates the number of bytes per `dictionary_size` and `offset` entry. I.e. a value of 0 indicates 1-byte offsets, 1 indicates 2-byte offsets, 2 indicates 3 byte offsets and 3 indicates 4-byte offsets.
- If `sorted_strings` is set to 1, strings in the dictionary must be unique and sorted in lexicographic order. If the value is set to 0, readers may not make any assumptions about string order or uniqueness.


# Value encoding

The grammar for an encoded value is:

```
value: <val_meta> <val>?
val_meta: 1 byte (<basic_type> | (<val_header> << 2))
basic_type: ID from Basic Type table. <val_header> must be a corresponding variation
val_header: <primitive_header> | <short_string_header> | <object_header> | <array_header>
primitive_header: ID from Primitive Type table. <val> must be a corresponding variation of <primitive_val>
short_string_header: unsigned string length in bytes from 0 to 63
object_header: (is_large << 4 | field_id_size_minus_one << 2 | field_offset_size_minus_one)
array_header: (is_large << 2 | field_offset_size_minus_one)
val:  <primitive_val> | <decimal_val> | <object_val> | <array_val>
primitive_val: see table for binary representation
short_string_val: bytes
decimal_val: <decimal_scale> <unscaled_decimal_value>
decimal_scale: one-byte value in the range [0, 38]
unscaled_decimal_value: see table
object_val: <num_elements> <field_id>* <field_offset>* <fields>
array_val: <num_elements> <field_offset>* <fields>
num_elements: a 1 or 4 byte value (depending on is_large in <object_header>/<array_header>)
field_id: a 1, 2, 3 or 4 byte value (depending on field_id_size_minus_one in <object_header>), indexing into the dictionary.
field_offset: a 1, 2, 3 or 4 byte value (depending on field_offset_size_minus_one in <object_header>/<array_header>), providing the offset in bytes within fields
fields: <value>*
```

Each `val` must correspond to the type defined by `val_meta`. Boolean and null types do not have a corresponding `val`, since their type defines their value.

Each `array_val` and `object_val` must contain exactly `num_elements + 1` values for `field_offset`. The last entry is the offset that is one byte past the last field (i.e. the total size of all fields in bytes). All offsets are relative to the first byte of the first field in the object/array.

`field_id_size_minus_one` and `field_offset_size_minus_one` indicate the number of bytes per field ID/offset. I.e. a value of 0 indicates 1-byte IDs, 1 indicates 2-byte IDs, 2 indicates 3 byte IDs and 3 indicates 4-byte IDs. The `is_large` flag for arrays and objects is used to indicate whether the number of elements is indicated using a one or three bytes. When more than 255 elements are present, `is_large` must be set to true. It is valid for an implementation to use a larger value than necessary for any of these fields (e.g. `is_large` may be true for an object with less than 256 elements).

The "short string" basic type may be used as an optimization to fold string length into the type byte for strings less than 64 bytes. It is semantically identical to the "string" primitive type.

String and binary values may also be represented as an index into the metadata dictionary. (See “string from metadata” and “binary from metadata” in the “Primitive Types” table) Writers may choose to use this mechanism to avoid repeating identical string values in a Variant object.

The Decimal type contains a scale, but no precision. The implied precision of a decimal value is `floor(log_10(val)) + 1`.

# Encoding types

| Basic Type   | ID |
|--------------|----|
| Primitive    | 0  |
| Short string | 1  |
| Object       | 2  |
| Array        | 3  |

| Primitive Type              | Type ID | Equivalent Parquet Type   | Binary format                                              |
|-----------------------------|---------|---------------------------|------------------------------------------------------------|
| null                        | 0       | any                       | none                                                       |
| boolean (True)              | 1       | BOOLEAN                   | none                                                       |
| boolean (False)             | 2       | BOOLEAN                   | none                                                       |
| int8                        | 3       | INT(8, signed)            | 1 byte                                                     |
| int16                       | 4       | INT(16, signed)           | 2 byte little-endian                                       |
| int32                       | 5       | INT(32, signed)           | 4 byte little-endian                                       |
| int64                       | 6       | INT(64, signed)           | 8 byte little-endian                                       |
| double                      | 7       | DOUBLE                    | IEEE little-endian                                         |
| decimal4                    | 8       | DECIMAL(precision, scale) | Little-endian, see decimal table                           |
| decimal8                    | 9       | DECIMAL(precision, scale) | Little-endian, see decimal table                           |
| decimal16                   | 10      | DECIMAL(precision, scale) | Little-endian, see decimal table                           |
| date                        | 11      | DATE                      | 4 byte little-endian                                       |
| timestamp                   | 12      | TIMESTAMP(true, MICROS)   | 8-byte little-endian                                       |
| timestamp without time zone | 13      | TIMESTAMP(false, MICROS)  | 8-byte little-endian                                       |
| float                       | 14      | FLOAT                     | IEEE little-endian                                         |
| binary                      | 15      | BINARY                    | 4 byte little-endian size, followed by bytes               |
| string                      | 16      | STRING                    | 4 byte little-endian size, followed by UTF-8 encoded bytes |
| binary from metadata        | 17      | BINARY                    | Little-endian index into the metadata dictionary. Number of bytes is equal to the metadata offset_size. |
| string from metadata        | 18      | STRING                    | Little-endian index into the metadata dictionary. Number of bytes is equal to the metadata offset_size. |

| Decimal Precision     | Decimal value type |
|-----------------------|--------------------|
| 1 <= precision <= 9   | int32              |
| 10 <= precision <= 18 | int64              |
| 18 <= precision <= 38 | int128             |
| > 38                  | Not supported      |

# Field ID order and uniqueness

For objects, field IDs and offsets must be listed in the order of the corresponding field names, sorted lexicographically. Note that the fields themselves are not required to follow this order. As a result, offsets will not necessarily be listed in ascending order.

An implementation may rely on this field ID order in searching for field names. E.g. a binary search on field IDs (combined with metadata lookups) may be used to find a field with a given field.

Field names are case-sensitive. Field names are required to be unique for each object. It is an error for an object to contain two fields with the same name, whether or not they have distinct dictionary IDs.

# Versions and extensions

An implementation is not expected to parse a Variant value whose metadata version is higher than the version supported by the implementation. However, new types may be added to the specification without incrementing the version ID. In such a situation, an implementation should be able to read the rest of the Variant value if desired.
