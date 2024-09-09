# Overview

A Variant represents a type that contain one of:
- Primitive: A type and corresponding value (e.g. INT, STRING)
- Array: An ordered list of Variant values
- Object: An unordered collection of string/Variant pairs (i.e. key/value pairs). An object may not contain duplicate keys.

A variant is encoded with 2 binary values, the [value](#value-encoding) and the [metadata](#metadata-encoding).

There are a fixed number of allowed primitive types, provided in the table below. These represent a commonly supported subset of the [logical types](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md) allowed by the Parquet.

The Variant spec allows representation of semi-structured data (e.g. JSON) in a form that can be efficiently queried by path. The design is intended to allow efficient access to nested data even in the presence of very wide or deep structures.

Another motivation for the representation is that (aside from metadata) each inner Variant value is contiguous and self-contained. For example, in a Variant containing an Array of Variant values, the representation of an inner Variant value, when paired with the metadata of the full variant, is itself a valid Variant.

# Metadata encoding

The encoded metadata always starts with a header byte.
```
             7     6  5   4  3             0
            +-------+---+---+---------------+
header      |       |   |   |    version    |
            +-------+---+---+---------------+
                ^         ^
                |         +-- sorted_strings
                +-- offset_size_minus_one
```
The `version` is a 4-bit value that must always contain the value `1`.
`sorted_strings` is a 1-bit value indicating whether dictionary strings are sorted and unique.
`offset_size_minus_one` is a 2-bit value providing the number of bytes per dictionary size and offset field.
The actual number of bytes, `offset_size`, is `offset_size_minus_one + 1`.

The entire metadata is encoded as the following diagram shows:
```
           7                     0
          +-----------------------+
metadata  |        header         |
          +-----------------------+
          |                       |
          :    dictionary_size    :  <-- little-endian, `offset_size` bytes
          |                       |
          +-----------------------+
          |                       |
          :        offset         :  <-- little-endian, `offset_size` bytes
          |                       |
          +-----------------------+
                      :
          +-----------------------+
          |                       |
          :        offset         :  <-- little-endian, `offset_size` bytes
          |                       |      (`dictionary_size + 1` offsets)
          +-----------------------+
          |                       |
          :         bytes         :
          |                       |
          +-----------------------+
```

The metadata is encoded first with the `header` byte, then `dictionary_size` which is a little-endian value of `offset_size` bytes, and represents the number of string values in the dictionary.
Next, is an `offset` list, which contains `dictionary_size + 1` values.
Each `offset` is a little-endian value of `offset_size` bytes, and represents the starting byte offset of the i-th string in `bytes`.
The first `offset` value will always be `0`, and the last `offset` value will always be the total length of `bytes`.
The last part of the metadata is `bytes`, which stores all the string values in the dictionary.

## Metadata encoding grammar

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
- The length of the ith string can be computed as `offset[i+1] - offset[i]`.
- The offset of the first string is always equal to 0 and is therefore redundant. It is included in the spec to simplify in-memory-processing.
- `offset_size_minus_one` indicates the number of bytes per `dictionary_size` and `offset` entry. I.e. a value of 0 indicates 1-byte offsets, 1 indicates 2-byte offsets, 2 indicates 3 byte offsets and 3 indicates 4-byte offsets.
- If `sorted_strings` is set to 1, strings in the dictionary must be unique and sorted in lexicographic order. If the value is set to 0, readers may not make any assumptions about string order or uniqueness.


# Value encoding

The entire encoded Variant value includes the `value_metadata` byte, and then 0 or more bytes for the `val`.
```
           7                                  2 1          0
          +------------------------------------+------------+
value     |            value_header            | basic_type |
          +------------------------------------+------------+
          |                                                 |
          :                   value_data                    :  <-- 0 or more bytes
          |                                                 |
          +-------------------------------------------------+
```
## Basic Type

The `basic_type` is 2-bit value that represents which basic type the Variant value is.
The [basic types table](#encoding-types) shows what each value represents.

## Value Header

The `value_header` is a 6-bit value that contains more information about the type, and the format depends on the `basic_type`.

### Value Header for Primitive type (`basic_type`=0)

When `basic_type` is `0`, `value_header` is a 6-bit `primitive_header`.
The [primitive types table](#encoding-types) shows what each value represents.
```
                 5                     0
                +-----------------------+
value_header    |   primitive_header    |
                +-----------------------+
```

### Value Header for Short string (`basic_type`=1)

When `basic_type` is `1`, `value_header` is a 6-bit `short_string_header`.
```
                 5                     0
                +-----------------------+
value_header    |  short_string_header  |
                +-----------------------+
```
The `short_string_header` value is the length of the string.

### Value Header for Object (`basic_type`=2)

When `basic_type` is `2`, `value_header` is made up of `field_offset_size_minus_one`, `field_id_size_minus_one`, and `is_large`.
```
                  5   4  3     2 1     0
                +---+---+-------+-------+
value_header    |   |   |       |       |
                +---+---+-------+-------+
                      ^     ^       ^
                      |     |       +-- field_offset_size_minus_one
                      |     +-- field_id_size_minus_one
                      +-- is_large
```
`field_offset_size_minus_one` and `field_id_size_minus_one` are 2-bit values that represent the number of bytes used to encode the field offsets and field ids.
The actual number of bytes is computed as `field_offset_size_minus_one + 1` and `field_id_size_minus_one + 1`.
`is_large` is a 1-bit value that indicates how many bytes are used to encode the number of elements.
If `is_large` is `0`, 1 byte is used, and if `is_large` is `1`, 4 bytes are used.

### Value Header for Array (`basic_type`=3)

When `basic_type` is `3`, `value_header` is made up of `field_offset_size_minus_one`, and `is_large`.
```
                 5         3  2  1     0
                +-----------+---+-------+
value_header    |           |   |       |
                +-----------+---+-------+
                              ^     ^
                              |     +-- field_offset_size_minus_one
                              +-- is_large
```
`field_offset_size_minus_one` is a 2-bit value that represents the number of bytes used to encode the field offset.
The actual number of bytes is computed as `field_offset_size_minus_one + 1`.
`is_large` is a 1-bit value that indicates how many bytes are used to encode the number of elements.
If `is_large` is `0`, 1 byte is used, and if `is_large` is `1`, 4 bytes are used.

## Value Data

The `value_data` encoding format depends on the type specified by `value_metadata`.
For some types, the `value_data` will be 0-bytes.

### Value Data for Primitive type (`basic_type`=0)

When `basic_type` is `0`, `value_data` depends on the `primitive_header` value.
The [primitive types table](#encoding-types) shows the encoding format for each primitive type.

### Value Data for Short string (`basic_type`=1)

When `basic_type` is `1`, `value_data` is the sequence of bytes that represents the string.

### Value Data for Object (`basic_type`=2)

When `basic_type` is `2`, `value_data` encodes an object.
The encoding format is shown in the following diagram:
```
                    7                     0
                   +-----------------------+
object value_data  |                       |
                   :     num_elements      :  <-- little-endian, 1 or 4 bytes
                   |                       |
                   +-----------------------+
                   |                       |
                   :       field_id        :  <-- little-endian, `field_id_size` bytes
                   |                       |
                   +-----------------------+
                               :
                   +-----------------------+
                   |                       |
                   :       field_id        :  <-- little-endian, `field_id_size` bytes
                   |                       |      (`num_elements` field_ids)
                   +-----------------------+
                   |                       |
                   :     field_offset      :  <-- little-endian, `field_offset_size` bytes
                   |                       |
                   +-----------------------+
                               :
                   +-----------------------+
                   |                       |
                   :     field_offset      :  <-- little-endian, `field_offset_size` bytes
                   |                       |      (`num_elements + 1` field_offsets)
                   +-----------------------+
                   |                       |
                   :         value         :
                   |                       |
                   +-----------------------+
                               :
                   +-----------------------+
                   |                       |
                   :         value         :  <-- (`num_elements` values)
                   |                       |
                   +-----------------------+
```
An object `value_data` begins with `num_elements`, a 1-byte or 4-byte little-endian value, representing the number of elements in the object.
The size in bytes of `num_elements` is indicated by `is_large` in the `value_header`.
Next, is a list of `field_id` values.
There are `num_elements` number of entries and each `field_id` is a little-endian value of `field_id_size` bytes.
A `field_id` is an index into the dictionary in the metadata.
The `field_id` list is followed by a `field_offset` list.
There are `num_elements + 1` number of entries and each `field_offset` is a little-endian value of `field_offset_size` bytes.
A `field_offset` represents the byte offset (relative to the first byte of the first `value`) where the i-th `value` starts.
The last `field_offset` points to the byte after the end of the last `value`.
The `field_offset` list is followed by the `value` list.
There are `num_elements` number of `value` entries and each `value` is an encoded Variant value.
For the i-th key-value pair of the object, the key is the metadata dictionary entry indexed by the i-th `field_id`, and the value is the Variant `value` starting from the i-th `field_offset` byte offset.

The field ids and field offsets must be in lexicographical order of the corresponding field names in the metadata dictionary.
However, the actual `value` entries do not need to be in any particular order.
This implies that the `field_offset` values may not be monotonically increasing.
For example, for the following object:
```
{
  "c": 3,
  "b": 2,
  "a": 1
}
```
The `field_id` list must be `[<id for key "a">, <id for key "b">, <id for key "c">]`, in lexicographical order.
The `field_offset` list must be `[<offset for value 1>, <offset for value 2>, <offset for value 3>, <last offset>]`.
The `value` list can be in any order.

### Value Data for Array (`basic_type`=3)

When `basic_type` is `3`, `value_data` encodes an array. The encoding format is shown in the following diagram:
```
                   7                     0
                  +-----------------------+
array value_data  |                       |
                  :     num_elements      :  <-- little-endian, 1 or 4 bytes
                  |                       |
                  +-----------------------+
                  |                       |
                  :     field_offset      :  <-- little-endian, `field_offset_size` bytes
                  |                       |
                  +-----------------------+
                              :
                  +-----------------------+
                  |                       |
                  :     field_offset      :  <-- little-endian, `field_offset_size` bytes
                  |                       |      (`num_elements + 1` field_offsets)
                  +-----------------------+
                  |                       |
                  :         value         :
                  |                       |
                  +-----------------------+
                              :
                  +-----------------------+
                  |                       |
                  :         value         :  <-- (`num_elements` values)
                  |                       |
                  +-----------------------+
```
An array `value_data` begins with `num_elements`, a 1-byte or 4-byte little-endian value, representing the number of elements in the array.
The size in bytes of `num_elements` is indicated by `is_large` in the `value_header`.
Next, is a `field_offset` list.
There are `num_elements + 1` number of entries and each `field_offset` is a little-endian value of `field_offset_size` bytes.
A `field_offset` represents the byte offset (relative to the first byte of the first `value`) where the i-th `value` starts.
The last `field_offset` points to the byte after the last byte of the last `value`.
The `field_offset` list is followed by the `value` list.
There are `num_elements` number of `value` entries and each `value` is an encoded Variant value.
For the i-th array entry, the value is the Variant `value` starting from the i-th `field_offset` byte offset.

## Value encoding grammar

The grammar for an encoded value is:

```
value: <value_metadata> <value_data>?
value_metadata: 1 byte (<basic_type> | (<value_header> << 2))
basic_type: ID from Basic Type table. <value_header> must be a corresponding variation
value_header: <primitive_header> | <short_string_header> | <object_header> | <array_header>
primitive_header: ID from Primitive Type table. <val> must be a corresponding variation of <primitive_val>
short_string_header: unsigned string length in bytes from 0 to 63
object_header: (is_large << 4 | field_id_size_minus_one << 2 | field_offset_size_minus_one)
array_header: (is_large << 2 | field_offset_size_minus_one)
value_data:  <primitive_val> | <short_string_val> | <object_val> | <array_val>
primitive_val: see table for binary representation
short_string_val: bytes
object_val: <num_elements> <field_id>* <field_offset>* <fields>
array_val: <num_elements> <field_offset>* <fields>
num_elements: a 1 or 4 byte little-endian value (depending on is_large in <object_header>/<array_header>)
field_id: a 1, 2, 3 or 4 byte little-endian value (depending on field_id_size_minus_one in <object_header>), indexing into the dictionary
field_offset: a 1, 2, 3 or 4 byte little-endian value (depending on field_offset_size_minus_one in <object_header>/<array_header>), providing the offset in bytes within fields
fields: <value>*
```

Each `value_data` must correspond to the type defined by `value_metadata`. Boolean and null types do not have a corresponding `value_data`, since their type defines their value.

Each `array_val` and `object_val` must contain exactly `num_elements + 1` values for `field_offset`. The last entry is the offset that is one byte past the last field (i.e. the total size of all fields in bytes). All offsets are relative to the first byte of the first field in the object/array.

`field_id_size_minus_one` and `field_offset_size_minus_one` indicate the number of bytes per field ID/offset. I.e. a value of 0 indicates 1-byte IDs, 1 indicates 2-byte IDs, 2 indicates 3 byte IDs and 3 indicates 4-byte IDs. The `is_large` flag for arrays and objects is used to indicate whether the number of elements is indicated using a one or four byte value. When more than 255 elements are present, `is_large` must be set to true. It is valid for an implementation to use a larger value than necessary for any of these fields (e.g. `is_large` may be true for an object with less than 256 elements).

The "short string" basic type may be used as an optimization to fold string length into the type byte for strings less than 64 bytes. It is semantically identical to the "string" primitive type.

The Decimal type contains a scale, but no precision. The implied precision of a decimal value is `floor(log_10(val)) + 1`.

# Encoding types

| Basic Type   | ID  | Description                                       |
|--------------|-----|---------------------------------------------------|
| Primitive    | `0` | One of the primitive types                        |
| Short string | `1` | A string with a length less than 64 bytes         |
| Object       | `2` | A collection of (string-key, variant-value) pairs |
| Array        | `3` | An ordered sequence of variant values             |

| Primitive Type              | Type ID | Equivalent Parquet Type     | Binary format                                                                                                       |
|-----------------------------|---------|-----------------------------|---------------------------------------------------------------------------------------------------------------------|
| null                        | `0`     | any                         | none                                                                                                                |
| boolean (True)              | `1`     | BOOLEAN                     | none                                                                                                                |
| boolean (False)             | `2`     | BOOLEAN                     | none                                                                                                                |
| int8                        | `3`     | INT(8, signed)              | 1 byte                                                                                                              |
| int16                       | `4`     | INT(16, signed)             | 2 byte little-endian                                                                                                |
| int32                       | `5`     | INT(32, signed)             | 4 byte little-endian                                                                                                |
| int64                       | `6`     | INT(64, signed)             | 8 byte little-endian                                                                                                |
| double                      | `7`     | DOUBLE                      | IEEE little-endian                                                                                                  |
| decimal4                    | `8`     | DECIMAL(precision, scale)   | 1 byte scale in range [0, 38], followed by little-endian unscaled value (see decimal table)                         |
| decimal8                    | `9`     | DECIMAL(precision, scale)   | 1 byte scale in range [0, 38], followed by little-endian unscaled value (see decimal table)                         |
| decimal16                   | `10`    | DECIMAL(precision, scale)   | 1 byte scale in range [0, 38], followed by little-endian unscaled value (see decimal table)                         |
| date                        | `11`    | DATE                        | 4 byte little-endian                                                                                                |
| timestamp                   | `12`    | TIMESTAMP(true, MICROS)     | 8-byte little-endian                                                                                                |
| timestamp without time zone | `13`    | TIMESTAMP(false, MICROS)    | 8-byte little-endian                                                                                                |
| float                       | `14`    | FLOAT                       | IEEE little-endian                                                                                                  |
| binary                      | `15`    | BINARY                      | 4 byte little-endian size, followed by bytes                                                                        |
| string                      | `16`    | STRING                      | 4 byte little-endian size, followed by UTF-8 encoded bytes                                                          |
| year-month interval         | `19`    | INT(32, signed)<sup>1</sup> | 1 byte denoting start field (1 bit) and end field (1 bit) starting at LSB followed by 4-byte little-endian value.   |
| day-time interval           | `20`    | INT(64, signed)<sup>1</sup> | 1 byte denoting start field (2 bits) and end field (2 bits) starting at LSB followed by 8-byte little-endian value. |

| Decimal Precision     | Decimal value type |
|-----------------------|--------------------|
| 1 <= precision <= 9   | int32              |
| 10 <= precision <= 18 | int64              |
| 18 <= precision <= 38 | int128             |
| > 38                  | Not supported      |

The year-month and day-time interval types have one byte at the beginning indicating the start and end fields. In the case of the year-month interval, the least significant bit denotes the start field and the next least significant bit denotes the end field. The remaining 6 bits are unused. A field value of 0 represents YEAR and 1 represents MONTH. In the case of the day-time interval, the least significant 2 bits denote the start field and the next least significant 2 bits denote the end field. The remaining 4 bits are unused. A field value of 0 represents DAY, 1 represents HOUR, 2 represents MINUTE, and 3 represents SECOND.

Type IDs 17 and 18 were originally reserved for a prototype feature (string-from-metadata) that was never implemented. These IDs are available for use by new types.

[1] The parquet format does not have pure equivalents for the year-month and day-time interval types. Year-month intervals are usually represented using int32 values and the day-time intervals are usually represented using int64 values. However, these values don't include the start and end fields of these types. Therefore, Spark stores them in the column metadata.

# Field ID order and uniqueness

For objects, field IDs and offsets must be listed in the order of the corresponding field names, sorted lexicographically. Note that the fields themselves are not required to follow this order. As a result, offsets will not necessarily be listed in ascending order.

An implementation may rely on this field ID order in searching for field names. E.g. a binary search on field IDs (combined with metadata lookups) may be used to find a field with a given field.

Field names are case-sensitive. Field names are required to be unique for each object. It is an error for an object to contain two fields with the same name, whether or not they have distinct dictionary IDs.

# Versions and extensions

An implementation is not expected to parse a Variant value whose metadata version is higher than the version supported by the implementation. However, new types may be added to the specification without incrementing the version ID. In such a situation, an implementation should be able to read the rest of the Variant value if desired.

# Shredding

For columnar storage formats, a single Variant object may have poor read performance when only a small number of fields are needed. A better approach is to create separate columns for individual fields, referred to as shredding or subcolumnarization. [shredding.md](shredding.md) describes an approach to shredding Variant columns in Parquet and similar columnar formats.
