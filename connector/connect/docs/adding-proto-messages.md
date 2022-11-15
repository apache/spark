# Required, Optional and default values

Connect adopts proto3, which does not support `required` constraint anymore. 
For non-message proto fields, there is also no `has_field_name` functions to easy tell
if a filed is set or not-set. (Read [proto3 field rules](https://developers.google.com/protocol-buffers/docs/proto3#specifying_field_rules))


### Required field

Even proto3 does not offer `required` constraint, there are still some fields that
are semantically required. For such case, we shall add comment `(Required)` on the
field to indicate it as required. The expectation for client implementation 
(or any submitted proto plan) is that such fields should be always set, and server will
always assume such fields are set and use whatever values from the fields directly.
It is the client side's fault to not offer meaningful value for `required` field and in that case,
the behavior on the server side is decided by the default value of the field.

Example:
```protobuf
message DataSource {
 // (Required) Supported formats include: parquet, orc, text, json, parquet, csv, avro.
 string format = 1;
}
```


### Optional field and default value

Semantically optional fields should be marked by proto3 `optional`. The server side will decide
if to use the generated `has_field_name` to tell the field is set (when its default value
is different from the Spark parameter default value) or use the field default value directly
(when its default value is the same as Spark parameter default value). It is also required
to use `(Optional)` in the comment to indicate this field is optional.

Example:
```protobuf
message DataSource {
  // (Optional) If not set, Spark will infer the schema.
  optional string schema = 2;
}
```
