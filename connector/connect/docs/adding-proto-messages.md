# Required, Optional and default values

Spark Connect adopts proto3, which does not support the use of the `required` constraint anymore. 
For non-message proto fields, there is also no `has_field_name` functions to easy tell
if a filed is set or not-set. (Read [proto3 field rules](https://developers.google.com/protocol-buffers/docs/proto3#specifying_field_rules))


### Required field

When adding fields that have required semantics, developers are required to follow
the outlined process. Fields that are semantically required for the server to 
correctly process the incoming message must be documented with `(Required)`. For scalar
fields the server will not perform any additional input validation. For compound fields,
the server will perform minimal checks to avoid null pointer exceptions but will not
perform any semantic validation.

Example:
```protobuf
message DataSource {
 // (Required) Supported formats include: parquet, orc, text, json, parquet, csv, avro.
 string format = 1;
}
```


### Optional fields

Semantically optional fields must be marked by `optional`. The server side will
then use this information to branch into different behaviors based on the presence or absence of this field. 

Due to the lack of configurable default values for scalar types, the pure presence of
an optional value does not define its default value. The server side implementation will interpret the observed value based on its own rules.

Example:
```protobuf
message DataSource {
  // (Optional) If not set, Spark will infer the schema.
  optional string schema = 2;
}
```
