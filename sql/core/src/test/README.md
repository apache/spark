# Notes for Parquet compatibility tests

The following directories and files are used for Parquet compatibility tests:

```
.
├── README.md                   # This file
├── avro
│   ├── parquet-compat.avdl     # Testing Avro IDL
│   └── parquet-compat.avpr     # !! NO TOUCH !! Protocol file generated from parquet-compat.avdl
├── gen-java                    # !! NO TOUCH !! Generated Java code
├── scripts
│   └── gen-code.sh             # Script used to generate Java code for Thrift and Avro
└── thrift
    └── parquet-compat.thrift   # Testing Thrift schema
```

Generated Java code are used in the following test suites:

- `org.apache.spark.sql.parquet.ParquetAvroCompatibilitySuite`
- `org.apache.spark.sql.parquet.ParquetThriftCompatibilitySuite`

To avoid code generation during build time, Java code generated from testing Thrift schema and Avro IDL are also checked in.

When updating the testing Thrift schema and Avro IDL, please run `gen-code.sh` to update all the generated Java code.

## Prerequisites

Please ensure `avro-tools` and `thrift` are installed.  You may install these two on Mac OS X via:

```bash
$ brew install thrift avro-tools
```
