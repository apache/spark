# Notes for Parquet compatibility tests

The following directories and files are used for Parquet compatibility tests:

```
.
├── README.md                   # This file
├── avro
│   ├── *.avdl                  # Testing Avro IDL(s)
│   └── *.avpr                  # !! NO TOUCH !! Protocol files generated from Avro IDL(s)
├── gen-java                    # !! NO TOUCH !! Generated Java code
├── scripts
│   ├── gen-avro.sh             # Script used to generate Java code for Avro
│   └── gen-thrift.sh           # Script used to generate Java code for Thrift
└── thrift
    └── *.thrift                # Testing Thrift schema(s)
```

To avoid code generation during build time, Java code generated from testing Thrift schema and Avro IDL are also checked in.

When updating the testing Thrift schema and Avro IDL, please run `gen-avro.sh` and `gen-thrift.sh` accordingly to update generated Java code.

## Prerequisites

Please ensure `avro-tools` and `thrift` are installed.  You may install these two on Mac OS X via:

```bash
$ brew install thrift avro-tools
```
