Spark SQL
=========

This module provides support for executing relational queries expressed in either SQL or the DataFrame/Dataset API.

Spark SQL is broken up into five subprojects:
 - API (sql/api) - Includes some public API like DataType, Row, etc. This component can be shared between Catalyst and Spark Connect client.
 - Catalyst (sql/catalyst) - An implementation-agnostic framework for manipulating trees of relational operators and expressions.
 - Execution (sql/core) - A query planner / execution engine for translating Catalyst's logical query plans into Spark RDDs.  This component also includes a new public interface, SQLContext, that allows users to execute SQL or LINQ statements against existing RDDs and Parquet files.
 - Hive Support (sql/hive) - Includes extensions that allow users to write queries using a subset of HiveQL and access data from a Hive Metastore using Hive SerDes. There are also wrappers that allow users to run queries that include Hive UDFs, UDAFs, and UDTFs.
 - HiveServer and CLI support (sql/hive-thriftserver) - Includes support for the SQL CLI (bin/spark-sql) and a HiveServer2 (for JDBC/ODBC) compatible server.

Running `./sql/create-docs.sh` generates SQL documentation for built-in functions under `sql/site`, and SQL configuration documentation that gets included as part of `configuration.md` in the main `docs` directory.
