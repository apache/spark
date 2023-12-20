
## Note on PostgreSQLQueryTestSuite

The SQL files in this directory are automatically opted into `PostgreSQLQueryTestSuite`, which
generates golden files with Postgres and runs Spark against these golden files. If you added a new
SQL test file, please generate the golden files with `PostgreSQLQueryTestSuite`. It is likely to
fail because of incompatibility between the SQL dialects of Spark and Postgres. To have queries that
are compatible for both systems, keep your SQL queries simple by using basic types like INTs and
strings only, if possible, and using functions that are present in both Spark and Postgres. Refer
to the header comment for `PostgreSQLQueryTestSuite` for more information.