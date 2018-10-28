---
layout: global
title: Troubleshooting
displayTitle: Troubleshooting
---

 * The JDBC driver class must be visible to the primordial class loader on the client session and on all executors. This is because Java's DriverManager class does a security check that results in it ignoring all drivers not visible to the primordial class loader when one goes to open a connection. One convenient way to do this is to modify compute_classpath.sh on all worker nodes to include your driver JARs.
 * Some databases, such as H2, convert all names to upper case. You'll need to use upper case to refer to those names in Spark SQL.
 * Users can specify vendor-specific JDBC connection properties in the data source options to do special treatment. For example, `spark.read.format("jdbc").option("url", oracleJdbcUrl).option("oracle.jdbc.mapDateToTimestamp", "false")`. `oracle.jdbc.mapDateToTimestamp` defaults to true, users often need to disable this flag to avoid Oracle date being resolved as timestamp.
